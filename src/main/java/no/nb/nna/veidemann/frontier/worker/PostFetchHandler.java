/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.frontier.worker;

import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest.Metrics;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 *
 */
public class PostFetchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PostFetchHandler.class);

    final StatusWrapper status;
    final Frontier frontier;
    final ConfigObject collectionConfig;
    final CrawlLimitsConfig limits;
    final QueuedUriWrapper qUri;
    final Collection<Annotation> scriptParameters;

    private long delayMs = 0L;
    private long fetchTimeMs = 0L;

    private AtomicBoolean done = new AtomicBoolean();
    private AtomicBoolean finalized = new AtomicBoolean();

    private List<QueuedUri> outlinkQueue = new ArrayList<>();

    public PostFetchHandler(String sessionToken, Frontier frontier) throws DbException {
        this(frontier.getCrawlQueueManager().getCrawlHostGroupForSessionToken(sessionToken), frontier);
        if (!qUri.getCrawlHostGroup().getSessionToken().equals(sessionToken)) {
            throw new IllegalSessionException("Session token mismatch. Fetch in progress from another harvester");
        }
    }

    public PostFetchHandler(CrawlHostGroup chg, Frontier frontier) throws DbException {
        if (chg == null) {
            LOG.debug("Could not find CrawlHostGroup. Fetch has probably timed out");
            throw new IllegalSessionException("Could not find CrawlHostGroup. Fetch has probably timed out");
        }
        // Refresh CHG busy timout to ensure postfetch has time to do its job.
        if (!frontier.getCrawlQueueManager().updateBusyTimeout(chg.getId(),
                chg.getSessionToken(), System.currentTimeMillis() + 60000L)) {
            LOG.debug("Could not refresh busy timeout. Fetch has probably timed out");
            throw new IllegalSessionException("Could not refresh busy timeout. Fetch has probably timed out");
        }

        QueuedUri queuedUri = frontier.getCrawlQueueManager().getQueuedUri(chg.getCurrentUriId());
        if (queuedUri == null) {
            LOG.debug("Could not find Queued URI. Fetch has probably timed out");
            throw new IllegalSessionException("Could not find Queued URI. Fetch has probably timed out");
        }
        fetchTimeMs = Durations.toMillis(Timestamps.between(chg.getFetchStartTimeStamp(), ProtoUtils.getNowTs()));

        this.status = StatusWrapper.getStatusWrapper(frontier, queuedUri.getExecutionId());
        this.collectionConfig = frontier.getConfig(status.getCrawlConfig().getCrawlConfig().getCollectionRef());
        ConfigObject seed = frontier.getConfig(ConfigRef.newBuilder()
                .setKind(Kind.seed).setId(status.getCrawlExecutionStatus().getSeedId())
                .build());
        this.scriptParameters = frontier.getScriptParameterResolver().GetScriptParameters(seed, status.getCrawlJobConfig());
        this.qUri = QueuedUriWrapper.getQueuedUriWrapperNoScopeCheck(frontier, queuedUri, collectionConfig.getMeta().getName())
                .clearError();

        this.frontier = frontier;
        this.limits = status.getCrawlJobConfig().getCrawlJob().getLimits();
    }

    public String getId() {
        return status.getId();
    }

    public QueuedUriWrapper getUri() {
        return qUri;
    }

    public String getSessionToken() throws DbQueryException {
        return qUri.getCrawlHostGroup().getSessionToken();
    }

    /**
     * Do post processing after a successful fetch.
     */
    public void postFetchSuccess(Metrics metrics) throws DbException {
        if (done.compareAndSet(false, true)) {
            MDC.put("eid", qUri.getExecutionId());
            MDC.put("uri", qUri.getUri());

            status.incrementDocumentsCrawled()
                    .incrementBytesCrawled(metrics.getBytesDownloaded())
                    .incrementUrisCrawled(metrics.getUriCount())
                    .removeCurrentUri(qUri).saveStatus();
        }
    }

    /**
     * Do post proccessing after a failed fetch.
     *
     * @param error the Error causing the failure
     */
    public void postFetchFailure(Error error) throws DbException {
        if (done.compareAndSet(false, true)) {
            MDC.put("eid", qUri.getExecutionId());
            MDC.put("uri", qUri.getUri());

            PreconditionState state = ErrorHandler.fetchFailure(frontier, status, qUri, error);
            switch (state) {
                case DENIED:
                    status.removeCurrentUri(qUri).saveStatus();
                    break;
                case RETRY:
                    qUri.save();
                    status.addCurrentUri(qUri).saveStatus();
                    break;
            }
        }
    }

    /**
     * Do some housekeeping.
     * </p>
     * This should be run regardless of if we fetched anything or if the fetch failed in any way.
     */
    public void postFetchFinally() {
        if (finalized.compareAndSet(false, true)) {
            try {
                calculateDelay();
            } catch (DbException e) {
                LOG.error(e.toString(), e);
            }

            MDC.put("eid", qUri.getExecutionId());
            MDC.put("uri", qUri.getUri());

            try {
                if (!CrawlExecutionHelpers.isAborted(frontier, status)) {
                    // Handle outlinks
                    Span span = GlobalTracer.get().activeSpan();
                    ConfigRef scopeScriptRef = status.getCrawlJobConfig().getCrawlJob().getScopeScriptRef();
                    forEach(span, frontier.getPostFetchThreadPool(), outlinkQueue, outlink -> {
                        try {
                            OutlinkHandler.processOutlink(frontier, status, qUri, outlink, scriptParameters, scopeScriptRef);
                        } catch (DbException e) {
                            // An error here indicates problems with DB communication. No idea how to handle that yet.
                            LOG.error("Error processing outlink: {}", e.toString(), e);
                        } catch (Throwable e) {
                            // Catch everything to ensure crawl host group gets released.
                            // Discovering this message in logs should be investigated as a possible bug.
                            LOG.error("Unknown error while processing outlink. Might be a bug", e);
                        }
                    });
                }
            } catch (DbException e) {
                LOG.error(e.toString(), e);
            }

            CrawlExecutionHelpers.postFetchFinally(frontier, status, qUri, getDelay(TimeUnit.MILLISECONDS));
        }
    }

    public static <E> void forEach(Span span, ForkJoinPool threadPool, List<E> array, Consumer<E> action) {
        class Task extends CountedCompleter<Void> {
            final int lo, hi;

            Task(Task parent, int lo, int hi) {
                super(parent, 31 - Integer.numberOfLeadingZeros(hi - lo));
                this.lo = lo;
                this.hi = hi;
            }

            public void compute() {
                try (Scope scope = GlobalTracer.get().activateSpan(span)) {
                    for (int n = hi - lo; n >= 2; n /= 2)
                        new Task(this, lo + n / 2, lo + n).fork();
                    action.accept(array.get(lo));
                    propagateCompletion();
                }
            }
        }
        if (array.size() > 0)
            threadPool.invoke(new Task(null, 0, array.size()));
    }

    public void queueOutlink(QueuedUri outlink) throws DbException {
        outlinkQueue.add(outlink);
    }

    private void calculateDelay() throws DbQueryException {
        if (delayMs < 0) {
            delayMs = 0L;
            return;
        }

        float delayFactor = qUri.getCrawlHostGroup().getDelayFactor();
        long minTimeBetweenPageLoadMs = qUri.getCrawlHostGroup().getMinTimeBetweenPageLoadMs();
        long maxTimeBetweenPageLoadMs = qUri.getCrawlHostGroup().getMaxTimeBetweenPageLoadMs();
        if (delayFactor == 0f) {
            delayFactor = 1f;
        } else if (delayFactor < 0f) {
            delayFactor = 0f;
        }
        delayMs = (long) (fetchTimeMs * delayFactor);
        if (minTimeBetweenPageLoadMs > 0) {
            delayMs = Math.max(delayMs, minTimeBetweenPageLoadMs);
        }
        if (maxTimeBetweenPageLoadMs > 0) {
            delayMs = Math.min(delayMs, maxTimeBetweenPageLoadMs);
        }
    }

    public long getDelay(TimeUnit unit) {
        return unit.convert(delayMs, TimeUnit.MILLISECONDS);
    }
}
