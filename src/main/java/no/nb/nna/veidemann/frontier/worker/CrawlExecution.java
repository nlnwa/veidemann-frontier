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

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvest.Metrics;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class CrawlExecution {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlExecution.class);

    final StatusWrapper status;

    final Frontier frontier;

    final ConfigObject crawlConfig;

    final ConfigObject politenessConfig;

    final ConfigObject collectionConfig;

    final CrawlLimitsConfig limits;

    final QueuedUriWrapper qUri;

    final CrawlHostGroup crawlHostGroup;

    private final OutlinkHandler outlinkHandler;

    /**
     * If true, this execution has added something to the queue. If so, No need to check if execution is finished
     */
    private boolean uriAdded;

    private long delayMs = 0L;

    private long fetchStart;

    private long fetchTimeMs = 0L;

    private Span span;

    private AtomicBoolean done = new AtomicBoolean();

    private AtomicBoolean finalized = new AtomicBoolean();

    public CrawlExecution(QueuedUri queuedUri, CrawlHostGroup crawlHostGroup, Frontier frontier) throws DbException {
        this.status = StatusWrapper.getStatusWrapper(frontier, queuedUri.getExecutionId());
        ConfigObject job = frontier.getConfig(status.getJobId());
        this.crawlConfig = frontier.getConfig(job.getCrawlJob().getCrawlConfigRef());
        this.collectionConfig = frontier.getConfig(crawlConfig.getCrawlConfig().getCollectionRef());
        try {
            this.qUri = QueuedUriWrapper.getQueuedUriWrapper(frontier, queuedUri, collectionConfig.getMeta().getName()).clearError();
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
        this.status.addCurrentUri(this.qUri).saveStatus();

        this.crawlHostGroup = crawlHostGroup;
        this.frontier = frontier;
        this.politenessConfig = frontier.getConfig(crawlConfig.getCrawlConfig().getPolitenessRef());
        this.limits = job.getCrawlJob().getLimits();
        this.outlinkHandler = new OutlinkHandler(this);
    }

    public String getId() {
        return status.getId();
    }

    public CrawlHostGroup getCrawlHostGroup() {
        return crawlHostGroup;
    }

    public QueuedUriWrapper getUri() {
        return qUri;
    }

    /**
     * Check if crawl is aborted.
     * </p>
     *
     * @return true if we should do the fetch
     */
    public PageHarvestSpec preFetch() {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        span = GlobalTracer.get()
                .buildSpan("runNextFetch")
                .withTag(Tags.COMPONENT.getKey(), "CrawlExecution")
                .withTag("uri", qUri.getUri())
                .withTag("executionId", getId())
                .ignoreActiveSpan()
                .startManual();

        try {
            LimitsCheck.isLimitReached(frontier, limits, status, qUri);
            if (isAborted()) {
                delayMs = -1L;
                status.removeCurrentUri(qUri);
                if (qUri.hasError() && qUri.getDiscoveryPath().isEmpty()) {
                    if (qUri.getError().getCode() == ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode()) {
                        // Seed precluded by robots.txt; mark crawl as finished
                        endCrawl(CrawlExecutionStatus.State.FINISHED, qUri.getError());
                    } else {
                        // Seed failed; mark crawl as failed
                        endCrawl(CrawlExecutionStatus.State.FAILED, qUri.getError());
                    }
                } else if (frontier.getCrawlQueueManager().countByCrawlExecution(getId()) == 0) {
                    // No URIs in queue; mark crawl as finished
                    endCrawl(CrawlExecutionStatus.State.FINISHED);
                }
                return null;
            }

            if (qUri.isUnresolved()) {
                QueuedUri preCheckUri = qUri.getQueuedUri();
                PreconditionState check = Preconditions.checkPreconditions(frontier, crawlConfig, status, qUri);
                switch (check) {
                    case DENIED:
                    case FAIL:
                        delayMs = -1L;
                        frontier.getCrawlQueueManager().removeQUri(preCheckUri, crawlHostGroup.getId(), false);
                        status.removeCurrentUri(qUri).saveStatus();
                        return null;
                    case RETRY:
                        qUri.setPriorityWeight(this.crawlConfig.getCrawlConfig().getPriorityWeight());
                        frontier.getCrawlQueueManager().removeQUri(preCheckUri, crawlHostGroup.getId(), false);
                        uriAdded = true;
                        qUri.addUriToQueue();
                        return null;
                    case OK:
                        // IP resolution done, requeue to account for politeness
                        frontier.getCrawlQueueManager().removeQUri(preCheckUri, crawlHostGroup.getId(), false);
                        qUri.setPriorityWeight(this.crawlConfig.getCrawlConfig().getPriorityWeight());
                }
            }

            LOG.debug("Fetching " + qUri.getUri());
            fetchStart = System.currentTimeMillis();

            return PageHarvestSpec.newBuilder()
                    .setQueuedUri(qUri.getQueuedUri())
                    .setCrawlConfig(crawlConfig)
                    .build();

        } catch (DbException e) {
            LOG.error("Failed communicating with DB: {}", e.toString(), e);
            delayMs = -1L;
            return null;
        } catch (Throwable t) {
            // Errors should be handled elsewhere. Exception here indicates a bug.
            LOG.error("Possible bug: {}", t.toString(), t);
            delayMs = -1L;
            return null;
        }
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

            LOG.info("Failed fetch of {}: {} {}", qUri.getUri(), error.getCode(), error.getMsg());
            retryUri(qUri, error);
        }
    }

    /**
     * Do post proccessing after a failed fetch.
     *
     * @param t the exception thrown
     */
    public void postFetchFailure(Throwable t) throws DbException {
        if (done.compareAndSet(false, true)) {
            MDC.put("eid", qUri.getExecutionId());
            MDC.put("uri", qUri.getUri());

            LOG.info("Failed fetch of {}", qUri.getUri(), t);
            retryUri(qUri, ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
        }
    }

    /**
     * Do some housekeeping.
     * </p>
     * This should be run regardless of if we fetched anything or if the fetch failed in any way.
     */
    public void postFetchFinally() {
        if (finalized.compareAndSet(false, true)) {

            MDC.put("eid", qUri.getExecutionId());
            MDC.put("uri", qUri.getUri());

            try {
                long fetchEnd = System.currentTimeMillis();
                fetchTimeMs = fetchEnd - fetchStart;
                calculateDelay();

                if (qUri.hasError() && qUri.getDiscoveryPath().isEmpty()) {
                    if (qUri.getError().getCode() == ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode()) {
                        // Seed precluded by robots.txt; mark crawl as finished
                        endCrawl(CrawlExecutionStatus.State.FINISHED, qUri.getError());
                    } else {
                        // Seed failed; mark crawl as failed
                        endCrawl(CrawlExecutionStatus.State.FAILED, qUri.getError());
                    }
                } else if (!uriAdded && frontier.getCrawlQueueManager().countByCrawlExecution(getId()) == 0) {
                    endCrawl(CrawlExecutionStatus.State.FINISHED);
                }

                // Save updated status
                status.saveStatus();

                // Recheck if user aborted crawl while fetching last uri.
                if (isAborted()) {
                    delayMs = 0L;
                }
            } catch (DbException e) {
                // An error here indicates problems with DB communication. No idea how to handle that yet.
                LOG.error("Error updating status after fetch: {}", e.toString(), e);
            } catch (Throwable e) {
                // Catch everything to ensure crawl host group gets released.
                // Discovering this message in logs should be investigated as a possible bug.
                LOG.error("Unknown error in post fetch. Might be a bug", e);
            }

            try {
                frontier.getCrawlQueueManager().releaseCrawlHostGroup(getCrawlHostGroup(), getDelay(TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
                // An error here indicates unknown problems with DB communication. No idea how to handle that yet.
                LOG.error("Error releasing CrawlHostGroup: {}", t.toString(), t);
            }
            span.finish();
        }
    }

    public void queueOutlink(QueuedUri outlink) throws DbException {
        if (outlinkHandler.processOutlink(frontier, outlink)) {
            uriAdded = true;
        }
    }

    private void calculateDelay() {
        if (delayMs == -1) {
            delayMs = 0L;
            return;
        }

        float delayFactor = politenessConfig.getPolitenessConfig().getDelayFactor();
        long minTimeBetweenPageLoadMs = politenessConfig.getPolitenessConfig().getMinTimeBetweenPageLoadMs();
        long maxTimeBetweenPageLoadMs = politenessConfig.getPolitenessConfig().getMaxTimeBetweenPageLoadMs();
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

    private void retryUri(QueuedUriWrapper qUri, Error error) throws DbException {
        ExtraStatusCodes eCode = ExtraStatusCodes.fromFetchError(error);
        if (eCode.isTemporary()) {
            QueuedUri oldUri = qUri.getQueuedUri();

            qUri.incrementRetries()
                    .setEarliestFetchDelaySeconds(politenessConfig.getPolitenessConfig().getRetryDelaySeconds())
                    .setError(error);

            if (LimitsCheck.isRetryLimitReached(politenessConfig, qUri)) {
                LOG.info("Failed fetching ({}) at attempt #{} due to retry limit", qUri, qUri.getRetries());
                status.incrementDocumentsFailed();
                status.removeCurrentUri(qUri).saveStatus();
            } else {
                LOG.info("Failed fetching ({}) at attempt #{}, retrying in {} seconds", qUri, qUri.getRetries(), politenessConfig.getPolitenessConfig().getRetryDelaySeconds());
                qUri.setPriorityWeight(this.crawlConfig.getCrawlConfig().getPriorityWeight());
                frontier.getCrawlQueueManager().removeQUri(oldUri, crawlHostGroup.getId(), false);
                uriAdded = true;
                qUri.addUriToQueue();
            }
        } else {
            LOG.info("Failed fetching ({}). URI will not be retried", qUri);
            qUri.setError(error);
            status.incrementDocumentsFailed();
            status.removeCurrentUri(qUri).saveStatus();
        }
    }

    private void endCrawl(CrawlExecutionStatus.State state) throws DbException {
        frontier.getCrawlQueueManager().removeCrawlExecutionFromTimeoutSchedule(status.getId());
        status.setEndState(state).saveStatus();
    }

    private void endCrawl(CrawlExecutionStatus.State state, Error error) throws DbException {
        if (status.getState() == State.FAILED) {
            status.setEndState(state)
                    .setError(error)
                    .incrementDocumentsDenied(frontier.getCrawlQueueManager()
                            .deleteQueuedUrisForExecution(status.getId()))
                    .saveStatus();
        } else {
            status.setEndState(state)
                    .setError(error)
                    .saveStatus();
        }
    }

    private boolean isAborted() throws DbException {
        switch (status.getState()) {
            case ABORTED_MANUAL:
            case ABORTED_TIMEOUT:
            case ABORTED_SIZE:
                status.incrementDocumentsDenied(frontier.getCrawlQueueManager()
                        .deleteQueuedUrisForExecution(status.getId()));

                // Re-set end state to ensure end time is updated
                status.setEndState(status.getState()).saveStatus();
                return true;
        }
        return false;
    }

}
