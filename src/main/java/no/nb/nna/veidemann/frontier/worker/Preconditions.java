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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.PolitenessConfig.RobotsPolicy;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 */
public class Preconditions {

    private static final Logger LOG = LoggerFactory.getLogger(Preconditions.class);

    public enum PreconditionState {
        OK,
        DENIED,
        RETRY
    }

    private Preconditions() {
    }

    public static ListenableFuture<PreconditionState> checkPreconditions(Frontier frontier, ConfigObject crawlConfig, StatusWrapper status,
                                                                         QueuedUriWrapper qUri) throws DbException {

        qUri.clearError();

        if (!qUri.shouldInclude()) {
            LOG.debug("URI '{}' precluded by scope check. Reason: {}", qUri.getUri(), qUri.getExcludedReasonStatusCode());
            switch (qUri.getExcludedReasonStatusCode()) {
                case -5001:
                case -4001:
                    // Do not log BLOCKED and TOO_MANY_HOPS
                    break;
                default:
                    DbUtil.writeLog(qUri);
            }
            status.incrementDocumentsOutOfScope();
            frontier.getOutOfScopeHandlerClient().submitUri(qUri.getQueuedUri());
            return Futures.immediateFuture(PreconditionState.DENIED);
        }

        if (qUri.isUnresolved()) {
            SettableFuture<PreconditionState> future = SettableFuture.create();

            LOG.debug("Resolve ip for URI '{}'", qUri.getUri());
            Futures.addCallback(frontier.getDnsServiceClient()
                            .resolve(qUri.getHost(), qUri.getPort(), crawlConfig.getCrawlConfig().getCollectionRef()),
                    new ResolveDnsCallback(frontier, qUri, status, crawlConfig, future),
                    MoreExecutors.directExecutor());
            return future;
        } else {
            return Futures.immediateFuture(PreconditionState.OK);
        }

    }

    static class ResolveDnsCallback implements FutureCallback<InetSocketAddress> {
        private final Frontier frontier;
        private final QueuedUriWrapper qUri;
        private final StatusWrapper status;
        private final ConfigObject crawlConfig;
        private final SettableFuture<PreconditionState> future;

        public ResolveDnsCallback(Frontier frontier, QueuedUriWrapper qUri, StatusWrapper status, ConfigObject crawlConfig, SettableFuture<PreconditionState> future) {
            this.frontier = frontier;
            this.qUri = qUri;
            this.status = status;
            this.crawlConfig = crawlConfig;
            this.future = future;
        }

        @Override
        public void onSuccess(@Nullable InetSocketAddress result) {
            try {
                ConfigObject politeness = frontier.getConfig(crawlConfig.getCrawlConfig().getPolitenessRef());
                ConfigObject browserConfig = frontier.getConfig(crawlConfig.getCrawlConfig().getBrowserConfigRef());

                boolean changedCrawlHostGroup = false;
                if (!qUri.getCrawlHostGroupId().isEmpty() && !qUri.getQueuedUri().getId().isEmpty()) {
                    changedCrawlHostGroup = true;
                    frontier.getCrawlQueueManager().removeTmpCrawlHostGroup(qUri.getQueuedUri());
                }
                qUri.setIp(result.getAddress().getHostAddress());
                qUri.setResolved(politeness);

                // IP ok, check robots.txt
                if (checkRobots(frontier, browserConfig.getBrowserConfig().getUserAgent(), crawlConfig, politeness, qUri)) {
                    if (changedCrawlHostGroup) {
                        frontier.getCrawlQueueManager().addToCrawlHostGroup(qUri.getQueuedUri(), false);
                        future.set(PreconditionState.RETRY);
                    } else {
                        future.set(PreconditionState.OK);
                    }
                } else {
                    status.incrementDocumentsDenied(1L);
                    DbUtil.writeLog(qUri);
                    future.set(PreconditionState.DENIED);
                }
            } catch (DbException e) {
                future.setException(e);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            LOG.info("Failed ip resolution for URI '{}' by extracting host '{}' and port '{}'.",
                    qUri.getUri(),
                    qUri.getHost(),
                    qUri.getPort());

            try {
                qUri.setError(ExtraStatusCodes.FAILED_DNS.toFetchError(t.toString()))
                        .setEarliestFetchDelaySeconds(qUri.getCrawlHostGroup().getRetryDelaySeconds());
                PreconditionState state = ErrorHandler.fetchFailure(frontier, status, qUri, qUri.getError());
                if (state == PreconditionState.RETRY && !qUri.getCrawlHostGroupId().isEmpty() && !qUri.getQueuedUri().getId().isEmpty()) {
                    try {
                        qUri.save();
                    } catch (DbException e) {
                        LOG.error("Unable to update uri earliest fetch timestamp", e);
                    }
                }
                future.set(state);
            } catch (DbException e) {
                future.setException(e);
            }
        }
    }

    private static boolean checkRobots(Frontier frontier, String userAgent, ConfigObject crawlConfig, ConfigObject politeness,
                                       QueuedUriWrapper qUri) throws DbException {
        LOG.debug("Check robots.txt for URI '{}'", qUri.getUri());
        if (politeness.getPolitenessConfig().getRobotsPolicy() != RobotsPolicy.IGNORE_ROBOTS
                && !frontier.getRobotsServiceClient().isAllowed(qUri.getQueuedUri(), userAgent, politeness,
                crawlConfig.getCrawlConfig().getCollectionRef())) {
            LOG.info("URI '{}' precluded by robots.txt", qUri.getUri());
            qUri.setError(ExtraStatusCodes.PRECLUDED_BY_ROBOTS.toFetchError());
            return false;
        }
        return true;
    }

}
