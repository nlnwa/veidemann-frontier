/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.frontier.worker;

import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.netpreserve.commons.uri.UriFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

public class OutlinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(OutlinkHandler.class);

    public static final UriFormat SURT_HOST_FORMAT = new UriFormat()
            .surtEncoding(true)
            .ignoreScheme(true)
            .ignoreUser(true)
            .ignorePassword(true)
            .ignorePath(true)
            .ignoreQuery(true)
            .ignoreFragment(true)
            .decodeHost(true);

    private final CrawlExecution crawlExecution;

    public OutlinkHandler(CrawlExecution crawlExecution) {
        this.crawlExecution = crawlExecution;
    }

    /**
     * Check if outlink is in scope for crawling and eventually add it to queue.
     * @param frontier
     * @param outlink the outlink to evaluate
     * @return true if outlink was added to queue
     * @throws DbException
     */
    public boolean processOutlink(Frontier frontier, QueuedUri outlink) throws DbException {
        boolean wasQueued = false;
        try {
            QueuedUriWrapper outUri = QueuedUriWrapper.getQueuedUriWrapper(frontier, crawlExecution.qUri, outlink,
                    crawlExecution.collectionConfig.getMeta().getName());
            if (shouldInclude(outUri) && uriNotIncludedInQueue(outUri)) {
                outUri.setSequence(outUri.getDiscoveryPath().length());

                PreconditionState check = Preconditions.checkPreconditions(crawlExecution.frontier,
                        crawlExecution.crawlConfig, crawlExecution.status, outUri);
                switch (check) {
                    case OK:
                        LOG.debug("Found new URI: {}, queueing.", outUri.getSurt());
                        outUri.setPriorityWeight(crawlExecution.crawlConfig.getCrawlConfig().getPriorityWeight());
                        outUri.addUriToQueue();
                        wasQueued = true;
                        break;
                    case RETRY:
                        LOG.debug("Failed preconditions for: {}, queueing for retry.", outUri.getSurt());
                        outUri.setPriorityWeight(crawlExecution.crawlConfig.getCrawlConfig().getPriorityWeight());
                        outUri.addUriToQueue();
                        wasQueued = true;
                        break;
                    case FAIL:
                    case DENIED:
                        break;
                }
            }
        } catch (URISyntaxException ex) {
            crawlExecution.status.incrementDocumentsFailed();
            LOG.info("Illegal URI {}", ex);
        }
        return wasQueued;
    }

    private boolean shouldInclude(QueuedUriWrapper outlink) throws DbException {
        if (!LimitsCheck.isQueueable(crawlExecution.limits, crawlExecution.status, outlink)) {
            return false;
        }

        if (!crawlExecution.frontier.getScopeChecker().isInScope(crawlExecution.status, outlink)) {
            return false;
        }

        return true;
    }

    private boolean uriNotIncludedInQueue(QueuedUriWrapper outlink) throws DbException {
        if (outlink.frontier.getCrawlQueueManager()
                .uriNotIncludedInQueue(outlink.getQueuedUri())) {
            return true;
        }

        LOG.debug("Found already included URI: {}, skipping.", outlink.getSurt());
        return false;
    }
}
