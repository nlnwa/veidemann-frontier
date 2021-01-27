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

import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collection;

public class OutlinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(OutlinkHandler.class);

    private final CrawlExecution crawlExecution;

    public OutlinkHandler(CrawlExecution crawlExecution) {
        this.crawlExecution = crawlExecution;
    }

    /**
     * Check if outlink is in scope for crawling and eventually add it to queue.
     *
     * @param frontier
     * @param outlink  the outlink to evaluate
     * @return true if outlink was added to queue
     * @throws DbException
     */
    public boolean processOutlink(
            Frontier frontier, QueuedUri outlink, Collection<Annotation> scriptParameters, ConfigRef scopeScriptRef)
            throws DbException {

        boolean wasQueued = false;
        try {
            QueuedUriWrapper outUri = QueuedUriWrapper.getOutlinkQueuedUriWrapper(frontier, crawlExecution.qUri, outlink,
                    crawlExecution.collectionConfig.getMeta().getName(), scriptParameters, scopeScriptRef);
            if (outUri.shouldInclude()) {
                outUri.setSequence(outUri.getDiscoveryPath().length());

                PreconditionState check = Preconditions.checkPreconditions(crawlExecution.frontier,
                        crawlExecution.crawlConfig, crawlExecution.status, outUri);
                switch (check) {
                    case OK:
                        LOG.debug("Found new URI: {}, queueing.", outUri.getUri());
                        outUri.setPriorityWeight(crawlExecution.crawlConfig.getCrawlConfig().getPriorityWeight());
                        if (outUri.addUriToQueue(crawlExecution.status)) {
                            wasQueued = true;
                        }
                        break;
                    case RETRY:
                        LOG.debug("Failed preconditions for: {}, queueing for retry.", outUri.getUri());
                        outUri.setPriorityWeight(crawlExecution.crawlConfig.getCrawlConfig().getPriorityWeight());
                        if (outUri.addUriToQueue(crawlExecution.status)) {
                            wasQueued = true;
                        }
                        break;
                    case DENIED:
                        break;
                }
            } else {
                outUri.logOutOfScope(crawlExecution.status);
            }
        } catch (URISyntaxException ex) {
            crawlExecution.status.incrementDocumentsFailed();
            LOG.info("Illegal URI {}", ex);
        }
        return wasQueued;
    }
}
