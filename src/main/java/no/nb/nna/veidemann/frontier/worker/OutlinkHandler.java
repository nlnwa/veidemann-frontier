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

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.netpreserve.commons.uri.UriFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Set;

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

    Multimap<String, QueuedUriWrapper> queuedUriMap = MultimapBuilder.hashKeys().hashSetValues().build();

    public OutlinkHandler(CrawlExecution crawlExecution) {
        this.crawlExecution = crawlExecution;
    }

    public void queueOutlink(QueuedUri outlink) throws DbException {
        try {
            QueuedUriWrapper outUri = QueuedUriWrapper.getQueuedUriWrapper(crawlExecution.qUri, outlink,
                    crawlExecution.collectionConfig.getMeta().getName());
            if (shouldInclude(outUri)) {
                queuedUriMap.put(outUri.getParsedSurt().toCustomString(SURT_HOST_FORMAT), outUri);
            }
        } catch (URISyntaxException ex) {
            crawlExecution.status.incrementDocumentsFailed();
            LOG.info("Illegal URI {}", ex);
        }
    }

    public void finish() throws DbException {
        processOutlinks();
    }

    private void processOutlinks() throws DbException {
        while (!queuedUriMap.isEmpty()) {
            Set<String> keySet = queuedUriMap.keySet();
            if (keySet.size() == 1) {
                String key = keySet.iterator().next();
                DistributedLock lock = DbService.getInstance()
                        .createDistributedLock(new Key("quri", key), 10);
                lock.lock();
                try {
                    processOutlinks(queuedUriMap.get(key));
                    queuedUriMap.removeAll(key);
                } finally {
                    lock.unlock();
                }

            } else {
                for (String key : keySet.toArray(new String[0])) {
                    Collection<QueuedUriWrapper> v = queuedUriMap.get(key);
                    DistributedLock lock = DbService.getInstance()
                            .createDistributedLock(new Key("quri", key), 10);
                    if (lock.tryLock()) {
                        try {
                            processOutlinks(v);
                            queuedUriMap.removeAll(key);
                        } finally {
                            lock.unlock();
                        }
                    } else {
                        LOG.debug("Waiting for lock on " + key);
                    }
                }
            }
        }
    }

    private void processOutlinks(Collection<QueuedUriWrapper> outlinks) throws DbException {
        for (QueuedUriWrapper outUri : outlinks) {
            if (uriNotIncludedInQueue(outUri)) {
                outUri.setSequence(outUri.getDiscoveryPath().length());

                PreconditionState check = Preconditions.checkPreconditions(crawlExecution.frontier,
                        crawlExecution.crawlConfig, crawlExecution.status, outUri);
                switch (check) {
                    case OK:
                        LOG.debug("Found new URI: {}, queueing.", outUri.getSurt());
                        outUri.setPriorityWeight(crawlExecution.crawlConfig.getCrawlConfig().getPriorityWeight());
                        outUri.addUriToQueue();
                        break;
                    case RETRY:
                        LOG.debug("Failed preconditions for: {}, queueing for retry.", outUri.getSurt());
                        outUri.setPriorityWeight(crawlExecution.crawlConfig.getCrawlConfig().getPriorityWeight());
                        outUri.addUriToQueue();
                        break;
                    case FAIL:
                    case DENIED:
                        break;
                }
            }
        }
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
        if (DbService.getInstance().getCrawlQueueAdapter()
                .uriNotIncludedInQueue(outlink.getQueuedUri(), crawlExecution.status.getStartTime())) {
            return true;
        }

        LOG.debug("Found already included URI: {}, skipping.", outlink.getSurt());
        return false;
    }
}
