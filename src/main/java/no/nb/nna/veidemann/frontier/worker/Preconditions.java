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

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.PolitenessConfig.RobotsPolicy;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

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

    public static PreconditionState checkPreconditions(Frontier frontier, ConfigObject crawlConfig, StatusWrapper status,
                                                       QueuedUriWrapper qUri) throws DbException {

        if (!qUri.shouldInclude()) {
            LOG.info("URI '{}' precluded by scope check. Reason: {}", qUri.getUri(), qUri.getExcludedReasonStatusCode());
            DbUtil.writeLog(qUri);
            return PreconditionState.DENIED;
        }

        ConfigObject politeness = frontier.getConfig(crawlConfig.getCrawlConfig().getPolitenessRef());
        ConfigObject browserConfig = frontier.getConfig(crawlConfig.getCrawlConfig().getBrowserConfigRef());

        qUri.clearError();

        if (resolveDns(frontier, crawlConfig, politeness, qUri)) {
            qUri.setResolved(politeness);
        } else {
            DbUtil.writeLog(qUri);
            return PreconditionState.RETRY;
        }

        if (!checkRobots(frontier, browserConfig.getBrowserConfig().getUserAgent(), crawlConfig, politeness, qUri)) {
            status.incrementDocumentsDenied(1L);
            DbUtil.writeLog(qUri);
            return PreconditionState.DENIED;
        }

        return PreconditionState.OK;
    }

    private static boolean checkRobots(Frontier frontier, String userAgent, ConfigObject crawlConfig, ConfigObject politeness,
                                       QueuedUriWrapper qUri) throws DbException {
        LOG.debug("Check robots.txt for URI '{}'", qUri.getUri());
        // Check robots.txt
        if (politeness.getPolitenessConfig().getRobotsPolicy() != RobotsPolicy.IGNORE_ROBOTS
                && !frontier.getRobotsServiceClient().isAllowed(qUri.getQueuedUri(), userAgent, politeness,
                crawlConfig.getCrawlConfig().getCollectionRef())) {
            LOG.info("URI '{}' precluded by robots.txt", qUri.getUri());
            qUri = qUri.setError(ExtraStatusCodes.PRECLUDED_BY_ROBOTS.toFetchError());
            return false;
        }
        return true;
    }

    private static boolean resolveDns(Frontier frontier, ConfigObject crawlConfig, ConfigObject politeness, QueuedUriWrapper qUri) {
        if (!qUri.getIp().isEmpty()) {
            return true;
        }

        LOG.debug("Resolve ip for URI '{}'", qUri.getUri());

        try {
            String ip = frontier.getDnsServiceClient()
                    .resolve(qUri.getHost(), qUri.getPort(), crawlConfig.getCrawlConfig().getCollectionRef())
                    .getAddress()
                    .getHostAddress();
            qUri.setIp(ip);
            return true;
        } catch (UnknownHostException ex) {
            LOG.info("Failed ip resolution for URI '{}' by extracting host '{}' and port '{}'",
                    qUri.getUri(),
                    qUri.getHost(),
                    qUri.getPort(),
                    ex.getCause());

            qUri.setError(ExtraStatusCodes.FAILED_DNS.toFetchError(ex.toString()))
                    .setEarliestFetchDelaySeconds(politeness.getPolitenessConfig().getRetryDelaySeconds());
            return false;
        }
    }

}
