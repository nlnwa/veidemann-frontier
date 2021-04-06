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

import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
public class CrawlExecutionHelpers {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlExecutionHelpers.class);

    /**
     * Do some housekeeping.
     * </p>
     * This should be run regardless of if we fetched anything or if the fetch failed in any way.
     */
    public static void postFetchFinally(Frontier frontier, StatusWrapper status, QueuedUriWrapper qUri, long delayMs) {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());
        try {
            if (qUri.hasError() && qUri.getDiscoveryPath().isEmpty()) {
                if (qUri.getError().getCode() == ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode()) {
                    // Seed precluded by robots.txt; mark crawl as finished
                    endCrawl(frontier, status, State.FINISHED, qUri.getError());
                } else {
                    // Seed failed; mark crawl as failed
                    endCrawl(frontier, status, State.FAILED, qUri.getError());
                }
            } else if (frontier.getCrawlQueueManager().countByCrawlExecution(status.getId()) <= 0) {
                endCrawl(frontier, status, State.FINISHED);
            } else {
                status.setState(State.SLEEPING);
            }

            // Save updated status
            status.saveStatus();

            // Recheck if user aborted crawl while fetching last uri.
            if (isAborted(frontier, status)) {
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
            frontier.getCrawlQueueManager().releaseCrawlHostGroup(qUri.getCrawlHostGroup(), delayMs);
        } catch (Throwable t) {
            // An error here indicates unknown problems with DB communication. No idea how to handle that yet.
            LOG.error("Error releasing CrawlHostGroup: {}", t.toString(), t);
        }
    }

    public static void endCrawl(Frontier frontier, StatusWrapper status, State state) throws DbException {
        frontier.getCrawlQueueManager().removeCrawlExecutionFromTimeoutSchedule(status.getId());
        status.setEndState(state).saveStatus();
    }

    public static void endCrawl(Frontier frontier, StatusWrapper status, State state, Error error) throws DbException {
        frontier.getCrawlQueueManager().removeCrawlExecutionFromTimeoutSchedule(status.getId());
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

    public static boolean isAborted(Frontier frontier, StatusWrapper status) throws DbException {
        switch (status.getDesiredState()) {
            case ABORTED_MANUAL:
            case ABORTED_TIMEOUT:
            case ABORTED_SIZE:
                // Set end state to desired state
                status.setEndState(status.getDesiredState()).saveStatus();
                return true;
        }
        return false;
    }

}
