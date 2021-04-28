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
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
public class ErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorHandler.class);

    /**
     * Do post proccessing after a failed fetch.
     *
     * @param error the Error causing the failure
     */
    public static PreconditionState fetchFailure(Frontier frontier, StatusWrapper status, QueuedUriWrapper qUri, Error error) throws DbException {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        qUri.setError(error);
        ExtraStatusCodes eCode = ExtraStatusCodes.fromFetchError(error);
        if (eCode.isTemporary()) {
            qUri.incrementRetries();

            if (LimitsCheck.isRetryLimitReached(qUri)) {
                LOG.info("Failed fetch of {} ({} {}) at attempt #{}. Uri will not be retried due to retry limit",
                        qUri.getUri(), error.getCode(), error.getMsg(), qUri.getRetries());
                frontier.writeLog(frontier, qUri, ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode());
                status.incrementDocumentsFailed();
            } else {
                LOG.info("Failed fetch of {} ({} {}) at attempt #{}, retrying in {} seconds",
                        qUri.getUri(), error.getCode(), error.getMsg(), qUri.getRetries(), qUri.getCrawlHostGroup().getRetryDelaySeconds());
                qUri.setPriorityWeight(status.getCrawlConfig().getCrawlConfig().getPriorityWeight());
                qUri.setEarliestFetchDelaySeconds(qUri.getCrawlHostGroup().getRetryDelaySeconds());
                status.incrementDocumentsRetried();
                return PreconditionState.RETRY;
            }
        } else {
            LOG.info("Failed fetch of {} ({} {}) at attempt #{}. Uri will not be retried because error is permanent",
                    qUri.getUri(), error.getCode(), error.getMsg(), qUri.getRetries());
            status.incrementDocumentsFailed();
        }
        return PreconditionState.DENIED;
    }
}
