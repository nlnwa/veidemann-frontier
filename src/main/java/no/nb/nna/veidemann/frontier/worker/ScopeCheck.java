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

import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.commons.client.OutOfScopeHandlerClient;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 */
public class ScopeCheck {

    private static final Logger LOG = LoggerFactory.getLogger(ScopeCheck.class);

    private final OutOfScopeHandlerClient outOfScopeHandlerClient;

    public ScopeCheck(OutOfScopeHandlerClient outOfScopeHandlerClient) {
        this.outOfScopeHandlerClient = outOfScopeHandlerClient;
    }

    public boolean isInScope(StatusWrapper status, QueuedUriWrapper qUri) {
        if (!qUri.getSurt().startsWith(status.getScope().getSurtPrefix())) {
            LOG.debug("URI '{}' is out of scope, skipping.", qUri.getSurt());
            status.incrementDocumentsOutOfScope();
            outOfScopeHandlerClient.submitUri(qUri.getQueuedUri());
            return false;
        }
        String seedId = status.getCrawlExecutionStatus().getSeedId();
        try {
            ConfigObject seed = DbService.getInstance().getConfigAdapter().getConfigObject(ConfigRef.newBuilder()
                    .setKind(Kind.seed)
                    .setId(seedId)
                    .build());

            if (isExcluded(qUri.getSurt(), seed.getMeta().getAnnotationList())) {
                LOG.debug("URI '{}' is out of scope, skipping.", qUri.getSurt());
                status.incrementDocumentsOutOfScope();
                return false;
            }
        } catch (DbException e) {
            LOG.warn("Could not load Seed '{}'", seedId, e);
        }
        return true;
    }

    boolean isExcluded(String surt, List<Annotation> annotations) {
        for (Annotation annotation : annotations) {
            if ("v7n_scope-exclude".equals(annotation.getKey().trim().toLowerCase())) {
                for (String surtPrefix : annotation.getValue().split("\\s+")) {
                    if (surt.startsWith(surtPrefix.trim())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
