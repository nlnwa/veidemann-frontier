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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

public class OutlinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(OutlinkHandler.class);

    private OutlinkHandler() {
    }

    /**
     * Check if outlink is in scope for crawling and eventually add it to queue.
     *
     * @param frontier
     * @param outlink  the outlink to evaluate
     * @return true if outlink was added to queue
     * @throws DbException
     */
    public static boolean processOutlink(Frontier frontier, StatusWrapper status, QueuedUriWrapper parentUri,
                                         QueuedUri outlink, Collection<Annotation> scriptParameters, ConfigRef scopeScriptRef)
            throws DbException {

        boolean wasQueued = false;
        Tracer tracer = frontier.getTracer();
        Span span = tracer.buildSpan("processOutlink")
                .withTag(Tags.COMPONENT, "Frontier")
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_SERVER)
                .withTag("uri", outlink.getUri())
                .start();
        try (Scope scope = tracer.scopeManager().activate(span)) {
            String canonicalizedUri = frontier.getScopeServiceClient().canonicalize(outlink.getUri());
            QueuedUri.Builder ol = outlink.toBuilder()
                    .setUri(canonicalizedUri);

            QueuedUriWrapper outUri = QueuedUriWrapper.getOutlinkQueuedUriWrapper(frontier, parentUri, ol,
                    null, scriptParameters, scopeScriptRef);

            PreconditionState check = Preconditions.checkPreconditions(frontier,
                    status.getCrawlConfig(), status, outUri).get();
            switch (check) {
                case OK:
                    LOG.debug("Found new URI: {}, queueing.", outUri.getUri());
                    outUri.setPriorityWeight(status.getCrawlConfig().getCrawlConfig().getPriorityWeight());
                    if (outUri.addUriToQueue()) {
                        wasQueued = true;
                    }
                    break;
                case RETRY:
                    LOG.debug("Failed preconditions for: {}, queueing for retry.", outUri.getUri());
                    outUri.setPriorityWeight(status.getCrawlConfig().getCrawlConfig().getPriorityWeight());
                    if (outUri.addUriToQueue()) {
                        wasQueued = true;
                    }
                    break;
                case DENIED:
                    break;
            }
        } catch (URISyntaxException ex) {
            status.incrementDocumentsFailed();
            LOG.info("Illegal URI {}", ex);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error(e.toString(), e);
        } finally {
            span.finish();
        }
        return wasQueued;
    }
}
