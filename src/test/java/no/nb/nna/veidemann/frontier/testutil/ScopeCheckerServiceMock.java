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

package no.nb.nna.veidemann.frontier.testutil;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.commons.v1.ParsedUri;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckRequest;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckResponse;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckResponse.Evaluation;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopesCheckerServiceGrpc;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.CanonicalizeRequest;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.CanonicalizeResponse;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.UriCanonicalizerServiceGrpc;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class ScopeCheckerServiceMock implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ScopeCheckerServiceMock.class);

    final Server server;
    public final RequestLog requestLog = new RequestLog();
    private final RequestMatcher denialForUrl = new RequestMatcher(requestLog);
    int maxHopsFromSeed = 0;
    boolean allowOffSeedHops = false;

    public ScopeCheckerServiceMock(int port) {
        server = ServerBuilder.forPort(port)
                .addService(new ScopeCheckerService())
                .addService(new UriCanonicalizerService())
                .build();
    }

    public ScopeCheckerServiceMock withMaxHopsFromSeed(int maxHopsFromSeed) {
        this.maxHopsFromSeed = maxHopsFromSeed;
        return this;
    }

    public ScopeCheckerServiceMock withAllowOffSeedHops(boolean allowOffSeedHops) {
        this.allowOffSeedHops = allowOffSeedHops;
        return this;
    }

    public ScopeCheckerServiceMock withDenialForAllUrlRequests(String url) {
        this.denialForUrl.withMatchAllRequests(url);
        return this;
    }

    public ScopeCheckerServiceMock withDenialForUrlRequests(String url, int from, int to) {
        this.denialForUrl.withMatchRequests(url, from, to);
        return this;
    }

    public ScopeCheckerServiceMock start() throws IOException {
        server.start();
        return this;
    }

    @Override
    public void close() throws Exception {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }

    public class UriCanonicalizerService extends UriCanonicalizerServiceGrpc.UriCanonicalizerServiceImplBase {
        /**
         * <pre>
         * Canonicalize URI for crawling.
         * Examples of canonicalization could be:
         * * Remove port numbers for well known schemes (i.e. http://example.com:80 =&gt; http://example.com)
         * * Normalize slash for empty path (i.e. http://example.com =&gt; http://example.com/)
         * * Normalize path (i.e. http://example.com/a//b/./c =&gt; http://example.com/a/b/c)
         * </pre>
         *
         * @param request
         * @param responseObserver
         */
        @Override
        public void canonicalize(CanonicalizeRequest request, StreamObserver<CanonicalizeResponse> responseObserver) {
            try {
                String qUri = request.getUri();
                URL u = new URL(qUri);
                ParsedUri pUri = ParsedUri.newBuilder()
                        .setHref(qUri)
                        .setScheme(u.getProtocol())
                        .setHost(u.getHost())
                        .setPort(80)
                        .setPath(u.getPath())
                        .build();
                responseObserver.onNext(CanonicalizeResponse.newBuilder()
                        .setUri(pUri)
                        .build());
                responseObserver.onCompleted();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
    }

    public class ScopeCheckerService extends ScopesCheckerServiceGrpc.ScopesCheckerServiceImplBase {
        /**
         * <pre>
         * Check if URI is in scope for this crawl
         * </pre>
         *
         * @param request
         * @param responseObserver
         */
        @Override
        public void scopeCheck(ScopeCheckRequest request, StreamObserver<ScopeCheckResponse> responseObserver) {
            String qUri = request.getQueuedUri().getUri();
            String seed = request.getQueuedUri().getSeedUri();

            // Add request to documentLog
            requestLog.addRequest(qUri);

            try {
                ScopeCheckResponse.Builder response = ScopeCheckResponse.newBuilder().setEvaluation(Evaluation.INCLUDE);

                if (request.getQueuedUri().getDiscoveryPath().length() > maxHopsFromSeed) {
                    // Limit depth
                    response.setEvaluation(Evaluation.EXCLUDE)
                            .setExcludeReason(ExtraStatusCodes.TOO_MANY_HOPS.getCode())
                            .setError(ExtraStatusCodes.TOO_MANY_HOPS.toFetchError());
                } else if (!allowOffSeedHops && !qUri.startsWith(seed)) {
                    // Deny off host hops
                    response.setEvaluation(Evaluation.EXCLUDE)
                            .setExcludeReason(ExtraStatusCodes.BLOCKED.getCode())
                            .setError(ExtraStatusCodes.BLOCKED.toFetchError());
                } else if (denialForUrl.match(qUri)) {
                    // Limit specific host, will reject seed
                    response.setEvaluation(Evaluation.EXCLUDE)
                            .setExcludeReason(ExtraStatusCodes.BLOCKED.getCode())
                            .setError(ExtraStatusCodes.BLOCKED.toFetchError());
                }

                URL u = new URL(qUri);
                ParsedUri icUri = ParsedUri.newBuilder()
                        .setHref(qUri)
                        .setScheme(u.getProtocol())
                        .setHost(u.getHost())
                        .setPort(80)
                        .setPath(u.getPath())
                        .build();
                responseObserver.onNext(response
                        .setIncludeCheckUri(icUri)
                        .build());
                responseObserver.onCompleted();
                LOG.debug("Scope evaluated to {} for '{}'.", response.getEvaluation(), qUri);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
    }
}
