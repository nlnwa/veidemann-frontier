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

package no.nb.nna.veidemann.frontier;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class ScopeCheckerServiceMock {
    private static final Logger LOG = LoggerFactory.getLogger(ScopeCheckerServiceMock.class);

    final Server server;

    public ScopeCheckerServiceMock(int port) {
        server = ServerBuilder.forPort(port)
                .addService(new ScopeCheckerService())
                .addService(new UriCanonicalizerService())
                .build();
    }

    public void start() throws IOException {
        server.start();
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
            try {
                Evaluation eval = Evaluation.INCLUDE;
                if (request.getQueuedUri().getDiscoveryPath().length() > 2) {
                    eval = Evaluation.EXCLUDE;
                }
                String qUri = request.getQueuedUri().getUri();
                String seed = request.getQueuedUri().getSeedUri();
                if (!qUri.startsWith(seed)) {
                    eval = Evaluation.EXCLUDE;
                }

                URL u = new URL(qUri);
                ParsedUri icUri = ParsedUri.newBuilder()
                        .setHref(qUri)
                        .setScheme(u.getProtocol())
                        .setHost(u.getHost())
                        .setPort(80)
                        .setPath(u.getPath())
                        .build();
                responseObserver.onNext(ScopeCheckResponse.newBuilder()
                        .setEvaluation(eval)
                        .setIncludeCheckUri(icUri)
                        .build());
                responseObserver.onCompleted();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
    }
}
