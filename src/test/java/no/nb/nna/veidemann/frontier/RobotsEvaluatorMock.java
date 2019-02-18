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
import no.nb.nna.veidemann.api.dnsresolver.v1.DnsResolverGrpc;
import no.nb.nna.veidemann.api.dnsresolver.v1.ResolveReply;
import no.nb.nna.veidemann.api.dnsresolver.v1.ResolveRequest;
import no.nb.nna.veidemann.api.robotsevaluator.v1.IsAllowedReply;
import no.nb.nna.veidemann.api.robotsevaluator.v1.IsAllowedRequest;
import no.nb.nna.veidemann.api.robotsevaluator.v1.RobotsEvaluatorGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RobotsEvaluatorMock {
    private static final Logger LOG = LoggerFactory.getLogger(RobotsEvaluatorMock.class);

    final Server server;
    public RobotsEvaluatorMock(int port) {
        server = ServerBuilder.forPort(port).addService(new RobotsService()).build();
    }

    public void start() throws IOException {
        server.start();
    }

    public class RobotsService extends RobotsEvaluatorGrpc.RobotsEvaluatorImplBase {
        @Override
        public void isAllowed(IsAllowedRequest request, StreamObserver<IsAllowedReply> responseObserver) {
//            System.out.println("R " + request);
            responseObserver.onNext(IsAllowedReply.newBuilder().setIsAllowed(true).build());
            responseObserver.onCompleted();
        }
    }
}
