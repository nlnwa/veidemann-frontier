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

import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.ooshandler.v1.OosHandlerGrpc;
import no.nb.nna.veidemann.api.ooshandler.v1.SubmitUriRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class OutOfScopeHandlerMock implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OutOfScopeHandlerMock.class);

    final Server server;

    public OutOfScopeHandlerMock(int port) {
        server = ServerBuilder.forPort(port).addService(new OosHandler()).build();
    }

    public OutOfScopeHandlerMock start() throws IOException {
        server.start();
        return this;
    }

    @Override
    public void close() throws Exception {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }

    public class OosHandler extends OosHandlerGrpc.OosHandlerImplBase {
        @Override
        public void submitUri(SubmitUriRequest request, StreamObserver<Empty> responseObserver) {
//            System.out.println("OOS " + request.getUri().getUri());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
