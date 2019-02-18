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

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.dnsresolver.v1.DnsResolverGrpc;
import no.nb.nna.veidemann.api.dnsresolver.v1.ResolveReply;
import no.nb.nna.veidemann.api.dnsresolver.v1.ResolveRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DnsResolverMock {
    private static final Logger LOG = LoggerFactory.getLogger(DnsResolverMock.class);
    Pattern seedNumPattern = Pattern.compile("stress-(\\d)(\\d)(\\d).com");

    final Server server;
    public DnsResolverMock(int port) {
        server = ServerBuilder.forPort(port).addService(new DnsService()).build();
    }

    public void start() throws IOException {
        server.start();
    }

    public class DnsService extends DnsResolverGrpc.DnsResolverImplBase {
        @Override
        public void resolve(ResolveRequest request, StreamObserver<ResolveReply> responseObserver) {
            Matcher m = seedNumPattern.matcher(request.getHost());
            if (!m.matches()) {
                System.out.println("Regex error");
            }

            String textualIp = String.format("127.%s.%s.%s", m.group(1), m.group(2), m.group(3));
            InetAddress ip = null;
            try {
                ip = InetAddress.getByName(textualIp);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            byte[] bytes = ip.getAddress();
            responseObserver.onNext(ResolveReply.newBuilder()
                    .setHost(request.getHost())
                    .setPort(request.getPort())
                    .setTextualIp(textualIp)
                    .setRawIp(ByteString.copyFrom(bytes))
                    .build());
            responseObserver.onCompleted();
        }
    }
}
