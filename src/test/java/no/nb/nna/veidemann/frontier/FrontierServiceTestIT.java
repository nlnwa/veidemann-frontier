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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class FrontierServiceTestIT {

    static String dbHost;

    static int dbPort;

    static String localIp;

    static int dnsresolverPort;

    static int robotsevaluatorPort;

    static String frontierHost;

    static int frontierPort;

    static ManagedChannel frontierChannel;

    static FrontierGrpc.FrontierBlockingStub frontierStub;

    static FrontierGrpc.FrontierStub frontierAsyncStub;

    @BeforeClass
    public static void init() throws DbConnectionException {
        dbHost = System.getProperty("db.host");
        dbPort = Integer.parseInt(System.getProperty("db.port"));
        localIp = System.getProperty("local.ip");
        dnsresolverPort = Integer.parseInt(System.getProperty("dnsresolver.port"));
        robotsevaluatorPort = Integer.parseInt(System.getProperty("robotsevaluator.port"));
        frontierHost = System.getProperty("frontier.host");
        frontierPort = Integer.parseInt(System.getProperty("frontier.port"));

        System.out.println("      DB: " + dbHost + ":" + dbPort);
        System.out.println("     DNS: " + localIp + ":" + dnsresolverPort);
        System.out.println("  ROBOTS: " + localIp + ":" + robotsevaluatorPort);
        System.out.println("FRONTIER: " + frontierHost + ":" + frontierPort);

        if (!DbService.isConfigured()) {
            CommonSettings dbSettings = new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword("");
            DbService.configure(dbSettings);
        }

        frontierChannel = ManagedChannelBuilder.forAddress(frontierHost, frontierPort).usePlaintext().build();
        frontierStub = FrontierGrpc.newBlockingStub(frontierChannel).withWaitForReady();
        frontierAsyncStub = FrontierGrpc.newStub(frontierChannel).withWaitForReady();
    }

    @Test
    public void stress() throws InterruptedException, DbException, IOException {
        SetupCrawl c = new SetupCrawl();
        c.setup(1000);

        DnsResolverMock dnsResolverMock = new DnsResolverMock(dnsresolverPort);
        dnsResolverMock.start();
        RobotsEvaluatorMock robotsEvaluatorMock = new RobotsEvaluatorMock(robotsevaluatorPort);
        robotsEvaluatorMock.start();
        HarvesterMock harvesterMock = new HarvesterMock(frontierAsyncStub);
        harvesterMock.start();

        JobExecutionStatus jes = c.runCrawl(frontierStub);

        while (true) {
            jes = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
            System.out.println("STATE " + jes.getExecutionsStateMap());
            Thread.sleep(5000);
            if (State.FINISHED == jes.getState()) {
                break;
            }
        }
        System.out.println("STATE " + jes.getExecutionsStateMap());
//        System.out.println(jes);
    }
}