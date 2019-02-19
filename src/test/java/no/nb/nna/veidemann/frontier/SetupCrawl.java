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

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest.Builder;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.commons.util.ApiTools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

public class SetupCrawl {
    ConfigAdapter c = DbService.getInstance().getConfigAdapter();
    ExecutionsAdapter e = DbService.getInstance().getExecutionsAdapter();
    List<ConfigObject> seeds = new ArrayList<>();
    ConfigObject crawlJob;

    public void setup(int seedCount) throws DbException {

        ConfigObject.Builder politenessBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.politenessConfig);
        politenessBuilder.getMetaBuilder().setName("stress");
        politenessBuilder.getPolitenessConfigBuilder().setDelayFactor(1f);
        ConfigObject politeness = c.saveConfigObject(politenessBuilder.build());

        ConfigObject.Builder browserConfigBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.browserConfig);
        browserConfigBuilder.getMetaBuilder().setName("stress");
        ConfigObject browserConfig = c.saveConfigObject(browserConfigBuilder.build());

        ConfigObject.Builder collectionBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.collection);
        collectionBuilder.getMetaBuilder().setName("stress");
        ConfigObject collection = c.saveConfigObject(collectionBuilder.build());

        ConfigObject.Builder crawlConfigBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlConfig);
        crawlConfigBuilder.getMetaBuilder().setName("stress");
        crawlConfigBuilder.getCrawlConfigBuilder()
                .setPolitenessRef(ApiTools.refForConfig(politeness))
                .setBrowserConfigRef(ApiTools.refForConfig(browserConfig))
                .setCollectionRef(ApiTools.refForConfig(collection));
        ConfigObject crawlConfig = c.saveConfigObject(crawlConfigBuilder.build());

        ConfigObject.Builder crawlJobBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlJob);
        crawlJobBuilder.getMetaBuilder().setName("stress");
        crawlJobBuilder.getCrawlJobBuilder()
                .setCrawlConfigRef(ApiTools.refForConfig(crawlConfig))
                .getLimitsBuilder().setDepth(2);
        crawlJob = c.saveConfigObject(crawlJobBuilder.build());

        genSeeds(ApiTools.refForConfig(crawlJob), seedCount);
    }

    public void genSeeds(ConfigRef jobRef, int count) throws DbException {
        System.out.print("Generating seeds ");
        for (int i = 0; i < count; i++) {
            ConfigObject.Builder entityBuilder = ConfigObject.newBuilder()
                    .setApiVersion("v1")
                    .setKind(Kind.crawlEntity);
            entityBuilder.getMetaBuilder().setName("stress-" + i);
            ConfigObject entity = c.saveConfigObject(entityBuilder.build());

            ConfigObject.Builder seedBuilder = ConfigObject.newBuilder()
                    .setApiVersion("v1")
                    .setKind(Kind.seed);
            seedBuilder.getMetaBuilder().setName(String.format("http://stress-%03d.com", i));
            seedBuilder.getSeedBuilder()
                    .setEntityRef(ApiTools.refForConfig(entity))
                    .addJobRef(jobRef);
            ConfigObject seed = c.saveConfigObject(seedBuilder.build());
            seeds.add(seed);
            System.out.print(".");
        }
        System.out.println(" DONE");
    }

    public JobExecutionStatus runCrawl(FrontierGrpc.FrontierBlockingStub frontierStub) throws DbException {
        System.out.print("Submitting seeds to job ");
        JobExecutionStatus jes = e.createJobExecutionStatus(crawlJob.getId());
        for (ConfigObject seed : seeds) {
            ForkJoinPool.commonPool().submit((Callable<Void>) () -> {
                Builder requestBuilder = CrawlSeedRequest.newBuilder()
                        .setJob(crawlJob)
                        .setSeed(seed)
                        .setJobExecutionId(jes.getId());
                frontierStub.crawlSeed(requestBuilder.build());
                return null;
            });
            System.out.print(".");
        }
        System.out.println(" DONE");
        return jes;
    }
}
