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

import com.google.common.util.concurrent.SettableFuture;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.PolitenessConfig.RobotsPolicy;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

public class SetupCrawl {
    ConfigAdapter c = DbService.getInstance().getConfigAdapter();
    ExecutionsAdapter e = DbService.getInstance().getExecutionsAdapter();
    public List<ConfigObject> seeds = new ArrayList<>();
    public Map<String, SettableFuture<CrawlExecutionId>> crawlExecutions = new HashMap<>();
    public ConfigObject crawlJob;

    public void setup(int seedCount) throws DbException {
        setup(seedCount, CrawlLimitsConfig.getDefaultInstance());
    }

    public void setup(int seedCount, CrawlLimitsConfig limits) throws DbException {
        ConfigObject.Builder defaultCrawlHostGroupConfig = c.getConfigObject(ConfigRef.newBuilder()
                .setKind(Kind.crawlHostGroupConfig).setId("chg-default")
                .build())
                .toBuilder();
        defaultCrawlHostGroupConfig.getCrawlHostGroupConfigBuilder()
                .setMinTimeBetweenPageLoadMs(1)
                .setMaxTimeBetweenPageLoadMs(10)
                .setDelayFactor(.5f)
                .setRetryDelaySeconds(2);
        c.saveConfigObject(defaultCrawlHostGroupConfig.build());

        ConfigObject.Builder politenessBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.politenessConfig);
        politenessBuilder.getMetaBuilder().setName("stress");
        politenessBuilder.getPolitenessConfigBuilder()
                .setRobotsPolicy(RobotsPolicy.OBEY_ROBOTS);
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
                .setPriorityWeight(1)
                .setPolitenessRef(ApiTools.refForConfig(politeness))
                .setBrowserConfigRef(ApiTools.refForConfig(browserConfig))
                .setCollectionRef(ApiTools.refForConfig(collection));
        ConfigObject crawlConfig = c.saveConfigObject(crawlConfigBuilder.build());

        ConfigObject.Builder scopeScriptBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.browserScript);
        scopeScriptBuilder.getMetaBuilder().setName("stress");
        ConfigObject scopeScript = c.saveConfigObject(scopeScriptBuilder.build());

        ConfigObject.Builder crawlJobBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlJob);
        crawlJobBuilder.getMetaBuilder().setName("stress");
        crawlJobBuilder.getCrawlJobBuilder()
                .setCrawlConfigRef(ApiTools.refForConfig(crawlConfig))
                .setScopeScriptRef(ApiTools.refForConfig(scopeScript))
                .setLimits(limits);
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

            String url = String.format("http://stress-%06d.com", i);
            ConfigObject.Builder seedBuilder = ConfigObject.newBuilder()
                    .setApiVersion("v1")
                    .setKind(Kind.seed);
            seedBuilder.getMetaBuilder().setName(url);
            seedBuilder.getSeedBuilder()
                    .setEntityRef(ApiTools.refForConfig(entity))
                    .addJobRef(jobRef);

            ConfigObject seed = c.saveConfigObject(seedBuilder.build());
            seeds.add(seed);
            crawlExecutions.put(seed.getId(), SettableFuture.create());
            System.out.print(".");
//            if (i == 10) {
//                seed = c.saveConfigObject(seedBuilder.build());
//                seeds.add(seed);
//            }
        }
        System.out.println(" DONE");
        System.out.flush();
        try {
            Thread.sleep(500);
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        }
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
                CrawlExecutionId ceid = frontierStub.crawlSeed(requestBuilder.build());
                crawlExecutions.get(seed.getId()).set(ceid);
                return null;
            });
            System.out.print(".");
        }
        System.out.println(" DONE");
        return jes;
    }
}