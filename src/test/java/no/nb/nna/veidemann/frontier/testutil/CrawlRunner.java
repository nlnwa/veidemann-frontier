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
import com.rethinkdb.net.Cursor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.frontier.settings.Settings;
import org.assertj.core.description.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class CrawlRunner implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CrawlRunner.class);

    ConfigAdapter c = DbService.getInstance().getConfigAdapter();
    ExecutionsAdapter e = DbService.getInstance().getExecutionsAdapter();
    RethinkDbConnection conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
    private final ManagedChannel frontierChannel;
    private final FrontierGrpc.FrontierBlockingStub frontierStub;
    private final RethinkDbData rethinkDbData;
    private final JedisPool jedisPool;
    private final Map<String, String> jobExecIdToJobName = new HashMap<>();

    public CrawlRunner(Settings settings, RethinkDbData rethinkDbData, JedisPool jedisPool) {
        frontierChannel = ManagedChannelBuilder.forAddress("localhost", settings.getApiPort()).usePlaintext().build();
        frontierStub = FrontierGrpc.newBlockingStub(frontierChannel).withWaitForReady();
        this.rethinkDbData = rethinkDbData;
        this.jedisPool = jedisPool;
    }

    public ConfigObject genJob(String name) throws DbException {
        return genJob(name, CrawlLimitsConfig.getDefaultInstance());
    }

    public ConfigObject genJob(String name, CrawlLimitsConfig limits) throws DbException {
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
        crawlJobBuilder.getMetaBuilder().setName(name);
        crawlJobBuilder.getCrawlJobBuilder()
                .setCrawlConfigRef(ApiTools.refForConfig(crawlConfig))
                .setScopeScriptRef(ApiTools.refForConfig(scopeScript))
                .setLimits(limits);
        return c.saveConfigObject(crawlJobBuilder.build());
    }

    public List<SeedAndExecutions> genSeeds(int count, String hostPrefix, ConfigObject... jobs) throws DbException {
        return genSeeds(0, count, hostPrefix, jobs);
    }

    public List<SeedAndExecutions> genSeeds(int offset, int count, String hostPrefix, ConfigObject... jobs) throws DbException {
        LOG.info("Generating {} seeds with prefix '{}'", count, hostPrefix);

        Set<ConfigRef> jobRefs = Arrays.stream(jobs).map(j -> ApiTools.refForConfig(j)).collect(Collectors.toSet());

        CompletionService submitSeedExecutor = new ExecutorCompletionService(ForkJoinPool.commonPool());
        SeedAndExecutions[] seeds = new SeedAndExecutions[count];

        for (int i = 0; i < count; i++) {
            int idx = i;
            String name = String.format("%s-%06d", hostPrefix, i + offset);
            String url = String.format("http://%s-%06d.com", hostPrefix, i + offset);

            submitSeedExecutor.submit(() -> {
                ConfigObject.Builder entityBuilder = ConfigObject.newBuilder()
                        .setApiVersion("v1")
                        .setKind(Kind.crawlEntity);
                entityBuilder.getMetaBuilder().setName(name);
                ConfigObject entity = c.saveConfigObject(entityBuilder.build());

                ConfigObject.Builder seedBuilder = ConfigObject.newBuilder()
                        .setApiVersion("v1")
                        .setKind(Kind.seed);
                seedBuilder.getMetaBuilder().setName(url);
                seedBuilder.getSeedBuilder()
                        .setEntityRef(ApiTools.refForConfig(entity))
                        .addAllJobRef(jobRefs);

                ConfigObject seed = c.saveConfigObject(seedBuilder.build());
                seeds[idx] = new SeedAndExecutions(seed, jobRefs);
                return null;
            });
        }
        for (int i = 0; i < count; i++) {
            try {
                submitSeedExecutor.take();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
        return Arrays.asList(seeds);
    }

    public RunningCrawl runCrawl(ConfigObject crawlJob, List<SeedAndExecutions> seeds) throws DbException {
        LOG.info("Submitting seeds to job '{}'", crawlJob.getMeta().getName());
        JobExecutionStatus jes = e.createJobExecutionStatus(crawlJob.getId());
        ForkJoinPool.commonPool().submit((Callable<Void>) () -> {
            for (SeedAndExecutions seed : seeds) {
                Builder requestBuilder = CrawlSeedRequest.newBuilder()
                        .setJob(crawlJob)
                        .setSeed(seed.seed)
                        .setJobExecutionId(jes.getId());
                CrawlExecutionId ceid = frontierStub.crawlSeed(requestBuilder.build());
                seed.crawlExecutions.get(crawlJob.getId()).set(ceid);
            }
            return null;
        });
        RunningCrawl c = new RunningCrawl();
        c.jobName = crawlJob.getMeta().getName();
        c.jes = jes;
        return c;
    }

    public void awaitCrawlFinished(RunningCrawl... runningCrawls) {
        awaitCrawlFinished(30, TimeUnit.SECONDS, runningCrawls);
    }

    public Duration awaitCrawlFinished(long timeout, TimeUnit unit, RunningCrawl... runningCrawls) {
        AtomicInteger emptyChgKeysCount = new AtomicInteger(0);
        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(timeout, unit)
                .until(() -> {
                    Set<String> chgKeys = jedisPool.getResource().keys("chg*");
                    if (chgKeys.isEmpty()) {
                        emptyChgKeysCount.incrementAndGet();
                    }

                    List<RunningCrawl> statuses = Arrays.stream(runningCrawls)
                            .map(j -> {
                                try {
                                    j.jes = e.getJobExecutionStatus(j.jes.getId());
                                    return j;
                                } catch (DbException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .filter(j -> j.jes.getState() == State.RUNNING)
                            .peek(j -> {
                                if (LOG.isTraceEnabled()) {
                                    LOG.trace("Job '{}' {}, Executions: CREATED={}, FETCHING={}, SLEEPING={}, FINISHED={}, ABORTED_TIMEOUT={}, ABORTED_SIZE={}, ABORTED_MANUAL={}, FAILED={}",
                                            j.jobName, j.jes.getState(),
                                            j.jes.getExecutionsStateMap().getOrDefault("CREATED", 0),
                                            j.jes.getExecutionsStateMap().getOrDefault("FETCHING", 0),
                                            j.jes.getExecutionsStateMap().getOrDefault("SLEEPING", 0),
                                            j.jes.getExecutionsStateMap().getOrDefault("FINISHED", 0),
                                            j.jes.getExecutionsStateMap().getOrDefault("ABORTED_TIMEOUT", 0),
                                            j.jes.getExecutionsStateMap().getOrDefault("ABORTED_SIZE", 0),
                                            j.jes.getExecutionsStateMap().getOrDefault("ABORTED_MANUAL", 0),
                                            j.jes.getExecutionsStateMap().getOrDefault("FAILED", 0));
                                }
                            }).collect(Collectors.toList());

                    if (statuses.stream().allMatch(j -> State.RUNNING != j.jes.getState())
                            && rethinkDbData.getQueuedUris().isEmpty()
                            && jedisPool.getResource().keys("*").size() <= 1) {
                        return true;
                    }
                    if (statuses.stream().anyMatch(j -> State.RUNNING == j.jes.getState())) {
                        Description desc = new Description() {
                            @Override
                            public String value() {
                                StringBuilder sb = new StringBuilder();
                                try (Jedis jedis = jedisPool.getResource()) {
                                    sb.append(String.format("Crawl is not finished, but redis chg keys are missing.\nRemaining REDIS keys: %s\n Queue count total: %s",
                                            jedis.keys("*"),
                                            jedis.get("QCT")));
                                    Cursor c = conn.exec("db-getQueuedUris", r.table(Tables.URI_QUEUE.name));
                                    c.forEach(v -> sb.append("\nURi in RethinkDB queue: ").append(v));
                                } catch (DbConnectionException dbConnectionException) {
                                    dbConnectionException.printStackTrace();
                                } catch (DbQueryException dbQueryException) {
                                    dbQueryException.printStackTrace();
                                }
                                return sb.toString();
                            }
                        };
                        assertThat(emptyChgKeysCount).as(desc).withFailMessage("").hasValueLessThan(3);
                    }
                    LOG.debug("Still running: {}", statuses.size());
                    return false;
                });
        return null;
    }

    @Override
    public void close() throws Exception {
        frontierChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static class SeedAndExecutions {
        final ConfigObject seed;
        Map<String, SettableFuture<CrawlExecutionId>> crawlExecutions = new HashMap<>();

        public SeedAndExecutions(ConfigObject seed, Collection<ConfigRef> jobRefs) {
            this.seed = seed;
            for (ConfigRef r : jobRefs) {
                crawlExecutions.put(r.getId(), SettableFuture.create());
            }
        }

        public ConfigObject getSeed() {
            return seed;
        }

        public SettableFuture<CrawlExecutionId> getCrawlExecution(ConfigObject job) {
            return crawlExecutions.get(job.getId());
        }
    }

    public static class RunningCrawl {
        String jobName;
        JobExecutionStatus jes;

        public JobExecutionStatus getStatus() {
            return jes;
        }
    }
}
