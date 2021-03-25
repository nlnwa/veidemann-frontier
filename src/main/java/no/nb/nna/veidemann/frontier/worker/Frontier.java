/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.frontier.worker;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.client.OutOfScopeHandlerClient;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import no.nb.nna.veidemann.frontier.settings.Settings;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URISyntaxException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Frontier implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Frontier.class);

    private final Settings settings;

    private final RobotsServiceClient robotsServiceClient;

    private final DnsServiceClient dnsServiceClient;

    private final ScopeServiceClient scopeServiceClient;

    private final OutOfScopeHandlerClient outOfScopeHandlerClient;

    private final CrawlQueueManager crawlQueueManager;

    private final LoadingCache<ConfigRef, ConfigObject> configCache;

    private final ScriptParameterResolver scriptParameterResolver;

    private final ExecutorService preFetchThreadPool =
            new ThreadPoolExecutor(2, 64, 5, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    new ThreadFactoryBuilder().setNameFormat("prefetch-%d").build(), new CallerRunsPolicy());
    final ExecutorService postFetchThreadPool =
            new ThreadPoolExecutor(8, 64, 5, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    new ThreadFactoryBuilder().setNameFormat("postfetch-%d").build(), new CallerRunsPolicy());

    private final JedisPool jedisPool;
    final RethinkDbConnection conn;
    static final RethinkDB r = RethinkDB.r;

    public Frontier(Settings settings, JedisPool jedisPool, RobotsServiceClient robotsServiceClient, DnsServiceClient dnsServiceClient,
                    ScopeServiceClient scopeServiceClient, OutOfScopeHandlerClient outOfScopeHandlerClient) {
        this.settings = settings;
        this.jedisPool = jedisPool;
        this.robotsServiceClient = robotsServiceClient;
        this.dnsServiceClient = dnsServiceClient;
        this.scopeServiceClient = scopeServiceClient;
        this.outOfScopeHandlerClient = outOfScopeHandlerClient;
        conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
        this.crawlQueueManager = new CrawlQueueManager(this, conn, jedisPool);

        configCache = CacheBuilder.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<>() {
                            public ConfigObject load(ConfigRef key) throws DbException {
                                ConfigObject co = DbService.getInstance().getConfigAdapter()
                                        .getConfigObject(key);
                                if (co == null) {
                                    return ConfigObject.getDefaultInstance();
                                }
                                return co;
                            }
                        });
        scriptParameterResolver = new ScriptParameterResolver(this);
    }

    public ListenableFuture<CrawlExecutionStatus> scheduleSeed(CrawlSeedRequest request) {
        return Futures.submit(() -> {

            // Create crawl execution
            StatusWrapper status = StatusWrapper.getStatusWrapper(this,
                    createCrawlExecutionStatus(
                            request.getJob().getId(),
                            request.getJobExecutionId(),
                            request.getSeed().getId()));

            LOG.debug("New crawl execution: " + status.getId());

            preFetchThreadPool.submit(() -> {
                try {
                    preprocessAndQueueSeed(request, status);
                } catch (DbException e) {
                    e.printStackTrace();
                }
            });
            return status.getCrawlExecutionStatus();

        }, preFetchThreadPool);
    }

    public void preprocessAndQueueSeed(CrawlSeedRequest request, StatusWrapper status) throws DbException {
        String uri = request.getSeed().getMeta().getName();

        try {
            Collection<Annotation> scriptParameters = getScriptParameterResolver().GetScriptParameters(request.getSeed(), request.getJob());
            ConfigObject crawlConfig = getConfig(request.getJob().getCrawlJob().getCrawlConfigRef());
            ConfigObject collectionConfig = getConfig(crawlConfig.getCrawlConfig().getCollectionRef());
            QueuedUriWrapper qUri = QueuedUriWrapper.createSeedQueuedUri(this, uri, request.getJobExecutionId(),
                    status.getId(), crawlConfig.getCrawlConfig().getPolitenessRef(), collectionConfig.getMeta().getName(),
                    scriptParameters, request.getJob().getCrawlJob().getScopeScriptRef());
            qUri.setPriorityWeight(crawlConfig.getCrawlConfig().getPriorityWeight());

            ListenableFuture<PreconditionState> future = Preconditions.checkPreconditions(this, crawlConfig, status, qUri);
            Futures.transformAsync(future, c -> {
                switch (c) {
                    case DENIED:
                        if (status.getState() == State.ABORTED_MANUAL) {
                            // Job was aborted before crawl execution was created. Ignore
                        } else if (qUri.shouldInclude()) {
                            // Seed was in scope, but failed for other reason
                            LOG.warn("Seed '{}' could not be crawled. Error: {}", qUri.getUri(), qUri.getError());
                            status.setEndState(State.FAILED)
                                    .setError(qUri.getError())
                                    .incrementDocumentsFailed()
                                    .saveStatus();
                        } else {
                            // Seed is out of scope
                            LOG.warn("Seed '{}' could not be crawled. Status: {}, Error: {}", qUri.getUri(), qUri.getExcludedReasonStatusCode(), qUri.getExcludedError());
                            if (qUri.hasExcludedError()) {
                                status.setError(qUri.getExcludedError());
                            } else if (qUri.hasError()) {
                                status.setError(qUri.getError());
                            }
                            status.setEndState(State.FAILED)
                                    .incrementDocumentsDenied(1)
                                    .saveStatus();
                        }
                        return null;
                    case RETRY:
                        status.incrementDocumentsRetried();
                }

                // Prefetch ok, add to queue
                try {
                    boolean wasAdded = qUri.addUriToQueue(status);
                    if (wasAdded) {
                        LOG.debug("Seed '{}' added to queue", qUri.getUri());
                    } else {
                        LOG.warn("Seed could not be crawled, probably because another seed with same URL was already crawled. Error: {}", qUri.getError());
                        status.setEndState(CrawlExecutionStatus.State.FAILED);
                        if (qUri.hasError()) {
                            status.setError(qUri.getError());
                        }
                    }
                    status.saveStatus();

                    OffsetDateTime timeout = null;
                    if (request.hasTimeout()) {
                        timeout = ProtoUtils.tsToOdt(request.getTimeout());
                    } else {
                        long maxDuration = request.getJob().getCrawlJob().getLimits().getMaxDurationS();
                        if (maxDuration > 0) {
                            timeout = ProtoUtils.tsToOdt(status.getCreatedTime()).plus(maxDuration, ChronoUnit.SECONDS);
                        }
                    }
                    if (timeout != null) {
                        getCrawlQueueManager().scheduleCrawlExecutionTimeout(status.getId(), timeout);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }, MoreExecutors.directExecutor());
        } catch (URISyntaxException ex) {
            status.incrementDocumentsFailed()
                    .setEndState(CrawlExecutionStatus.State.FAILED)
                    .setError(ExtraStatusCodes.ILLEGAL_URI.toFetchError(ex.toString()))
                    .saveStatus();
        } catch (Exception ex) {
            LOG.warn(ex.toString(), ex);
            status.incrementDocumentsFailed()
                    .setEndState(CrawlExecutionStatus.State.FAILED)
                    .setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(ex.toString()))
                    .saveStatus();
        }
    }

    public RobotsServiceClient getRobotsServiceClient() {
        return robotsServiceClient;
    }

    public DnsServiceClient getDnsServiceClient() {
        return dnsServiceClient;
    }

    public CrawlQueueManager getCrawlQueueManager() {
        return crawlQueueManager;
    }

    public ScopeServiceClient getScopeServiceClient() {
        return scopeServiceClient;
    }

    public OutOfScopeHandlerClient getOutOfScopeHandlerClient() {
        return outOfScopeHandlerClient;
    }

    public ScriptParameterResolver getScriptParameterResolver() {
        return scriptParameterResolver;
    }

    /**
     * Get the settings object.
     * <p>
     *
     * @return the settings
     */
    public Settings getSettings() {
        return settings;
    }

    public CrawlExecutionStatus createCrawlExecutionStatus(String jobId, String jobExecutionId, String seedId) throws DbException {
        Objects.requireNonNull(jobId, "jobId must be set");
        Objects.requireNonNull(jobExecutionId, "jobExecutionId must be set");
        Objects.requireNonNull(seedId, "seedId must be set");

        CrawlExecutionStatus status = CrawlExecutionStatus.newBuilder()
                .setJobId(jobId)
                .setJobExecutionId(jobExecutionId)
                .setSeedId(seedId)
                .setState(CrawlExecutionStatus.State.CREATED)
                .build();

        Map rMap = ProtoUtils.protoToRethink(status);
        rMap.put("lastChangeTime", r.now());
        rMap.put("createdTime", r.now());
        // Set desiredState to ABORTED_MANUAL if JobExecution has desiredState ABORTED_MANUAL.
        rMap.put("desiredState", r.table(Tables.JOB_EXECUTIONS.name).get(jobExecutionId).g("desiredState").default_("")
                .do_(j -> r.branch(j.eq("ABORTED_MANUAL"), "ABORTED_MANUAL", "UNDEFINED")));

        crawlQueueManager.updateJobExecutionStatus(jobExecutionId, State.UNDEFINED, State.CREATED, CrawlExecutionStatusChange.getDefaultInstance());

        Insert qry = r.table(Tables.EXECUTIONS.name).insert(rMap);
        return conn.executeInsert("db-createExecutionStatus", qry, CrawlExecutionStatus.class);
    }

    public ConfigObject getConfig(ConfigRef ref) throws DbQueryException {
        try {
            return configCache.get(ref);
        } catch (ExecutionException e) {
            throw new DbQueryException(e);
        }
    }

    @Override
    public void close() {
        System.out.println("Shutting down Frontier");
        Future preFetchFuture = shutdownPool("preFetchPool", preFetchThreadPool, 60, TimeUnit.SECONDS);
        Future postFetchFuture = shutdownPool("postFetchPool", postFetchThreadPool, 60, TimeUnit.SECONDS);
        try {
            crawlQueueManager.close();
            preFetchFuture.get();
            postFetchFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("Frontier shut down");
    }

    public Future shutdownPool(String name, ExecutorService pool, long timeout, TimeUnit unit) {
        pool.shutdown(); // Disable new tasks from being submitted
        return ForkJoinPool.commonPool().submit(() -> {
            try {
                // Wait a while for existing tasks to terminate
                if (!pool.awaitTermination(timeout, unit)) {
                    pool.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!pool.awaitTermination(timeout, unit))
                        System.err.println(name + " did not terminate");
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                pool.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Check the health of the Frontier.
     *
     * @return the serving status of the Frontier
     */
    public ServingStatus checkHealth() {
        try (Jedis jedis = jedisPool.getResource()) {
            if (!"PONG".equals(jedis.ping())) {
                return ServingStatus.NOT_SERVING;
            }
        } catch (Throwable t) {
            return ServingStatus.NOT_SERVING;
        }
        return ServingStatus.SERVING;
    }
}
