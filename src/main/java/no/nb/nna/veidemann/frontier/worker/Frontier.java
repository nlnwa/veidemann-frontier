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
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.model.MapObject;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.client.DnsServiceClient;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Frontier implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Frontier.class);

    private final RobotsServiceClient robotsServiceClient;

    private final DnsServiceClient dnsServiceClient;

    private final ScopeServiceClient scopeServiceClient;

    private final OutOfScopeHandlerClient outOfScopeHandlerClient;

    private final CrawlQueueManager crawlQueueManager;

    private final LoadingCache<ConfigRef, ConfigObject> configCache;

    private final ScriptParameterResolver scriptParameterResolver;

    private final JedisPool jedisPool;
    final RethinkDbConnection conn;
    static final RethinkDB r = RethinkDB.r;

    public Frontier(JedisPool jedisPool, RobotsServiceClient robotsServiceClient, DnsServiceClient dnsServiceClient,
                    ScopeServiceClient scopeServiceClient, OutOfScopeHandlerClient outOfScopeHandlerClient) {
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
                        new CacheLoader<ConfigRef, ConfigObject>() {
                            public ConfigObject load(ConfigRef key) throws DbException {
                                return DbService.getInstance().getConfigAdapter()
                                        .getConfigObject(key);
                            }
                        });
        scriptParameterResolver = new ScriptParameterResolver(this);
    }

    ScheduledExecutorService x = Executors.newScheduledThreadPool(1);

    public CrawlExecutionStatus scheduleSeed(CrawlSeedRequest request) throws DbException {
        // Create execution
        StatusWrapper status = StatusWrapper.getStatusWrapper(this,
                createCrawlExecutionStatus(
                        request.getJob().getId(),
                        request.getJobExecutionId(),
                        request.getSeed().getId()));

        LOG.debug("New crawl execution: " + status.getId());

        String uri = request.getSeed().getMeta().getName();

        try {
            Collection<Annotation> scriptParameters = getScriptParameterResolver().GetScriptParameters(request.getSeed(), request.getJob());
            ConfigObject crawlConfig = getConfig(request.getJob().getCrawlJob().getCrawlConfigRef());
            ConfigObject collectionConfig = getConfig(crawlConfig.getCrawlConfig().getCollectionRef());
            QueuedUriWrapper qUri = QueuedUriWrapper.createSeedQueuedUri(this, uri, request.getJobExecutionId(),
                    status.getId(), crawlConfig.getCrawlConfig().getPolitenessRef(), collectionConfig.getMeta().getName(),
                    scriptParameters, request.getJob().getCrawlJob().getScopeScriptRef());
            qUri.setPriorityWeight(crawlConfig.getCrawlConfig().getPriorityWeight());
            boolean wasAdded = qUri.addUriToQueue(status);
            if (wasAdded) {
                LOG.debug("Seed '{}' added to queue", qUri.getUri());
            } else {
                LOG.warn("Seed could not be crawled. Status: {}, Error: {}", qUri.getExcludedReasonStatusCode(), qUri.getError());
                status.setEndState(CrawlExecutionStatus.State.FAILED);
                if (qUri.hasError()) {
                    status.setError(qUri.getError());
                }
                status.saveStatus();
            }
        } catch (URISyntaxException ex) {
            status.incrementDocumentsFailed()
                    .setEndState(CrawlExecutionStatus.State.FAILED)
                    .setError(ExtraStatusCodes.ILLEGAL_URI.toFetchError(ex.toString()))
                    .saveStatus();
        } catch (Exception ex) {
            status.incrementDocumentsFailed()
                    .setEndState(CrawlExecutionStatus.State.FAILED)
                    .setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(ex.toString()))
                    .saveStatus();
        }

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

        return status.getCrawlExecutionStatus();
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

        Insert qry = r.table(Tables.EXECUTIONS.name).insert(rMap);
        return conn.executeInsert("db-createExecutionStatus", qry, CrawlExecutionStatus.class);
    }

    public void updateJobExecution(String jobExecutionId) throws DbException {
        // Get a count of still running CrawlExecutions for this execution's JobExecution
        Long notEndedCount = conn.exec("db-updateJobExecution",
                r.table(Tables.EXECUTIONS.name)
                        .between(r.array(jobExecutionId, r.minval()), r.array(jobExecutionId, r.maxval()))
                        .optArg("index", "jobExecutionId_seedId")
                        .filter(row -> row.g("state").match("UNDEFINED|CREATED|FETCHING|SLEEPING"))
                        .count()
        );

        // If all CrawlExecutions are done for this JobExectuion, update the JobExecution with end statistics
        if (notEndedCount == 0) {
            LOG.debug("JobExecution '{}' finished, saving stats", jobExecutionId);
            crawlQueueManager.removeAlreadyIncludedQueue(jobExecutionId);

            // Fetch the JobExecutionStatus object this CrawlExecution is part of
            JobExecutionStatus jes = conn.executeGet("db-getJobExecutionStatus",
                    r.table(Tables.JOB_EXECUTIONS.name).get(jobExecutionId),
                    JobExecutionStatus.class);
            if (jes == null) {
                throw new IllegalStateException("Can't find JobExecution: " + jobExecutionId);
            }

            // Set JobExecution's status to FINISHED if it wasn't already aborted
            JobExecutionStatus.State state;
            switch (jes.getState()) {
                case DIED:
                case FAILED:
                case ABORTED_MANUAL:
                    state = jes.getState();
                    break;
                default:
                    state = JobExecutionStatus.State.FINISHED;
                    break;
            }

            // Update aggregated statistics
            Map sums = summarizeJobExecutionStats(jobExecutionId);
            JobExecutionStatus.Builder jesBuilder = jes.toBuilder()
                    .setState(state)
                    .setEndTime(ProtoUtils.getNowTs())
                    .setDocumentsCrawled((long) sums.get("documentsCrawled"))
                    .setDocumentsDenied((long) sums.get("documentsDenied"))
                    .setDocumentsFailed((long) sums.get("documentsFailed"))
                    .setDocumentsOutOfScope((long) sums.get("documentsOutOfScope"))
                    .setDocumentsRetried((long) sums.get("documentsRetried"))
                    .setUrisCrawled((long) sums.get("urisCrawled"))
                    .setBytesCrawled((long) sums.get("bytesCrawled"));

            for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                jesBuilder.putExecutionsState(s.name(), ((Long) sums.get(s.name())).intValue());
            }

            conn.exec("db-saveJobExecutionStatus",
                    r.table(Tables.JOB_EXECUTIONS.name).get(jesBuilder.getId()).update(ProtoUtils.protoToRethink(jesBuilder)));
        }
    }

    private Map summarizeJobExecutionStats(String jobExecutionId) throws DbException {
        String[] EXECUTIONS_STAT_FIELDS = new String[]{"documentsCrawled", "documentsDenied",
                "documentsFailed", "documentsOutOfScope", "documentsRetried", "urisCrawled", "bytesCrawled"};

        return conn.exec("db-summarizeJobExecutionStats",
                r.table(Tables.EXECUTIONS.name)
                        .between(r.array(jobExecutionId, r.minval()), r.array(jobExecutionId, r.maxval()))
                        .optArg("index", "jobExecutionId_seedId")
                        .map(doc -> {
                                    MapObject m = r.hashMap();
                                    for (String f : EXECUTIONS_STAT_FIELDS) {
                                        m.with(f, doc.getField(f).default_(0));
                                    }
                                    for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                                        m.with(s.name(), r.branch(doc.getField("state").eq(s.name()), 1, 0));
                                    }
                                    return m;
                                }
                        )
                        .reduce((left, right) -> {
                                    MapObject m = r.hashMap();
                                    for (String f : EXECUTIONS_STAT_FIELDS) {
                                        m.with(f, left.getField(f).add(right.getField(f)));
                                    }
                                    for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                                        m.with(s.name(), left.getField(s.name()).add(right.getField(s.name())));
                                    }
                                    return m;
                                }
                        ).default_((doc) -> {
                            MapObject m = r.hashMap();
                            for (String f : EXECUTIONS_STAT_FIELDS) {
                                m.with(f, 0);
                            }
                            for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                                m.with(s.name(), 0);
                            }
                            return m;
                        }
                )
        );
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
