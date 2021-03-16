package no.nb.nna.veidemann.frontier.api;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.commons.client.OutOfScopeHandlerClient;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.frontier.settings.Settings;
import no.nb.nna.veidemann.frontier.testutil.DnsResolverMock;
import no.nb.nna.veidemann.frontier.testutil.HarvesterMock;
import no.nb.nna.veidemann.frontier.testutil.OutOfScopeHandlerMock;
import no.nb.nna.veidemann.frontier.testutil.RedisData;
import no.nb.nna.veidemann.frontier.testutil.RethinkDbData;
import no.nb.nna.veidemann.frontier.testutil.RobotsEvaluatorMock;
import no.nb.nna.veidemann.frontier.testutil.ScopeCheckerServiceMock;
import no.nb.nna.veidemann.frontier.testutil.SetupCrawl;
import no.nb.nna.veidemann.frontier.testutil.SkipUntilFilter;
import no.nb.nna.veidemann.frontier.worker.DnsServiceClient;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.ScopeServiceClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import static com.rethinkdb.RethinkDB.r;
import static no.nb.nna.veidemann.frontier.testutil.FrontierAssertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@Tag("integration")
@Tag("redis")
@Tag("rethinkDb")
public class HarvestTest {
    private static final Logger LOG = LoggerFactory.getLogger(HarvestTest.class);

    ManagedChannel frontierChannel;
    FrontierGrpc.FrontierBlockingStub frontierStub;
    FrontierGrpc.FrontierStub frontierAsyncStub;
    RethinkDbConnection conn;
    FrontierApiServer apiServer;
    Frontier frontier;
    DnsResolverMock dnsResolverMock;
    RobotsEvaluatorMock robotsEvaluatorMock;
    OutOfScopeHandlerMock outOfScopeHandlerMock;
    ScopeCheckerServiceMock scopeCheckerServiceMock;
    HarvesterMock harvesterMock;
    RobotsServiceClient robotsServiceClient;
    DnsServiceClient dnsServiceClient;
    ScopeServiceClient scopeServiceClient;
    OutOfScopeHandlerClient outOfScopeHandlerClient;

    JedisPool jedisPool;
    RedisData redisData;
    RethinkDbData rethinkDbData;

    ExecutorService asyncFunctionsExecutor = new ThreadPoolExecutor(2, 128, 15, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("asyncFunc-%d").build(), new CallerRunsPolicy());

    private static Network network = Network.newNetwork();

    @Container
    public static GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:5.0.7-alpine"))
            .withExposedPorts(6379)
            .withLogConsumer(new SkipUntilFilter("Ready to accept connections", new Slf4jLogConsumer(LoggerFactory.getLogger("REDIS"))));

    @Container
    public static GenericContainer rethinkDb = new GenericContainer(DockerImageName.parse("rethinkdb:2.4.1-buster-slim"))
            .withNetwork(network)
            .withNetworkAliases("db")
            .withExposedPorts(28015);

    @Container
    public static GenericContainer dbInitializer = new GenericContainer(DockerImageName.parse("norsknettarkiv/veidemann-db-initializer:0"))
            .withNetwork(network)
            .dependsOn(rethinkDb)
            .withEnv("DB_HOST", "db")
            .withEnv("DB_PORT", "28015")
            .withEnv("DB_USER", "admin")
            .withStartupCheckStrategy(
                    new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(60))
            );

    @BeforeEach
    public void setup() throws DbQueryException, DbConnectionException, IOException {
        Settings settings = new Settings();
        settings.setDnsResolverHost(System.getProperty("dnsresolver.host", "localhost"));
        settings.setDnsResolverPort(Integer.parseInt(System.getProperty("dnsresolver.port", "9500")));
        settings.setRobotsEvaluatorHost(System.getProperty("robotsevaluator.host", "localhost"));
        settings.setRobotsEvaluatorPort(Integer.parseInt(System.getProperty("robotsevaluator.port", "9501")));
        settings.setOutOfScopeHandlerHost(System.getProperty("ooshandler.host", "localhost"));
        settings.setOutOfScopeHandlerPort(Integer.parseInt(System.getProperty("ooshandler.port", "9502")));
        settings.setScopeserviceHost(System.getProperty("scopeChecker.host", "localhost"));
        settings.setScopeservicePort(Integer.parseInt(System.getProperty("scopeChecker.port", "9503")));
        settings.setDbHost(rethinkDb.getHost());
        settings.setDbPort(rethinkDb.getFirstMappedPort());
        settings.setDbName("veidemann");
        settings.setDbUser("admin");
        settings.setDbPassword("");
        settings.setBusyTimeout(Duration.ofSeconds(2));
        settings.setApiPort(Integer.parseInt(System.getProperty("frontier.port", "9504")));
        settings.setTerminationGracePeriodSeconds(10);
        settings.setRedisHost(redis.getHost());
        settings.setRedisPort(redis.getFirstMappedPort());

        frontierChannel = ManagedChannelBuilder.forAddress("localhost", settings.getApiPort()).usePlaintext().build();
        frontierStub = FrontierGrpc.newBlockingStub(frontierChannel).withWaitForReady();
        frontierAsyncStub = FrontierGrpc.newStub(frontierChannel).withWaitForReady();
        dnsResolverMock = new DnsResolverMock(settings.getDnsResolverPort()).start();
        robotsEvaluatorMock = new RobotsEvaluatorMock(settings.getRobotsEvaluatorPort()).start();
        outOfScopeHandlerMock = new OutOfScopeHandlerMock(settings.getOutOfScopeHandlerPort()).start();
        scopeCheckerServiceMock = new ScopeCheckerServiceMock(settings.getScopeservicePort()).start();
        harvesterMock = new HarvesterMock(frontierAsyncStub).start();

        robotsServiceClient = new RobotsServiceClient(settings.getRobotsEvaluatorHost(), settings.getRobotsEvaluatorPort());
        dnsServiceClient = new DnsServiceClient(settings.getDnsResolverHost(), settings.getDnsResolverPort(), asyncFunctionsExecutor);
        scopeServiceClient = new ScopeServiceClient(settings.getScopeserviceHost(), settings.getScopeservicePort());
        outOfScopeHandlerClient = new OutOfScopeHandlerClient(settings.getOutOfScopeHandlerHost(), settings.getOutOfScopeHandlerPort());


        if (!DbService.isConfigured()) {
            CommonSettings dbSettings = new CommonSettings()
                    .withDbHost(rethinkDb.getHost())
                    .withDbPort(rethinkDb.getFirstMappedPort())
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword("");
            DbService.configure(dbSettings);
        }
        conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(24);
        jedisPool = new JedisPool(jedisPoolConfig, settings.getRedisHost(), settings.getRedisPort());
        redisData = new RedisData(jedisPool);
        rethinkDbData = new RethinkDbData(conn);

        frontier = new Frontier(settings, jedisPool, robotsServiceClient, dnsServiceClient, scopeServiceClient,
                outOfScopeHandlerClient);
        apiServer = new FrontierApiServer(settings.getApiPort(), settings.getTerminationGracePeriodSeconds(), frontier);

        apiServer.start();
    }

    @AfterEach
    public void shutdown() throws Exception {
        apiServer.shutdown();
        robotsServiceClient.close();
        dnsServiceClient.close();
        scopeServiceClient.close();
        outOfScopeHandlerClient.close();
        dnsResolverMock.close();
        robotsEvaluatorMock.close();
        outOfScopeHandlerMock.close();
        scopeCheckerServiceMock.close();
        harvesterMock.close();

        frontierChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);

        conn.exec(r.table(Tables.URI_QUEUE.name).delete());
        conn.exec(r.table(Tables.EXECUTIONS.name).delete());
        conn.exec(r.table(Tables.JOB_EXECUTIONS.name).delete());
        conn.exec(r.table(Tables.SEEDS.name).delete());
        conn.exec(r.table(Tables.CRAWL_ENTITIES.name).delete());
        conn.exec(r.table(Tables.CRAWL_LOG.name).delete());
        conn.exec(r.table(Tables.PAGE_LOG.name).delete());

        jedisPool.getResource().flushAll();
        jedisPool.close();
    }

    @Test
    public void testOneSuccessfullSeed() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);
        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);


        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(20, TimeUnit.SECONDS)
                .until(() -> {
                    JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                    if (j.getExecutionsStateCount() > 0) {
                        System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                    }
                    if (State.FINISHED == j.getState() && rethinkDbData.getQueuedUris().isEmpty()) {
                        return true;
                    }
                    return false;
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0)
                .crawlLogs().hasNumberOfElements(0);

        assertThat(redisData)
                .hasQueueTotalCount(0)
                .crawlHostGroups().hasNumberOfElements(0);
        assertThat(redisData)
                .crawlExecutionQueueCounts().hasNumberOfElements(0);
        assertThat(redisData)
                .sessionTokens().hasNumberOfElements(0);
        assertThat(redisData)
                .readyQueue().hasNumberOfElements(0);
    }

    @Test
    public void testHarvesterException() throws Exception {
        int seedCount = 4;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock
                .withExceptionForAllUrlRequests("http://stress-000000.com")
                .withExceptionForUrlRequests("http://stress-000001.com", 1, 1)
                .withExceptionForAllUrlRequests("http://stress-000002.com/p0/p2")
                .withLinksPerLevel(linksPerLevel);
        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);


        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(20, TimeUnit.SECONDS)
                .until(() -> {
                    JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                    if (j.getExecutionsStateCount() > 0) {
                        System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                    }
                    if (State.FINISHED == j.getState() && rethinkDbData.getQueuedUris().isEmpty()) {
                        return true;
                    }
                    return false;
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        rethinkDbData.getCrawlLogs().forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        assertThat(rethinkDbData)
                .hasQueueTotalCount(0)
                .crawlLogs().hasNumberOfElements(2);

        assertThat(redisData)
                .hasQueueTotalCount(0)
                .crawlHostGroups().hasNumberOfElements(0);
        assertThat(redisData)
                .crawlExecutionQueueCounts().hasNumberOfElements(0);
        assertThat(redisData)
                .sessionTokens().hasNumberOfElements(0);
        assertThat(redisData)
                .readyQueue().hasNumberOfElements(0);
    }

    @Test
    public void testHarvesterTimeout() throws Exception {
        int seedCount = 3;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock
                .withLongFetchTimeForAllUrlRequests("http://stress-000000.com")
                .withLongFetchTimeForUrlRequests("http://stress-000001.com/p0", 1, 1)
                .withLinksPerLevel(linksPerLevel);
        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);


        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(20, TimeUnit.SECONDS)
                .until(() -> {
                    JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                    if (j.getExecutionsStateCount() > 0) {
                        System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                    }
                    if (State.FINISHED == j.getState() && rethinkDbData.getQueuedUris().isEmpty()) {
                        return true;
                    }
                    return false;
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        rethinkDbData.getCrawlLogs().forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        assertThat(rethinkDbData)
                .hasQueueTotalCount(0)
                .crawlLogs().hasNumberOfElements(1);

        assertThat(redisData)
                .hasQueueTotalCount(0)
                .crawlHostGroups().hasNumberOfElements(0);
        assertThat(redisData)
                .crawlExecutionQueueCounts().hasNumberOfElements(0);
        assertThat(redisData)
                .sessionTokens().hasNumberOfElements(0);
        assertThat(redisData)
                .readyQueue().hasNumberOfElements(0);
    }

    @Test
    public void testDnsFailureOnce() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 0;
        int maxHopsFromSeed = 1;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);
        dnsResolverMock.withFetchErrorForHostRequests("stress-000000.com", 1, 1);

        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);


        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(20, TimeUnit.SECONDS)
                .until(() -> {
                    JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                    if (j.getExecutionsStateCount() > 0) {
                        System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                    }
                    if (State.FINISHED == j.getState() && rethinkDbData.getQueuedUris().isEmpty()) {
                        return true;
                    }
                    return false;
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        rethinkDbData.getCrawlLogs().forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        assertThat(rethinkDbData)
                .hasQueueTotalCount(0)
                .crawlLogs().hasNumberOfElements(0);

        assertThat(redisData)
                .hasQueueTotalCount(0)
                .crawlHostGroups().hasNumberOfElements(0);
        assertThat(redisData)
                .crawlExecutionQueueCounts().hasNumberOfElements(0);
        assertThat(redisData)
                .sessionTokens().hasNumberOfElements(0);
        assertThat(redisData)
                .readyQueue().hasNumberOfElements(0);
    }

    @Test
    public void testDnsFailureTwice() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 0;
        int maxHopsFromSeed = 1;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);
        dnsResolverMock.withFetchErrorForHostRequests("stress-000000.com", 1, 2);

        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);


        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(20, TimeUnit.SECONDS)
                .until(() -> {
                    JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                    if (j.getExecutionsStateCount() > 0) {
                        System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                    }
                    if (State.FINISHED == j.getState() && rethinkDbData.getQueuedUris().isEmpty()) {
                        return true;
                    }
                    return false;
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        rethinkDbData.getCrawlLogs().forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        assertThat(rethinkDbData)
                .hasQueueTotalCount(0)
                .crawlLogs().hasNumberOfElements(0);

        assertThat(redisData)
                .hasQueueTotalCount(0)
                .crawlHostGroups().hasNumberOfElements(0);
        assertThat(redisData)
                .crawlExecutionQueueCounts().hasNumberOfElements(0);
        assertThat(redisData)
                .sessionTokens().hasNumberOfElements(0);
        assertThat(redisData)
                .readyQueue().hasNumberOfElements(0);
    }

    @Test
    public void testDnsFailureThreeTimes() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 0;
        int maxHopsFromSeed = 1;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);
        dnsResolverMock.withFetchErrorForAllHostRequests("stress-000000.com");

        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);


        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(20, TimeUnit.SECONDS)
                .until(() -> {
                    JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                    if (j.getExecutionsStateCount() > 0) {
                        System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                    }
                    if (State.FINISHED == j.getState() && rethinkDbData.getQueuedUris().isEmpty()) {
                        return true;
                    }
                    return false;
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        rethinkDbData.getCrawlLogs().forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        assertThat(rethinkDbData)
                .hasQueueTotalCount(0)
                .crawlLogs().hasNumberOfElements(1);

        assertThat(redisData)
                .hasQueueTotalCount(0)
                .crawlHostGroups().hasNumberOfElements(0);
        assertThat(redisData)
                .crawlExecutionQueueCounts().hasNumberOfElements(0);
        assertThat(redisData)
                .sessionTokens().hasNumberOfElements(0);
        assertThat(redisData)
                .readyQueue().hasNumberOfElements(0);
    }
}