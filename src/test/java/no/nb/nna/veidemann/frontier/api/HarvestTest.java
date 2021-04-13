package no.nb.nna.veidemann.frontier.api;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.nb.nna.veidemann.api.config.v1.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.commons.client.OutOfScopeHandlerClient;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import no.nb.nna.veidemann.frontier.settings.Settings;
import no.nb.nna.veidemann.frontier.testutil.*;
import no.nb.nna.veidemann.frontier.worker.*;
import org.assertj.core.api.Assertions;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
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
@Tag("scylla")
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
    LogServiceMock logServiceMock;
    RobotsServiceClient robotsServiceClient;
    DnsServiceClient dnsServiceClient;
    ScopeServiceClient scopeServiceClient;
    OutOfScopeHandlerClient outOfScopeHandlerClient;
    LogServiceClient logServiceClient;

    JedisPool jedisPool;
    RedisData redisData;
    RethinkDbData rethinkDbData;

    ExecutorService asyncFunctionsExecutor = new ThreadPoolExecutor(2, 128, 15, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("asyncFunc-%d").build(), new CallerRunsPolicy());

    private static Network network = Network.newNetwork();

    @Container
    public static GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:6-alpine"))
            .withNetwork(network)
            .withExposedPorts(6379)
            .withLogConsumer(new SkipUntilFilter("Ready to accept connections", new Slf4jLogConsumer(LoggerFactory.getLogger("REDIS"))));

    @Container
    public static GenericContainer rethinkDb = new GenericContainer(DockerImageName.parse("rethinkdb:2.4.1-buster-slim"))
            .withNetwork(network)
            .withNetworkAliases("db")
            .withExposedPorts(28015);

    @Container
    public static GenericContainer dbInitializer = new GenericContainer(DockerImageName.parse("norsknettarkiv/veidemann-db-initializer:0.6.1"))
            .withNetwork(network)
            .dependsOn(rethinkDb)
            .withEnv("DB_HOST", "db")
            .withEnv("DB_PORT", "28015")
            .withEnv("DB_USER", "admin")
            .withStartupCheckStrategy(
                    new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(60))
            );

    private String getStringProperty(String name, String def) {
        String prop = System.getProperty(name);
        return prop.isBlank() ? def : prop;
    }

    private int getIntProperty(String name, int def) {
        String prop = System.getProperty(name);
        return prop.isBlank() ? def : Integer.parseInt(prop);
    }

    @BeforeEach
    public void setup() throws DbConnectionException, IOException {
        Settings settings = new Settings();
        settings.setDnsResolverHost(getStringProperty("dnsresolver.host", "localhost"));
        settings.setDnsResolverPort(getIntProperty("dnsresolver.port", 9500));
        settings.setRobotsEvaluatorHost(getStringProperty("robotsevaluator.host", "localhost"));
        settings.setRobotsEvaluatorPort(getIntProperty("robotsevaluator.port", 9501));
        settings.setOutOfScopeHandlerHost(getStringProperty("ooshandler.host", "localhost"));
        settings.setOutOfScopeHandlerPort(getIntProperty("ooshandler.port", 9502));
        settings.setScopeserviceHost(getStringProperty("scopeChecker.host", "localhost"));
        settings.setScopeservicePort(getIntProperty("scopeChecker.port", 9503));
        settings.setLogServiceHost(getStringProperty("logService.host", "localhost"));
        settings.setLogServicePort(getIntProperty("logService.port", 9505));
        settings.setDbHost(rethinkDb.getHost());
        settings.setDbPort(rethinkDb.getFirstMappedPort());
        settings.setDbName("veidemann");
        settings.setDbUser("admin");
        settings.setDbPassword("");
        settings.setBusyTimeout(Duration.ofSeconds(2));
        settings.setApiPort(getIntProperty("frontier.port", 9504));
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
        logServiceMock = new LogServiceMock(settings.getLogServicePort()).start();
        robotsServiceClient = new RobotsServiceClient(settings.getRobotsEvaluatorHost(), settings.getRobotsEvaluatorPort(), asyncFunctionsExecutor);
        dnsServiceClient = new DnsServiceClient(settings.getDnsResolverHost(), settings.getDnsResolverPort(), asyncFunctionsExecutor);
        scopeServiceClient = new ScopeServiceClient(settings.getScopeserviceHost(), settings.getScopeservicePort());
        outOfScopeHandlerClient = new OutOfScopeHandlerClient(settings.getOutOfScopeHandlerHost(), settings.getOutOfScopeHandlerPort());
        logServiceClient = new LogServiceClient(settings.getLogServiceHost(), settings.getLogServicePort());

        if (!DbService.isConfigured()) {
            CommonSettings dbSettings = new CommonSettings()
                    .withDbHost(rethinkDb.getHost())
                    .withDbPort(rethinkDb.getMappedPort(28015))
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
                outOfScopeHandlerClient, logServiceClient);
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
        logServiceClient.close();
        dnsResolverMock.close();
        logServiceMock.close();
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
                .hasQueueTotalCount(0);

        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(0);

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
        // logServiceMock.withExpectedNrOfWrites(2);
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

        logServiceMock.crawlLogs.forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(2);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

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

        logServiceMock.crawlLogs.forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));

        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(1);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

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

        logServiceMock.crawlLogs.forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

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

        logServiceMock.crawlLogs.forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

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

        logServiceMock.crawlLogs.forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(1);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

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
    public void testDnsExceptionThreeTimes() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 0;
        int maxHopsFromSeed = 1;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);
        dnsResolverMock.withExceptionForAllHostRequests("stress-000000.com");

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

        logServiceMock.crawlLogs.forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(1);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

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
    public void testDeniedByRobotsTxt() throws Exception {
        int seedCount = 2;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);
        robotsEvaluatorMock
                .withFetchDenialForUrl("http://stress-000000.com")
                .withFetchDenialForUrl("http://stress-000001.com/p0")
                .withFetchDenialForUrl("http://stress-000001.com/p1")
                .withExceptionForUrl("http://stress-000001.com/p2");

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

        logServiceMock.crawlLogs.forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(3);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1).hasEntrySatisfying(jes.getId(), j -> {
            assertThat(j)
                    .hasState(JobExecutionStatus.State.FINISHED)
                    .hasStartTime(true)
                    .hasEndTime(true)
                    .documentsCrawledEquals(5)
                    .documentsDeniedEquals(3)
                    .documentsFailedEquals(1)
                    .documentsRetriedEquals(0)
                    .documentsOutOfScopeEquals(9);
        });
        String crawlExecutionId1 = c.crawlExecutions.get(c.seeds.get(0).getId()).get().getId();
        String crawlExecutionId2 = c.crawlExecutions.get(c.seeds.get(1).getId()).get().getId();
        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount)
                .hasEntrySatisfying(crawlExecutionId1, s -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FAILED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(0)
                            .documentsDeniedEquals(1)
                            .documentsFailedEquals(1)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(0)
                            .currentUriIdCountIsEqualTo(0);
                })
                .hasEntrySatisfying(crawlExecutionId2, s -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(5)
                            .documentsDeniedEquals(2)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(9)
                            .currentUriIdCountIsEqualTo(0);
                });

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
    public void testRecheckScope() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 2;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock
                .withMaxHopsFromSeed(maxHopsFromSeed)
                .withDenialForUrlRequests("http://stress-000000.com/p0", 2, 2);
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

        logServiceMock.crawlLogs.forEach(cl -> System.out.println(String.format("Status: %3d %s %s", cl.getStatusCode(), cl.getRequestedUri(), cl.getError())));
        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        // TODO .crawlLogs().hasNumberOfElements(0);
        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(0);

        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1).hasEntrySatisfying(jes.getId(), j -> {
            assertThat(j)
                    .hasState(JobExecutionStatus.State.FINISHED)
                    .hasStartTime(true)
                    .hasEndTime(true)
                    .documentsCrawledEquals(4)
                    .documentsDeniedEquals(0)
                    .documentsFailedEquals(0)
                    .documentsRetriedEquals(0)
                    .documentsOutOfScopeEquals(5)
                    .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_MANUAL, 0)
                    .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_TIMEOUT, 0)
                    .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_SIZE, 0)
                    .executionsStateCountEquals(CrawlExecutionStatus.State.FINISHED, 1)
                    .executionsStateCountEquals(CrawlExecutionStatus.State.FAILED, 0)
                    .executionsStateCountEquals(CrawlExecutionStatus.State.CREATED, 0)
                    .executionsStateCountEquals(CrawlExecutionStatus.State.FETCHING, 0)
                    .executionsStateCountEquals(CrawlExecutionStatus.State.SLEEPING, 0);
        });
        String crawlExecutionId1 = c.crawlExecutions.get(c.seeds.get(0).getId()).get().getId();
        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount)
                .hasEntrySatisfying(crawlExecutionId1, s -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(4)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(5)
                            .currentUriIdCountIsEqualTo(0);
                });

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
    public void testAbortCrawlExecutionEarly() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);

        // Abort the first execution as soon as it is created
        String crawlExecutionId = c.crawlExecutions.get(c.seeds.get(0).getId()).get().getId();
        DbService.getInstance().getExecutionsAdapter().setCrawlExecutionStateAborted(crawlExecutionId, CrawlExecutionStatus.State.ABORTED_MANUAL);

        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                    if (j.getExecutionsStateCount() > 0) {
                        System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                    }
                    if (State.RUNNING != j.getState() && rethinkDbData.getQueuedUris().isEmpty() && jedisPool.getResource().keys("*").size() <= 1) {
                        return true;
                    }
                    return false;
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        // TODO .crawlLogs().hasNumberOfElements(0);
        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount)
                .hasEntrySatisfying(crawlExecutionId, s -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.ABORTED_MANUAL)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(0)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(0)
                            .currentUriIdCountIsEqualTo(0);
                });
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1)
                .hasEntrySatisfying(jes.getId(), s -> {
                    assertThat(s)
                            .hasState(JobExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(0)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(0);
                });

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
    public void testAbortCrawlExecutionLate() throws Exception {
        int seedCount = 10;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);

        String crawlExecutionId1 = c.crawlExecutions.get(c.seeds.get(0).getId()).get().getId();
        String crawlExecutionId2 = c.crawlExecutions.get(c.seeds.get(1).getId()).get().getId();
        String crawlExecutionId3 = c.crawlExecutions.get(c.seeds.get(2).getId()).get().getId();
        String crawlExecutionId4 = c.crawlExecutions.get(c.seeds.get(3).getId()).get().getId();

        // Abort the first execution as soon as it is fetching
        await().pollDelay(100, TimeUnit.MILLISECONDS).pollInterval(100, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    CrawlExecutionStatus ces = DbService.getInstance().getExecutionsAdapter().getCrawlExecutionStatus(crawlExecutionId1);
                    if (ces.getState() == CrawlExecutionStatus.State.FETCHING) {
                        DbService.getInstance().getExecutionsAdapter().setCrawlExecutionStateAborted(crawlExecutionId1, CrawlExecutionStatus.State.ABORTED_MANUAL);
                        return true;
                    }
                    return false;
                });


        // Abort the second execution as soon as it is sleeping
        await().pollDelay(100, TimeUnit.MILLISECONDS).pollInterval(10, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    CrawlExecutionStatus ces = DbService.getInstance().getExecutionsAdapter().getCrawlExecutionStatus(crawlExecutionId2);
                    if (ces.getState() == CrawlExecutionStatus.State.SLEEPING) {
                        DbService.getInstance().getExecutionsAdapter().setCrawlExecutionStateAborted(crawlExecutionId2, CrawlExecutionStatus.State.ABORTED_MANUAL);
                        return true;
                    }
                    return false;
                });


        // Abort the third execution as soon as it is finished
        await().pollDelay(100, TimeUnit.MILLISECONDS).pollInterval(10, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    CrawlExecutionStatus ces = DbService.getInstance().getExecutionsAdapter().getCrawlExecutionStatus(crawlExecutionId3);
                    if (ces.getState() == CrawlExecutionStatus.State.FINISHED) {
                        DbService.getInstance().getExecutionsAdapter().setCrawlExecutionStateAborted(crawlExecutionId3, CrawlExecutionStatus.State.ABORTED_MANUAL);
                        return true;
                    }
                    return false;
                });

        // Wait for crawl to finish
        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                    if (j.getExecutionsStateCount() > 0) {
                        System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                    }
                    if (State.RUNNING != j.getState() && rethinkDbData.getQueuedUris().isEmpty() && jedisPool.getResource().keys("*").size() <= 1) {
                        return true;
                    }
                    return false;
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount)
                // Check first seed
                .hasEntrySatisfying(crawlExecutionId1, s -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.ABORTED_MANUAL)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(1)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeSatisfies(d -> d.isBetween(0L, 2L))
                            .currentUriIdCountIsEqualTo(0);
                })
                // Check second seed
                .hasEntrySatisfying(crawlExecutionId2, s -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.ABORTED_MANUAL)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(1)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeSatisfies(d -> d.isBetween(0L, 2L))
                            .currentUriIdCountIsEqualTo(0);
                })
                // Check third seed
                .hasEntrySatisfying(crawlExecutionId3, s -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(13)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(27)
                            .currentUriIdCountIsEqualTo(0);
                })
                // Check fourth seed
                .hasEntrySatisfying(crawlExecutionId4, s -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(13)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(27)
                            .currentUriIdCountIsEqualTo(0);
                });
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1)
                .hasEntrySatisfying(jes.getId(), j -> {
                    assertThat(j)
                            .hasState(JobExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(106)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(216)
                            .executionsStateCountSatifies(CrawlExecutionStatus.State.ABORTED_MANUAL, d -> d.isGreaterThan(0))
                            .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_TIMEOUT, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_SIZE, 0)
                            .executionsStateCountSatifies(CrawlExecutionStatus.State.FINISHED, d -> d.isGreaterThan(0))
                            .executionsStateCountEquals(CrawlExecutionStatus.State.FAILED, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.CREATED, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.FETCHING, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.SLEEPING, 0);
                });

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
    public void testAbortJobExecution() throws Exception {
        int seedCount = 20;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);
        dnsResolverMock.withSimulatedLookupTimeMs(300);

        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount);

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);

        // Abort the first execution as soon as one seed is completed
        await().pollDelay(100, TimeUnit.MILLISECONDS).pollInterval(100, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    try (Jedis jedis = jedisPool.getResource()) {
                        Map<String, String> f = jedis.hgetAll(CrawlQueueManager.JOB_EXECUTION_PREFIX + jes.getId());
                        if (!f.getOrDefault("FINISHED", "0").equals("0")) {
                            return true;
                        }
                        return false;
                    }
                });
        DbService.getInstance().getExecutionsAdapter().setJobExecutionStateAborted(jes.getId());

        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    try (Jedis jedis = jedisPool.getResource()) {
                        JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                        if (j.getExecutionsStateCount() > 0) {
                            System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                            System.out.println(jedisPool.getResource().keys("*") + " :: QCT=" + jedisPool.getResource().get("QCT"));
                        }
                        if (State.RUNNING != j.getState() && rethinkDbData.getQueuedUris().isEmpty() && jedis.keys("*").size() <= 1) {
                            return true;
                        }
                        return false;
                    }
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1)
                .hasEntrySatisfying(jes.getId(), j -> {
                    assertThat(j)
                            .hasState(JobExecutionStatus.State.ABORTED_MANUAL)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledSatisfies(d -> d.isGreaterThan(0))
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeSatisfies(d -> d.isGreaterThan(0))
                            .executionsStateCountSatifies(CrawlExecutionStatus.State.ABORTED_MANUAL, d -> d.isGreaterThan(0))
                            .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_TIMEOUT, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_SIZE, 0)
                            .executionsStateCountSatifies(CrawlExecutionStatus.State.FINISHED, d -> d.isGreaterThan(0))
                            .executionsStateCountEquals(CrawlExecutionStatus.State.FAILED, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.CREATED, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.FETCHING, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.SLEEPING, 0);
                });

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
    public void testAbortTimeout() throws Exception {
        int seedCount = 20;
        int linksPerLevel = 3;
        int outOfScopeLinksPerLevel = 2;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);
        harvesterMock.withOutOfScopeLinksPerLevel(outOfScopeLinksPerLevel);
        dnsResolverMock.withSimulatedLookupTimeMs(300);

        SetupCrawl c = new SetupCrawl();
        c.setup(seedCount, CrawlLimitsConfig.newBuilder().setMaxDurationS(5).build());

        Instant testStart = Instant.now();

        JobExecutionStatus jes = c.runCrawl(frontierStub);

        await().pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    try (Jedis jedis = jedisPool.getResource()) {
                        JobExecutionStatus j = DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jes.getId());
                        if (j.getExecutionsStateCount() > 0) {
                            System.out.println("STATE " + j.getExecutionsStateMap() + " :: " + j.getState());
                        }
                        if (State.RUNNING != j.getState() && rethinkDbData.getQueuedUris().isEmpty() && jedis.keys("*").size() <= 1) {
                            return true;
                        }
                        return false;
                    }
                });

        Duration testTime = Duration.between(testStart, Instant.now());
        System.out.println(String.format("Test time: %02d:%02d:%02d.%d",
                testTime.toHoursPart(), testTime.toMinutesPart(), testTime.toSecondsPart(), testTime.toMillisPart()));

        Assertions.assertThat(logServiceMock.crawlLogs.size()).isEqualTo(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(20);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1)
                .hasEntrySatisfying(jes.getId(), j -> {
                    assertThat(j)
                            .hasState(JobExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledSatisfies(d -> d.isGreaterThan(0))
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeSatisfies(d -> d.isGreaterThan(0))
                            .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_MANUAL, 0)
                            .executionsStateCountSatifies(CrawlExecutionStatus.State.ABORTED_TIMEOUT, d -> d.isGreaterThan(0))
                            .executionsStateCountEquals(CrawlExecutionStatus.State.ABORTED_SIZE, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.FINISHED, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.FAILED, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.CREATED, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.FETCHING, 0)
                            .executionsStateCountEquals(CrawlExecutionStatus.State.SLEEPING, 0);
                });

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
