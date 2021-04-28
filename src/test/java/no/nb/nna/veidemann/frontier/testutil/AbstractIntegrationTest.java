package no.nb.nna.veidemann.frontier.testutil;

import io.opentracing.mock.MockTracer;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.frontier.api.FrontierApiServer;
import no.nb.nna.veidemann.frontier.settings.Settings;
import no.nb.nna.veidemann.frontier.worker.DnsServiceClient;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.LogServiceClient;
import no.nb.nna.veidemann.frontier.worker.OutOfScopeHandlerClient;
import no.nb.nna.veidemann.frontier.worker.RobotsServiceClient;
import no.nb.nna.veidemann.frontier.worker.ScopeServiceClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Duration;

import static com.rethinkdb.RethinkDB.r;

public class AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractIntegrationTest.class);
    private static Network network = Network.newNetwork();

    @Container
    public static GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:6-alpine"))
            .withNetwork(network)
            .withNetworkAliases("redis")
            .withExposedPorts(6379)
            .withLogConsumer(new SkipUntilFilter("Ready to accept connections", new Slf4jLogConsumer(LoggerFactory.getLogger("REDIS"))))
            .withStartupCheckStrategy(
                    new IsRunningStartupCheckStrategy().withTimeout(Duration.ofSeconds(60))
            );
    @Container
    public static GenericContainer rethinkDb = new GenericContainer(DockerImageName.parse("rethinkdb:2.4.1-buster-slim"))
            .withNetwork(network)
            .withNetworkAliases("db")
            .withExposedPorts(28015)
            .withStartupCheckStrategy(
                    new IsRunningStartupCheckStrategy().withTimeout(Duration.ofSeconds(60))
            );
    @Container
    public static GenericContainer dbInitializer = new GenericContainer(
            DockerImageName.parse("norsknettarkiv/veidemann-db-initializer").withTag(AbstractIntegrationTest.getStringProperty("veidemann.rethinkdbadapter.version", "v0.8.0")))
            .withNetwork(network)
            .dependsOn(rethinkDb)
            .withEnv("DB_HOST", "db")
            .withEnv("DB_PORT", "28015")
            .withEnv("DB_USER", "admin")
            .withStartupCheckStrategy(
                    new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(60))
            );
    public Settings settings;
    RethinkDbConnection conn;
    FrontierApiServer apiServer;
    Frontier frontier;
    public DnsResolverMock dnsResolverMock;
    public RobotsEvaluatorMock robotsEvaluatorMock;
    public OutOfScopeHandlerMock outOfScopeHandlerMock;
    public ScopeCheckerServiceMock scopeCheckerServiceMock;
    public HarvesterMock harvesterMock;
    public LogServiceMock logServiceMock;
    RobotsServiceClient robotsServiceClient;
    DnsServiceClient dnsServiceClient;
    ScopeServiceClient scopeServiceClient;
    OutOfScopeHandlerClient outOfScopeHandlerClient;
    LogServiceClient logServiceClient;
    public JedisPool jedisPool;
    public RedisData redisData;
    public RethinkDbData rethinkDbData;
    public MockTracer tracer;
    public CrawlRunner crawlRunner;


    private static String getStringProperty(String name, String def) {
        String prop = System.getProperty(name);
        return (prop == null || prop.isBlank()) ? def : prop;
    }

    private static int getIntProperty(String name, int def) {
        String prop = System.getProperty(name);
        return (prop == null || prop.isBlank()) ? def : Integer.parseInt(prop);
    }

    @BeforeEach
    public void setup() throws DbConnectionException, IOException {
        tracer = new MockTracer();

        settings = new Settings();
        settings.setDnsResolverHost(AbstractIntegrationTest.getStringProperty("dnsresolver.host", "localhost"));
        settings.setDnsResolverPort(AbstractIntegrationTest.getIntProperty("dnsresolver.port", 9500));
        settings.setRobotsEvaluatorHost(AbstractIntegrationTest.getStringProperty("robotsevaluator.host", "localhost"));
        settings.setRobotsEvaluatorPort(AbstractIntegrationTest.getIntProperty("robotsevaluator.port", 9501));
        settings.setOutOfScopeHandlerHost(AbstractIntegrationTest.getStringProperty("ooshandler.host", "localhost"));
        settings.setOutOfScopeHandlerPort(AbstractIntegrationTest.getIntProperty("ooshandler.port", 9502));
        settings.setScopeserviceHost(AbstractIntegrationTest.getStringProperty("scopeChecker.host", "localhost"));
        settings.setScopeservicePort(AbstractIntegrationTest.getIntProperty("scopeChecker.port", 9503));
        settings.setLogServiceHost(AbstractIntegrationTest.getStringProperty("logService.host", "localhost"));
        settings.setLogServicePort(AbstractIntegrationTest.getIntProperty("logService.port", 9505));
        settings.setDbHost(rethinkDb.getHost());
        settings.setDbPort(rethinkDb.getFirstMappedPort());
        settings.setDbName("veidemann");
        settings.setDbUser("admin");
        settings.setDbPassword("");
        settings.setBusyTimeout(Duration.ofSeconds(2));
        settings.setApiPort(AbstractIntegrationTest.getIntProperty("frontier.port", 9504));
        settings.setTerminationGracePeriodSeconds(10);
        settings.setRedisHost(redis.getHost());
        settings.setRedisPort(redis.getFirstMappedPort());

        dnsResolverMock = new DnsResolverMock(settings.getDnsResolverPort()).start();
        robotsEvaluatorMock = new RobotsEvaluatorMock(settings.getRobotsEvaluatorPort()).start();
        outOfScopeHandlerMock = new OutOfScopeHandlerMock(settings.getOutOfScopeHandlerPort()).start();
        scopeCheckerServiceMock = new ScopeCheckerServiceMock(settings.getScopeservicePort()).start();
        harvesterMock = new HarvesterMock(settings).start();
        logServiceMock = new LogServiceMock(settings.getLogServicePort()).start();
        robotsServiceClient = new RobotsServiceClient(settings.getRobotsEvaluatorHost(), settings.getRobotsEvaluatorPort());
        dnsServiceClient = new DnsServiceClient(settings.getDnsResolverHost(), settings.getDnsResolverPort());
        scopeServiceClient = new ScopeServiceClient(settings.getScopeserviceHost(), settings.getScopeservicePort());
        outOfScopeHandlerClient = new OutOfScopeHandlerClient(settings.getOutOfScopeHandlerHost(), settings.getOutOfScopeHandlerPort());
        logServiceClient = new LogServiceClient(settings.getLogServiceHost(), settings.getLogServicePort());

        CommonSettings dbSettings = new CommonSettings()
                .withDbHost(settings.getDbHost())
                .withDbPort(settings.getDbPort())
                .withDbName(settings.getDbName())
                .withDbUser(settings.getDbUser())
                .withDbPassword(settings.getDbPassword());
        if (!DbService.isConfigured()) {
            DbService.configure(dbSettings);
        }
        conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
        conn.connect(dbSettings);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(512);
        jedisPoolConfig.setMaxIdle(32);
        jedisPoolConfig.setMinIdle(2);

        jedisPool = new JedisPool(jedisPoolConfig, settings.getRedisHost(), settings.getRedisPort());
        redisData = new RedisData(jedisPool);
        rethinkDbData = new RethinkDbData(conn);

        frontier = new Frontier(tracer, settings, jedisPool, robotsServiceClient, dnsServiceClient, scopeServiceClient,
                outOfScopeHandlerClient, logServiceClient);
        apiServer = new FrontierApiServer(settings.getApiPort(), settings.getTerminationGracePeriodSeconds(), frontier);
        apiServer.start();

        crawlRunner = new CrawlRunner(settings, rethinkDbData, jedisPool);
    }

    @AfterEach
    public void shutdown() throws Exception {
        harvesterMock.close();
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
        crawlRunner.close();

        conn.exec(r.table(Tables.URI_QUEUE.name).delete());
        conn.exec(r.table(Tables.EXECUTIONS.name).delete());
        conn.exec(r.table(Tables.JOB_EXECUTIONS.name).delete());
        conn.exec(r.table(Tables.SEEDS.name).delete());
        conn.exec(r.table(Tables.CRAWL_ENTITIES.name).delete());

        jedisPool.getResource().flushAll();
        jedisPool.close();
    }
}
