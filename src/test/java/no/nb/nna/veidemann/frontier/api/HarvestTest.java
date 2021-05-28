package no.nb.nna.veidemann.frontier.api;

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import no.nb.nna.veidemann.frontier.testutil.CrawlRunner.RunningCrawl;
import no.nb.nna.veidemann.frontier.testutil.CrawlRunner.SeedAndExecutions;
import no.nb.nna.veidemann.frontier.testutil.HarvesterMock;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static no.nb.nna.veidemann.commons.ExtraStatusCodes.*;
import static no.nb.nna.veidemann.frontier.testutil.FrontierAssertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@Tag("integration")
@Tag("redis")
@Tag("rethinkDb")
public class HarvestTest extends no.nb.nna.veidemann.frontier.testutil.AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(HarvestTest.class);

    @Test
    public void testOneSuccessfullSeed() throws Exception {
        int seedCount = 1;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1).hasEntrySatisfying(crawl.getStatus().getId(), j -> {
            assertThat(j)
                    .hasState(JobExecutionStatus.State.FINISHED)
                    .hasStartTime(true)
                    .hasEndTime(true)
                    .documentsCrawledEquals(13)
                    .documentsDeniedEquals(0)
                    .documentsFailedEquals(0)
                    .documentsRetriedEquals(0)
                    .documentsOutOfScopeEquals(27);
        });
        String crawlExecutionId1 = seeds.get(0).getCrawlExecution(job).get().getId();

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount)
                .hasEntrySatisfying(crawlExecutionId1, s -> {
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

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

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
                .withExceptionForAllUrlRequests("http://a.seed-000000.com")
                .withExceptionForUrlRequests("http://a.seed-000001.com", 1, 1)
                .withExceptionForAllUrlRequests("http://a.seed-000002.com/p0/p2")
                .withLinksPerLevel(linksPerLevel);
        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(2);
        assertThat(logServiceMock.crawlLogs.get(0))
                .hasWarcId()
                .statusCodeEquals(RETRY_LIMIT_REACHED)
                .requestedUriEquals("http://a.seed-000000.com")
                .error().isNotNull().codeEquals(RUNTIME_EXCEPTION);
        assertThat(logServiceMock.crawlLogs.get(1))
                .hasWarcId()
                .statusCodeEquals(RETRY_LIMIT_REACHED.getCode())
                .requestedUriEquals("http://a.seed-000002.com/p0/p2")
                .error().isNotNull().codeEquals(RUNTIME_EXCEPTION);

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
                .withLongFetchTimeForAllUrlRequests("http://a.seed-000000.com")
                .withLongFetchTimeForUrlRequests("http://a.seed-000001.com/p0", 1, 1)
                .withLinksPerLevel(linksPerLevel);

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(1);
        assertThat(logServiceMock.crawlLogs.get(0))
                .hasWarcId()
                .statusCodeEquals(RETRY_LIMIT_REACHED)
                .requestedUriEquals("http://a.seed-000000.com")
                .error().isNotNull().codeEquals(RUNTIME_EXCEPTION);

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
    public void testHarvesterClosed() throws Exception {
        int seedCount = 3;
        int linksPerLevel = 3;
        int maxHopsFromSeed = 2;

        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);

        harvesterMock.close();
//        Thread.sleep(1000);
        harvesterMock = new HarvesterMock(settings).start();

        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

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
        dnsResolverMock.withFetchErrorForHostRequests("a.seed-000000.com", 1, 1);

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

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
        dnsResolverMock.withFetchErrorForHostRequests("a.seed-000000.com", 1, 2);

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

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
        dnsResolverMock.withFetchErrorForAllHostRequests("a.seed-000000.com");

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(1);
        assertThat(logServiceMock.crawlLogs.get(0))
                .hasWarcId()
                .statusCodeEquals(RETRY_LIMIT_REACHED)
                .requestedUriEquals("http://a.seed-000000.com")
                .error().isNotNull().codeEquals(FAILED_DNS);

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
        dnsResolverMock.withExceptionForAllHostRequests("a.seed-000000.com");

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(1);
        assertThat(logServiceMock.crawlLogs.get(0))
                .hasWarcId()
                .statusCodeEquals(RETRY_LIMIT_REACHED)
                .requestedUriEquals("http://a.seed-000000.com")
                .error().isNotNull().codeEquals(FAILED_DNS);

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
                .withFetchDenialForUrl("http://a.seed-000000.com")
                .withFetchDenialForUrl("http://a.seed-000001.com/p0")
                .withFetchDenialForUrl("http://a.seed-000001.com/p1")
                .withExceptionForUrl("http://a.seed-000001.com/p2");

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(3)
                .hasRequestSatisfying(r -> {
                    assertThat(r)
                            .hasWarcId()
                            .statusCodeEquals(PRECLUDED_BY_ROBOTS)
                            .requestedUriEquals("http://a.seed-000000.com")
                            .error().isNotNull().codeEquals(PRECLUDED_BY_ROBOTS);
                })
                .hasRequestSatisfying(r -> {
                    assertThat(r)
                            .hasWarcId()
                            .statusCodeEquals(PRECLUDED_BY_ROBOTS)
                            .requestedUriEquals("http://a.seed-000001.com/p0")
                            .error().isNotNull().codeEquals(PRECLUDED_BY_ROBOTS);
                })
                .hasRequestSatisfying(r -> {
                    assertThat(r)
                            .hasWarcId()
                            .statusCodeEquals(PRECLUDED_BY_ROBOTS)
                            .requestedUriEquals("http://a.seed-000001.com/p1")
                            .error().isNotNull().codeEquals(PRECLUDED_BY_ROBOTS);
                });

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1).hasEntrySatisfying(crawl.getStatus().getId(), j -> {
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
        String crawlExecutionId1 = seeds.get(0).getCrawlExecution(job).get().getId();
        String crawlExecutionId2 = seeds.get(1).getCrawlExecution(job).get().getId();
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
                .withDenialForUrlRequests("http://a.seed-000000.com/p0", 2, 2);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1).hasEntrySatisfying(crawl.getStatus().getId(), j -> {
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
        String crawlExecutionId1 = seeds.get(0).getCrawlExecution(job).get().getId();
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

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);

        // Abort the first execution as soon as it is created
        String crawlExecutionId = seeds.get(0).getCrawlExecution(job).get().getId();
        DbService.getInstance().getExecutionsAdapter().setCrawlExecutionStateAborted(crawlExecutionId, CrawlExecutionStatus.State.ABORTED_MANUAL);

        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
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
                .hasEntrySatisfying(crawl.getStatus().getId(), s -> {
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

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);

        String crawlExecutionId1 = seeds.get(0).getCrawlExecution(job).get().getId();
        String crawlExecutionId2 = seeds.get(1).getCrawlExecution(job).get().getId();
        String crawlExecutionId3 = seeds.get(2).getCrawlExecution(job).get().getId();
        String crawlExecutionId4 = seeds.get(3).getCrawlExecution(job).get().getId();

        // Abort the first execution as soon as it is fetching
        await().pollDelay(10, TimeUnit.MILLISECONDS).pollInterval(10, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    CrawlExecutionStatus ces = DbService.getInstance().getExecutionsAdapter().getCrawlExecutionStatus(crawlExecutionId1);
                    if (ces.getState() == CrawlExecutionStatus.State.FETCHING) {
                        DbService.getInstance().getExecutionsAdapter().setCrawlExecutionStateAborted(crawlExecutionId1, CrawlExecutionStatus.State.ABORTED_MANUAL);
                        return true;
                    }
                    return false;
                });


        // Abort the second execution as soon as it is sleeping
        await().pollDelay(10, TimeUnit.MILLISECONDS).pollInterval(10, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    CrawlExecutionStatus ces = DbService.getInstance().getExecutionsAdapter().getCrawlExecutionStatus(crawlExecutionId2);
                    if (ces.getState() == CrawlExecutionStatus.State.SLEEPING) {
                        DbService.getInstance().getExecutionsAdapter().setCrawlExecutionStateAborted(crawlExecutionId2, CrawlExecutionStatus.State.ABORTED_MANUAL);
                        return true;
                    }
                    return false;
                });


        // Abort the third execution as soon as it is finished
        await().pollDelay(10, TimeUnit.MILLISECONDS).pollInterval(10, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    CrawlExecutionStatus ces = DbService.getInstance().getExecutionsAdapter().getCrawlExecutionStatus(crawlExecutionId3);
                    if (ces.getState() == CrawlExecutionStatus.State.FINISHED) {
                        DbService.getInstance().getExecutionsAdapter().setCrawlExecutionStateAborted(crawlExecutionId3, CrawlExecutionStatus.State.ABORTED_MANUAL);
                        return true;
                    }
                    return false;
                });

        // Wait for crawl to finish
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

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
                            .documentsCrawledSatisfies(d -> d.isBetween(1L, 2L))
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
                .hasEntrySatisfying(crawl.getStatus().getId(), j -> {
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

        ConfigObject job = crawlRunner.genJob("job1");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);

        // Abort the first execution as soon as one seed is completed
        await().pollDelay(100, TimeUnit.MILLISECONDS).pollInterval(100, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    try (Jedis jedis = jedisPool.getResource()) {
                        Map<String, String> f = jedis.hgetAll(CrawlQueueManager.JOB_EXECUTION_PREFIX + crawl.getStatus().getId());
                        if (!f.getOrDefault("FINISHED", "0").equals("0")) {
                            return true;
                        }
                        return false;
                    }
                });
        DbService.getInstance().getExecutionsAdapter().setJobExecutionStateAborted(crawl.getStatus().getId());

        // Wait for crawl to finish
        crawlRunner.awaitCrawlFinished(crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1)
                .hasEntrySatisfying(crawl.getStatus().getId(), j -> {
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
        harvesterMock.withLinksPerLevel(linksPerLevel)
                .withOutOfScopeLinksPerLevel(outOfScopeLinksPerLevel)
                .withFetchTime(200);
        dnsResolverMock.withSimulatedLookupTimeMs(300);

        ConfigObject job = crawlRunner.genJob("job1", CrawlLimitsConfig.newBuilder().setMaxDurationS(5).build());
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl crawl = crawlRunner.runCrawl(job, seeds);
        crawlRunner.awaitCrawlFinished(2, TimeUnit.MINUTES, crawl);

        assertThat(logServiceMock.crawlLogs).hasNumberOfRequests(0);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(20);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(1)
                .hasEntrySatisfying(crawl.getStatus().getId(), j -> {
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
