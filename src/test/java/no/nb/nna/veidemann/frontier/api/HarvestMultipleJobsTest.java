package no.nb.nna.veidemann.frontier.api;

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.frontier.testutil.CrawlRunner.RunningCrawl;
import no.nb.nna.veidemann.frontier.testutil.CrawlRunner.SeedAndExecutions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static no.nb.nna.veidemann.frontier.testutil.FrontierAssertions.assertThat;

@Testcontainers
@Tag("integration")
@Tag("redis")
@Tag("rethinkDb")
public class HarvestMultipleJobsTest extends no.nb.nna.veidemann.frontier.testutil.AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(HarvestMultipleJobsTest.class);

    int seedCount = 2;
    int linksPerLevel = 1;
    int maxHopsFromSeed = 1;
    int numberOfJobs = 40;

    @Test
    public void testSameSeedsInParallellJobs() throws Exception {
        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        ConfigObject[] jobs = new ConfigObject[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            jobs[i] = crawlRunner.genJob("job" + (i + 1));
        }
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", jobs);
        RunningCrawl[] crawls = new RunningCrawl[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            crawls[i] = crawlRunner.runCrawl(jobs[i], seeds);
        }
        crawlRunner.awaitCrawlFinished(5, TimeUnit.MINUTES, crawls);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(numberOfJobs)
                .allSatisfy((id, j) -> {
                    assertThat(j)
                            .hasState(JobExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2 * seedCount)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(seedCount);
                });

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount * numberOfJobs)
                .allSatisfy((id, s) -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(1)
                            .currentUriIdCountIsEqualTo(0);
                });

        assertThat(rethinkDbData).jobStatsMatchesCrawlExecutions();

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
    public void testUniqueSeedsWithSameIpInParallellJobs() throws Exception {
        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        ConfigObject[] jobs = new ConfigObject[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            jobs[i] = crawlRunner.genJob("job" + (i + 1));
        }
        List<SeedAndExecutions>[] seeds = new List[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            String hostPrefix = String.format("a%03d.seed", i);
            seeds[i] = crawlRunner.genSeeds(seedCount, hostPrefix, jobs[i]);
        }
        RunningCrawl[] crawls = new RunningCrawl[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            crawls[i] = crawlRunner.runCrawl(jobs[i], seeds[i]);
        }
        crawlRunner.awaitCrawlFinished(5, TimeUnit.MINUTES, crawls);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(numberOfJobs)
                .allSatisfy((id, j) -> {
                    assertThat(j)
                            .hasState(JobExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2 * seedCount)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(seedCount);
                });
        String crawlExecutionId1 = seeds[1].get(0).getCrawlExecution(jobs[1]).get().getId();

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount * numberOfJobs)
                .allSatisfy((id, s) -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(1)
                            .currentUriIdCountIsEqualTo(0);
                });

        assertThat(rethinkDbData).jobStatsMatchesCrawlExecutions();

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
    public void testUniqueSeedsInParallellJobs() throws Exception {
        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        ConfigObject[] jobs = new ConfigObject[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            jobs[i] = crawlRunner.genJob("job" + (i + 1));
        }
        List<SeedAndExecutions>[] seeds = new List[numberOfJobs];
        int offset = 0;
        for (int i = 0; i < numberOfJobs; i++) {
            String hostPrefix = String.format("a%03d.seed", i);
            seeds[i] = crawlRunner.genSeeds(offset, seedCount, hostPrefix, jobs[i]);
            offset += seedCount;
        }
        RunningCrawl[] crawls = new RunningCrawl[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            crawls[i] = crawlRunner.runCrawl(jobs[i], seeds[i]);
        }
        crawlRunner.awaitCrawlFinished(1, TimeUnit.MINUTES, crawls);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(numberOfJobs)
                .allSatisfy((id, j) -> {
                    assertThat(j)
                            .hasState(JobExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2 * seedCount)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(seedCount);
                });

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount * numberOfJobs)
                .allSatisfy((id, s) -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(1)
                            .currentUriIdCountIsEqualTo(0);
                });

        assertThat(rethinkDbData).jobStatsMatchesCrawlExecutions();

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
    public void testUniqueSeedsWithSameIpInOneJob() throws Exception {
        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        ConfigObject job = crawlRunner.genJob("job");
        List<SeedAndExecutions>[] seeds = new List[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            String hostPrefix = String.format("a%03d.seed", i);
            seeds[i] = crawlRunner.genSeeds(seedCount, hostPrefix, job);
        }
        RunningCrawl[] crawls = new RunningCrawl[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            crawls[i] = crawlRunner.runCrawl(job, seeds[i]);
        }
        crawlRunner.awaitCrawlFinished(1, TimeUnit.MINUTES, crawls);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(numberOfJobs)
                .allSatisfy((id, j) -> {
                    assertThat(j)
                            .hasState(JobExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2 * seedCount)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(seedCount);
                });
        String crawlExecutionId1 = seeds[1].get(0).getCrawlExecution(job).get().getId();

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount * numberOfJobs)
                .allSatisfy((id, s) -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(1)
                            .currentUriIdCountIsEqualTo(0);
                });

        assertThat(rethinkDbData).jobStatsMatchesCrawlExecutions();

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
    public void testSameSeedsInOneJob() throws Exception {
        scopeCheckerServiceMock.withMaxHopsFromSeed(maxHopsFromSeed);
        harvesterMock.withLinksPerLevel(linksPerLevel);

        ConfigObject job = crawlRunner.genJob("job");
        List<SeedAndExecutions> seeds = crawlRunner.genSeeds(seedCount, "a.seed", job);
        RunningCrawl[] crawls = new RunningCrawl[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            crawls[i] = crawlRunner.runCrawl(job, seeds);
        }
        crawlRunner.awaitCrawlFinished(1, TimeUnit.MINUTES, crawls);

        assertThat(rethinkDbData)
                .hasQueueTotalCount(0);
        assertThat(rethinkDbData)
                .jobExecutionStatuses().hasSize(numberOfJobs)
                .allSatisfy((id, j) -> {
                    assertThat(j)
                            .hasState(JobExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2 * seedCount)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(seedCount);
                });
        String crawlExecutionId1 = seeds.get(0).getCrawlExecution(job).get().getId();

        assertThat(rethinkDbData)
                .crawlExecutionStatuses().hasSize(seedCount * numberOfJobs)
                .allSatisfy((id, s) -> {
                    assertThat(s)
                            .hasState(CrawlExecutionStatus.State.FINISHED)
                            .hasStartTime(true)
                            .hasEndTime(true)
                            .documentsCrawledEquals(2)
                            .documentsDeniedEquals(0)
                            .documentsFailedEquals(0)
                            .documentsRetriedEquals(0)
                            .documentsOutOfScopeEquals(1)
                            .currentUriIdCountIsEqualTo(0);
                });

        assertThat(rethinkDbData).jobStatsMatchesCrawlExecutions();

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
}
