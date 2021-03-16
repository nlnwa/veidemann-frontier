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

package no.nb.nna.veidemann.frontier.worker;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.frontier.db.script.ChgAddScript;
import no.nb.nna.veidemann.frontier.db.script.ChgDelayedQueueScript;
import no.nb.nna.veidemann.frontier.db.script.ChgGetScript;
import no.nb.nna.veidemann.frontier.db.script.ChgNextScript;
import no.nb.nna.veidemann.frontier.db.script.ChgQueueCountScript;
import no.nb.nna.veidemann.frontier.db.script.ChgReleaseScript;
import no.nb.nna.veidemann.frontier.db.script.ChgUpdateBusyTimeoutScript;
import no.nb.nna.veidemann.frontier.db.script.ChgUpdateScript;
import no.nb.nna.veidemann.frontier.db.script.JobExecutionGetScript;
import no.nb.nna.veidemann.frontier.db.script.JobExecutionUpdateScript;
import no.nb.nna.veidemann.frontier.db.script.NextUriScript;
import no.nb.nna.veidemann.frontier.db.script.RedisJob.JedisContext;
import no.nb.nna.veidemann.frontier.db.script.UriAddScript;
import no.nb.nna.veidemann.frontier.db.script.UriRemoveScript;
import no.nb.nna.veidemann.frontier.testutil.RedisData;
import no.nb.nna.veidemann.frontier.testutil.SkipUntilFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_READY_KEY;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_WAIT_KEY;
import static no.nb.nna.veidemann.frontier.testutil.FrontierAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

@Tag("integration")
@Tag("redis")
@Testcontainers
public class CrawlHostGroupTest {
    private static final Logger LOG = LoggerFactory.getLogger(CrawlHostGroupTest.class);

    @Container
    public static GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:5.0.7-alpine"))
            .withExposedPorts(6379);

    JedisPool jedisPool;
    RedisData redisData;

    @BeforeEach
    public void cleanDb() throws DbQueryException, DbConnectionException {
        String redisHost = redis.getHost();
        Integer redisPort = redis.getFirstMappedPort();

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(24);
        jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort);
        redisData = new RedisData(jedisPool);
    }

    @AfterEach
    public void shutdown() {
        jedisPool.getResource().flushAll();
        jedisPool.close();
    }

    @Test
    public void testChgAddScript() throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            JedisContext ctx = JedisContext.forPool(jedisPool);

            String chgId1 = "myChgId";
            String chgId2 = "mySecondChgId";
            String eId1 = "myCrawlExecutionId";
            String eId2 = "mySecondCrawlExecutionId";
            Timestamp earliestFetchTimestamp1 = ProtoUtils.odtToTs(OffsetDateTime.parse("2107-12-03T10:15:30+01:00"));
            Timestamp earliestFetchTimestamp2 = ProtoUtils.odtToTs(OffsetDateTime.parse("2107-12-13T10:15:30+01:00"));

            ChgAddScript chgAddScript = new ChgAddScript();

            chgAddScript.run(ctx, chgId1, eId1, earliestFetchTimestamp1, false, 1000);
            assertThat(redisData)
                    .hasQueueTotalCount(1)
                    .crawlExecutionQueueCounts().hasNumberOfElements(1).hasQueueCount(eId1, 1);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(1).id(chgId1).hasQueueCount(1);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(1)
                    .element(0).hasTimestamp(earliestFetchTimestamp1).hasValue(chgId1);
            assertThat(redisData).busyQueue().hasNumberOfElements(0);
            assertThat(redisData).readyQueue().hasNumberOfElements(0);


            chgAddScript.run(ctx, chgId1, eId1, earliestFetchTimestamp2, false, 1000);
            assertThat(redisData)
                    .hasQueueTotalCount(2)
                    .crawlExecutionQueueCounts().hasNumberOfElements(1).hasQueueCount(eId1, 2);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(1).id(chgId1).hasQueueCount(2);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(1)
                    .element(0).hasTimestamp(earliestFetchTimestamp1).hasValue(chgId1);
            assertThat(redisData).busyQueue().hasNumberOfElements(0);
            assertThat(redisData).readyQueue().hasNumberOfElements(0);


            chgAddScript.run(ctx, chgId1, eId2, earliestFetchTimestamp2, false, 1000);
            assertThat(redisData)
                    .hasQueueTotalCount(3)
                    .crawlExecutionQueueCounts().hasNumberOfElements(2).hasQueueCount(eId1, 2).hasQueueCount(eId2, 1);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(1).id(chgId1).hasQueueCount(3);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(1)
                    .element(0).hasTimestamp(earliestFetchTimestamp1).hasValue(chgId1);
            assertThat(redisData).busyQueue().hasNumberOfElements(0);
            assertThat(redisData).readyQueue().hasNumberOfElements(0);


            chgAddScript.run(ctx, chgId2, eId2, earliestFetchTimestamp2, false, 1000);
            assertThat(redisData)
                    .hasQueueTotalCount(4)
                    .crawlExecutionQueueCounts().hasNumberOfElements(2).hasQueueCount(eId1, 2).hasQueueCount(eId2, 2);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(2)
                    .id(chgId1).hasQueueCount(3)
                    .id(chgId2).hasQueueCount(1);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(2)
                    .element(0).hasTimestamp(earliestFetchTimestamp1).hasValue(chgId1)
                    .element(1).hasTimestamp(earliestFetchTimestamp2).hasValue(chgId2);
            assertThat(redisData).busyQueue().hasNumberOfElements(0);
            assertThat(redisData).readyQueue().hasNumberOfElements(0);
        }
    }

    @Test
    public void testChgDelayedQueueScript() throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            JedisContext ctx = JedisContext.forPool(jedisPool);

            String chgId1 = "myChgId";
            String chgId2 = "mySecondChgId";
            String eId1 = "myCrawlExecutionId";
            String eId2 = "mySecondCrawlExecutionId";
            Timestamp earliestFetchTimestamp1 = ProtoUtils.getNowTs();
            Timestamp earliestFetchTimestamp2 = ProtoUtils.odtToTs(OffsetDateTime.parse("2107-12-13T10:15:30+01:00"));

            ChgAddScript chgAddScript = new ChgAddScript();
            ChgDelayedQueueScript chgDelayedQueueScript = new ChgDelayedQueueScript();

            // Add some CrawlHostGroups
            chgAddScript.run(ctx, chgId1, eId1, earliestFetchTimestamp1, false, 1000);
            chgAddScript.run(ctx, chgId1, eId1, earliestFetchTimestamp2, false, 1000);
            chgAddScript.run(ctx, chgId1, eId2, earliestFetchTimestamp2, false, 1000);
            chgAddScript.run(ctx, chgId2, eId2, earliestFetchTimestamp2, false, 1000);

            // Check expected state
            assertThat(redisData)
                    .hasQueueTotalCount(4)
                    .crawlExecutionQueueCounts().hasNumberOfElements(2).hasQueueCount(eId1, 2).hasQueueCount(eId2, 2);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(2)
                    .id(chgId1).hasQueueCount(3)
                    .id(chgId2).hasQueueCount(1);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(2)
                    .element(0).hasTimestamp(earliestFetchTimestamp1).hasValue(chgId1)
                    .element(1).hasTimestamp(earliestFetchTimestamp2).hasValue(chgId2);
            assertThat(redisData).busyQueue().hasNumberOfElements(0);
            assertThat(redisData).readyQueue().hasNumberOfElements(0);

            // Call DelayedQueue script
            Long moved = chgDelayedQueueScript.run(ctx, CHG_WAIT_KEY, CHG_READY_KEY);

            // Check expected state after move
            assertThat(moved)
                    .withFailMessage("Expected number of moved CrawlHostGroups from wait to ready to be <%d>, but was <%d>", 1, moved)
                    .isEqualTo(1);
            assertThat(redisData)
                    .hasQueueTotalCount(4)
                    .crawlExecutionQueueCounts().hasNumberOfElements(2).hasQueueCount(eId1, 2).hasQueueCount(eId2, 2);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(2)
                    .id(chgId1).hasQueueCount(3)
                    .id(chgId2).hasQueueCount(1);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(1)
                    .element(0).hasTimestamp(earliestFetchTimestamp2).hasValue(chgId2);
            assertThat(redisData).busyQueue().hasNumberOfElements(0);
            assertThat(redisData).readyQueue().hasNumberOfElements(1)
                    .containsExactly(chgId1);
        }
    }

    class ToStdOutConsumer extends BaseConsumer<ToStdOutConsumer> {
        @Override
        public void accept(OutputFrame outputFrame) {
            System.out.println(outputFrame.getUtf8String());
        }
    }

    @Test
    public void testChgNextScript() throws Exception {
        redis.followOutput(new SkipUntilFilter("Ready to accept connections", new ToStdOutConsumer()));

        try (Jedis jedis = jedisPool.getResource()) {
            JedisContext ctx = JedisContext.forPool(jedisPool);

            String chgId1 = "myFirstChgId";
            String chgId2 = "mySecondChgId";
            String chgId3 = "myThirdChgId";
            String eId1 = "myFirstCrawlExecutionId";
            String eId2 = "mySecondCrawlExecutionId";
            String eId3 = "myThirdCrawlExecutionId";
            String eId4 = "myFourthCrawlExecutionId";
            Timestamp earliestFetchTimestamp1 = ProtoUtils.getNowTs();
            Timestamp earliestFetchTimestamp2 = Timestamps.add(earliestFetchTimestamp1, Durations.fromSeconds(1));

            ChgAddScript chgAddScript = new ChgAddScript();
            ChgDelayedQueueScript chgDelayedQueueScript = new ChgDelayedQueueScript();
            ChgNextScript chgNextScript = new ChgNextScript().withWaitForReadyTimeout(1);
            ChgUpdateScript chgUpdateScript = new ChgUpdateScript();
            ChgGetScript chgGetScript = new ChgGetScript();
            ChgReleaseScript chgReleaseScript = new ChgReleaseScript();

            // Add some CrawlHostGroups and move to ready
            chgAddScript.run(ctx, chgId1, eId1, earliestFetchTimestamp1, false, 1000);
            chgAddScript.run(ctx, chgId1, eId1, earliestFetchTimestamp1, false, 1000);
            chgAddScript.run(ctx, chgId1, eId2, earliestFetchTimestamp2, false, 1000);
            chgAddScript.run(ctx, chgId2, eId3, earliestFetchTimestamp1, false, 1000);
            chgAddScript.run(ctx, chgId3, eId4, earliestFetchTimestamp2, false, 1000);
            Long moved = chgDelayedQueueScript.run(ctx, CHG_WAIT_KEY, CHG_READY_KEY);

            // Check expected state
            Long expectedMoved = 2L;
            assertThat(moved)
                    .withFailMessage("Expected number of moved CrawlHostGroups from wait to ready to be <%d>, but was <%d>", expectedMoved, moved)
                    .isEqualTo(expectedMoved);
            assertThat(redisData)
                    .hasQueueTotalCount(5)
                    .crawlExecutionQueueCounts().hasNumberOfElements(4)
                    .hasQueueCount(eId1, 2)
                    .hasQueueCount(eId2, 1)
                    .hasQueueCount(eId3, 1)
                    .hasQueueCount(eId4, 1);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(3)
                    .id(chgId1).hasQueueCount(3)
                    .id(chgId2).hasQueueCount(1)
                    .id(chgId3).hasQueueCount(1);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(1)
                    .element(0).hasTimestamp(earliestFetchTimestamp2).hasValue(chgId3);
            assertThat(redisData).busyQueue().hasNumberOfElements(0);
            assertThat(redisData).readyQueue().hasNumberOfElements(2)
                    .containsExactly(chgId1, chgId2);

            CrawlHostGroup chg1 = chgNextScript.run(ctx, 2000);
            assertThat(chg1).hasQueueCount(3);

            assertThat(redisData)
                    .hasQueueTotalCount(5)
                    .crawlExecutionQueueCounts().hasNumberOfElements(4)
                    .hasQueueCount(eId1, 2)
                    .hasQueueCount(eId2, 1)
                    .hasQueueCount(eId3, 1)
                    .hasQueueCount(eId4, 1);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(3)
                    .id(chgId1).hasQueueCount(3)
                    .id(chgId2).hasQueueCount(1)
                    .id(chgId3).hasQueueCount(1);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(1)
                    .element(0).hasTimestamp(earliestFetchTimestamp2).hasValue(chgId3);
            assertThat(redisData).busyQueue().hasNumberOfElements(1).element(0)
                    .hasTimestampCloseTo(Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(2)), within(200, ChronoUnit.MILLIS))
                    .hasValue(chgId1);
            assertThat(redisData).readyQueue().hasNumberOfElements(1)
                    .containsExactly(chgId2);

            CrawlHostGroup chg2 = chgNextScript.run(ctx, 2000);
            assertThat(chg2).hasQueueCount(1);
            assertThat(redisData).busyQueue().hasNumberOfElements(2)
                    .element(0)
                    .hasTimestampCloseTo(Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(2)), within(200, ChronoUnit.MILLIS))
                    .hasValue(chgId1)
                    .element(1)
                    .hasTimestampCloseTo(Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(2)), within(200, ChronoUnit.MILLIS))
                    .hasValue(chgId2);
            assertThat(redisData).readyQueue().hasNumberOfElements(0);

            CrawlHostGroup chg3 = chgNextScript.run(ctx, 2000);
            assertThat(chg3).isNull();
//            System.out.println(redis.getLogs());
//            System.out.println("LOGS\n" + toStringConsumer.toUtf8String());
            Timestamp fetchStartTimestamp = ProtoUtils.getNowTs();
            chg1 = chg1.toBuilder()
                    .setCurrentUriId("uri1")
                    .setFetchStartTimeStamp(fetchStartTimestamp)
                    .setSessionToken("sess1")
                    .setRetryDelaySeconds(1)
                    .setMaxRetries(3)
                    .setMinTimeBetweenPageLoadMs(10)
                    .setMaxTimeBetweenPageLoadMs(1000)
                    .setDelayFactor(1.5f)
                    .build();
            chgUpdateScript.run(ctx, chg1);

            assertThat(redisData)
                    .hasQueueTotalCount(5)
                    .crawlExecutionQueueCounts().hasNumberOfElements(4)
                    .hasQueueCount(eId1, 2)
                    .hasQueueCount(eId2, 1)
                    .hasQueueCount(eId3, 1)
                    .hasQueueCount(eId4, 1);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(3)
                    .id(chgId1)
                    .hasQueueCount(3)
                    .hasPolitenessValues(10, 1000, 3, 1, 1.5f)
                    .hasSessionToken("sess1")
                    .hasCurrentUriId("uri1")
                    .hasFetchStartTimeStamp(fetchStartTimestamp)
                    .id(chgId2).hasQueueCount(1)
                    .id(chgId3).hasQueueCount(1);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(1)
                    .element(0).hasTimestamp(earliestFetchTimestamp2).hasValue(chgId3);
            assertThat(redisData).busyQueue().hasNumberOfElements(2)
                    .element(0)
                    .hasValue(chgId1)
                    .hasTimestampCloseTo(Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(1)), within(2000, ChronoUnit.MILLIS))
                    .element(1)
                    .hasValue(chgId2)
                    .hasTimestampCloseTo(Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(1)), within(2000, ChronoUnit.MILLIS));
            assertThat(redisData).readyQueue().hasNumberOfElements(0);
            assertThat(redisData).sessionTokens().hasNumberOfElements(1).hasCrawlHostId("sess1", chgId1);

            CrawlHostGroup result = chgGetScript.run(ctx,chgId1);
            assertThat(result).isEqualTo(chg1);

            chgReleaseScript.run(ctx, chgId1, "sess1", 1999);
            assertThat(redisData)
                    .hasQueueTotalCount(5)
                    .crawlExecutionQueueCounts().hasNumberOfElements(4)
                    .hasQueueCount(eId1, 2)
                    .hasQueueCount(eId2, 1)
                    .hasQueueCount(eId3, 1)
                    .hasQueueCount(eId4, 1);
            assertThat(redisData)
                    .crawlHostGroups().hasNumberOfElements(3)
                    .id(chgId1)
                    .hasQueueCount(3)
                    .hasPolitenessValues(0, 0, 0, 0, 0)
                    .hasSessionToken("")
                    .hasCurrentUriId("")
                    .id(chgId2).hasQueueCount(1)
                    .id(chgId3).hasQueueCount(1);
            assertThat(redisData).waitQueue()
                    .hasNumberOfElements(2)
                    .element(0).hasValue(chgId3).hasTimestamp(earliestFetchTimestamp2)
                    .element(1).hasTimestampCloseTo(Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(2)), within(200, ChronoUnit.MILLIS));
            assertThat(redisData).busyQueue().hasNumberOfElements(1)
//                    .element(0)
//                    .hasValue(chgId1)
//                    .hasTimestampCloseTo(Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(1)), within(200, ChronoUnit.MILLIS))
                    .element(0)
                    .hasValue(chgId2)
                    .hasTimestampCloseTo(Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(1)), within(2000, ChronoUnit.MILLIS));
            assertThat(redisData).readyQueue().hasNumberOfElements(0);
            assertThat(redisData).sessionTokens().hasNumberOfElements(0);
        }
    }

    public void forOtherTests() {
        UriAddScript uriAddScript = new UriAddScript();
        UriRemoveScript uriRemoveScript = new UriRemoveScript();
        NextUriScript nextUriScript = new NextUriScript();
        ChgAddScript chgAddScript = new ChgAddScript();
        ChgNextScript getNextChgScript = new ChgNextScript();
        ChgReleaseScript releaseChgScript = new ChgReleaseScript();
        ChgQueueCountScript countChgScript = new ChgQueueCountScript();
        ChgUpdateBusyTimeoutScript chgUpdateBusyTimeoutScript = new ChgUpdateBusyTimeoutScript();
        ChgUpdateScript chgUpdateScript = new ChgUpdateScript();
        ChgGetScript chgGetScript = new ChgGetScript();
        JobExecutionGetScript jobExecutionGetScript = new JobExecutionGetScript();
        JobExecutionUpdateScript jobExecutionUpdateScript = new JobExecutionUpdateScript();
    }
}