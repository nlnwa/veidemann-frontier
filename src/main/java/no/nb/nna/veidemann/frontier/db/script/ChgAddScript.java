package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class ChgAddScript extends RedisJob<Long> {
    final LuaScript chgAddScript;

    public ChgAddScript() {
        super("chgAddScript");
        chgAddScript = new LuaScript("chg_add.lua");
    }

    /**
     * Add uri to queue.
     *
     * @param ctx
     * @param busyTimeout if it is possible to set chg as busy, this is the timeout
     * @return number of uris in queue for this CrawlHostGroup
     */
    public long run(JedisContext ctx, String chgId, String crawlExecutionId, Timestamp earliestFetchTimestamp, long busyTimeout) {
        return execute(ctx, jedis -> {
            String chgKey = CHG_PREFIX + chgId;

            // Handle CHG and counters
            long readyTime = Timestamps.toMillis(earliestFetchTimestamp);
            String readyTimeString = Long.toString(readyTime);

            List<String> chgKeys = ImmutableList.of(chgKey, CHG_WAIT_KEY, CRAWL_EXECUTION_ID_COUNT_KEY, QUEUE_COUNT_TOTAL_KEY);
            List<String> chgArgs = ImmutableList.of(readyTimeString, crawlExecutionId, chgId);
            return (Long) chgAddScript.runString(jedis, chgKeys, chgArgs);
        });
    }
}
