package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class ChgAddScript extends RedisJob<Boolean> {
    final LuaScript chgAddScript;

    public ChgAddScript() {
        super("chgAddScript");
        chgAddScript = new LuaScript("chg_add.lua");
    }

    /**
     * Add uri to queue.
     *
     * @param ctx
     * @param attemptToSetAsBusy if true, try to set uri's chg to busy
     * @param busyTimeout        if it is possible to set chg as busy, this is the timeout
     * @return true if chg was set to busy, false otherwise
     */
    public boolean run(JedisContext ctx, String chgId, String crawlExecutionId, Timestamp earliestFetchTimestamp, boolean attemptToSetAsBusy, long busyTimeout) {
        return execute(ctx, jedis -> {
            String chgKey = CHG_PREFIX + chgId;

            // Handle CHG and counters
            long readyTime = Timestamps.toMillis(earliestFetchTimestamp);
            String readyTimeString = Long.toString(readyTime);
            String busyExpireTime = String.valueOf(System.currentTimeMillis() + busyTimeout);

            List<String> chgKeys = ImmutableList.of(chgKey, CHG_WAIT_KEY, CHG_BUSY_KEY, CHG_READY_KEY,
                    CRAWL_EXECUTION_ID_COUNT_KEY, QUEUE_COUNT_TOTAL_KEY);
            List<String> chgArgs = ImmutableList.of(readyTimeString, crawlExecutionId, chgId, String.valueOf(attemptToSetAsBusy), busyExpireTime);
            String queueCount = (String) chgAddScript.runString(jedis, chgKeys, chgArgs);

            return queueCount.equals("true");
        });
    }
}
