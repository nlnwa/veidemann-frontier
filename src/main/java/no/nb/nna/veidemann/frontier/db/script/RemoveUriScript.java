package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class RemoveUriScript extends RedisJob<Long> {
    final LuaScript removeUriScript;

    public RemoveUriScript() {
        super("removeUri");
        removeUriScript = new LuaScript("removeuri.lua");
    }

    public long run(JedisContext ctx, String uriId, String chgp, String eid, long sequence, long fetchTime, boolean deleteUri) {
        return execute(ctx, jedis -> {
            if (uriId == null || uriId.isEmpty()) {
                new RuntimeException("Missing id: " + uriId).printStackTrace();
            }
            String ueIdKey = String.format("%s%s:%s",
                    UEID,
                    chgp,
                    eid);
            String ueIdVal = String.format("%4d:%d:%s",
                    sequence,
                    fetchTime,
                    uriId);
            String uchgKey = String.format("%s%s",
                    UCHG,
                    chgp);
            String chgpKey = CHG_PREFIX + chgp;

            String removeQueue = "";
            if (deleteUri) {
                removeQueue = uriId;
            }

            List<String> keys = ImmutableList.of(ueIdKey, uchgKey, chgpKey, CHG_READY_KEY, CHG_BUSY_KEY,
                    CHG_WAIT_KEY, CRAWL_EXECUTION_ID_COUNT_KEY, QUEUE_COUNT_TOTAL_KEY, REMOVE_URI_QUEUE_KEY);
            List<String> args = ImmutableList.of(ueIdVal, eid, chgp, removeQueue);
            long urisRemoved = (long) removeUriScript.runString(jedis, keys, args);
            return urisRemoved;
        });
    }
}
