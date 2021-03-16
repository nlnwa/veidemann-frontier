package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class UriRemoveScript extends RedisJob<Long> {
    final LuaScript uriRemoveScript;

    public UriRemoveScript() {
        super("uriRemoveScript");
        uriRemoveScript = new LuaScript("uri_remove.lua");
    }

    public long run(JedisContext ctx, String uriId, String chgId, String eid, long sequence, long fetchTime, boolean deleteUri) {
        return execute(ctx, jedis -> {
            if (uriId == null || uriId.isEmpty()) {
                new RuntimeException("Missing id: " + uriId).printStackTrace();
            }
            String ueIdKey = String.format("%s%s:%s",
                    UEID,
                    chgId,
                    eid);
            String ueIdVal = String.format("%4d:%d:%s",
                    sequence,
                    fetchTime,
                    uriId);
            String uchgKey = String.format("%s%s",
                    UCHG,
                    chgId);
            String chgKey = CHG_PREFIX + chgId;

            String removeQueue = "";
            if (deleteUri) {
                removeQueue = uriId;
            }

            List<String> keys = ImmutableList.of(ueIdKey, uchgKey, chgKey, CRAWL_EXECUTION_ID_COUNT_KEY,
                    QUEUE_COUNT_TOTAL_KEY, REMOVE_URI_QUEUE_KEY);
            List<String> args = ImmutableList.of(ueIdVal, eid, removeQueue);
            long urisRemoved = (long) uriRemoveScript.runString(jedis, keys, args);
            return urisRemoved;
        });
    }
}
