package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;

import java.util.List;
import java.util.Locale;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.UCHG;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.UEID;

public class UriAddScript extends RedisJob<Boolean> {
    final LuaScript uriAddScript;

    public UriAddScript() {
        super("uriAddScript");
        uriAddScript = new LuaScript("uri_add.lua");
    }

    /**
     * Add uri to queue.
     *
     * @param ctx
     * @param qUri the uri to add
     */
    public void run(JedisContext ctx, QueuedUri qUri) {
        execute(ctx, jedis -> {
            String chgId = qUri.getCrawlHostGroupId();
            String ueIdKey = String.format("%s%s:%s",
                    UEID,
                    chgId,
                    qUri.getExecutionId());
            String ueIdVal = String.format("%4d:%d:%s",
                    qUri.getSequence(),
                    qUri.getEarliestFetchTimeStamp().getSeconds(),
                    qUri.getId());
            String uchgKey = String.format("%s%s",
                    UCHG,
                    chgId);
            String weight = String.format(Locale.ENGLISH, "%1.2f", qUri.getPriorityWeight());
            String eid = qUri.getExecutionId();
            List<String> keys = ImmutableList.of(ueIdKey, uchgKey);
            List<String> args = ImmutableList.of(ueIdVal, weight, eid);

            uriAddScript.runString(jedis, keys, args);
            return null;
        });
    }
}
