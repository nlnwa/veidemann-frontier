package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import redis.clients.jedis.JedisPool;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_PREFIX;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_WAIT_KEY;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.EIDC;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.NEXT_FETCH_TIME_FIELD;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.QUEUE_COUNT_TOTAL_KEY;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.UCHG;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.UEID;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.URI_COUNT_FIELD;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.createChgPolitenessKey;

public class AddUriScript extends RedisJob<Void> {
    final LuaScript addUriScript;
    final LuaScript addChgScript;

    public AddUriScript(JedisPool jedisPool) {
        super(jedisPool, "adduri");
        addUriScript = new LuaScript("adduri.lua");
        addChgScript = new LuaScript("chg_add.lua");
    }

    public void run(QueuedUri qUri) {
        execute(jedis -> {
            String chgp = createChgPolitenessKey(qUri);
            String ueIdKey = String.format("%s%s:%s",
                    UEID,
                    chgp,
                    qUri.getExecutionId());
            String ueIdVal = String.format("%4d:%d:%s",
                    qUri.getSequence(),
                    qUri.getEarliestFetchTimeStamp().getSeconds(),
                    qUri.getId());
            String uchgKey = String.format("%s%s",
                    UCHG,
                    chgp);
            String chgpKey = CHG_PREFIX + chgp;
            String weight = String.format("%1.2f", qUri.getPriorityWeight());
            String eid = qUri.getExecutionId();
            List<String> keys = ImmutableList.of(ueIdKey, uchgKey, chgpKey);
            List<String> args = ImmutableList.of(ueIdVal, weight, eid);

            addUriScript.runString(jedis, keys, args);

            // Handle CHG and counters
            long readyTime = Timestamps.toMillis(qUri.getEarliestFetchTimeStamp());
            if (readyTime < System.currentTimeMillis()) {
                readyTime = System.currentTimeMillis() + 10;
            }
            byte[] readyTimeString = Long.toString(readyTime).getBytes();

            List<byte[]> chgKeys = ImmutableList.of(chgpKey.getBytes(), CHG_WAIT_KEY.getBytes(),
                    EIDC.getBytes(), QUEUE_COUNT_TOTAL_KEY.getBytes());
            List<byte[]> chgArgs = ImmutableList.of(NEXT_FETCH_TIME_FIELD, readyTimeString, URI_COUNT_FIELD);
            addChgScript.runBytes(jedis, chgKeys, chgArgs);

            return null;
        });
    }
}
