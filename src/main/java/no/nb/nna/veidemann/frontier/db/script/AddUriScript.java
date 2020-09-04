package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import redis.clients.jedis.JedisPool;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class AddUriScript extends RedisJob<Void> {
    final LuaScript addUriScript;

    public AddUriScript(JedisPool jedisPool) {
        super(jedisPool, "adduri");
        addUriScript = new LuaScript("adduri.lua");
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
            CrawlHostGroup chg = CrawlHostGroup.newBuilder()
                    .setId(qUri.getCrawlHostGroupId())
                    .setPolitenessId(qUri.getPolitenessRef().getId())
                    .setNextFetchTime(qUri.getEarliestFetchTimeStamp())
                    .setBusy(false)
                    .build();

            chg = chg.toBuilder().setBusy(false).build();
            long readyTime = Timestamps.toMillis(qUri.getEarliestFetchTimeStamp());

            long changes = jedis.hset(chgpKey.getBytes(), serializeCrawlHostGroup(chg));
            // Increment chg queue count
            jedis.hincrBy(chgpKey, "c", 1);
            // Increment crawl execution queue count
            jedis.hincrBy(EIDC, eid, 1);
            // Increment total queue count
            jedis.incr(QUEUE_COUNT_TOTAL_KEY);

            // If new chg was created, queue it.
            if (changes > 0) {
                if (readyTime > System.currentTimeMillis()) {
                    jedis.zadd(CHG_WAIT_KEY, readyTime, chgp);
                } else {
                    jedis.rpush(CHG_READY_KEY, chgp);
                }
            }
            return null;
        });
    }
}
