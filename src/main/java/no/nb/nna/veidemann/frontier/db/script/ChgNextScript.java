package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Tuple;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class ChgNextScript extends RedisJob<CrawlHostGroup> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgNextScript.class);
    private static final int DEFAULT_WAIT_FOR_READY_TIMEOUT = 5;

    final LuaScript chgNextScript;
    int waitForReadyTimeout = DEFAULT_WAIT_FOR_READY_TIMEOUT;

    public ChgNextScript() {
        super("chgNextScript");
        chgNextScript = new LuaScript("chg_next.lua");
    }

    public ChgNextScript withWaitForReadyTimeout(int waitForReadyTimeout) {
        this.waitForReadyTimeout = waitForReadyTimeout;
        return this;
    }

    public CrawlHostGroup run(JedisContext ctx, long busyTimeout) {
        return execute(ctx, jedis -> {
            List<String> res = jedis.blpop(waitForReadyTimeout, CHG_READY_KEY);
            if (res == null) {
                if (LOG.isTraceEnabled()) {
                    long now = System.currentTimeMillis();
                    for (Tuple t : jedis.zrangeWithScores(CHG_WAIT_KEY, 0, 0)) {
                        LOG.trace("No ready crawlhost group. An idle CHG with key {} is waiting due to politeness for {} ms", t.getElement(), (long) t.getScore() - now);
                    }
                    for (Tuple t : jedis.zrangeWithScores(CHG_BUSY_KEY, 0, 0)) {
                        LOG.trace("No ready crawlhost group. A busy CHG with key {} will be realeased in {} ms if fetch is to slow", t.getElement(), (long) t.getScore() - now);
                    }
                }
                return null;
            }
            String chgId = res.get(1);
            String chgKey = CHG_PREFIX + chgId;
            List<String> keys = ImmutableList.of(CHG_BUSY_KEY, chgKey);
            List<String> args = ImmutableList.of(chgId, String.valueOf(System.currentTimeMillis() + busyTimeout));
            String result = (String) chgNextScript.runString(jedis, keys, args);

            CrawlHostGroup.Builder chg = CrawlHostGroup.newBuilder()
                    .setId(chgId);
            if (result != null) {
                Long count = Longs.tryParse(result);
                if (count != null) {
                    chg.setQueuedUriCount(count);
                }
            }
            return chg.build();
        });
    }
}
