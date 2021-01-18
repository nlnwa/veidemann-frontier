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
    final LuaScript chgNextScript;

    public ChgNextScript() {
        super("chgNext");
        chgNextScript = new LuaScript("chg_next.lua");
    }

    public CrawlHostGroup run(JedisContext ctx, long busyTimeout) {
        return execute(ctx, jedis -> {
            List<String> res = jedis.brpop(5, CHG_READY_KEY);
            if (res == null) {
                if (LOG.isInfoEnabled()) {
                    long now = System.currentTimeMillis();
                    for (Tuple t : jedis.zrangeWithScores(CHG_WAIT_KEY, 0, 0)) {
                        LOG.debug("No ready crawlhost group. An idle CHG with key {} is waiting due to politeness for {} ms", t.getElement(), (long) t.getScore() - now);
                    }
                    for (Tuple t : jedis.zrangeWithScores(CHG_BUSY_KEY, 0, 0)) {
                        LOG.debug("No ready crawlhost group. A busy CHG with key {} will be realeased in {} ms if fetch is to slow", t.getElement(), (long) t.getScore() - now);
                    }
                }
                return null;
            }
            String chgp = res.get(1);
            String chgpKey = CHG_PREFIX + chgp;
            List<byte[]> keys = ImmutableList.of(CHG_BUSY_KEY.getBytes(), chgpKey.getBytes());
            List<byte[]> args = ImmutableList.of(chgp.getBytes(), String.valueOf(System.currentTimeMillis() + busyTimeout).getBytes());
            byte[] result = (byte[]) chgNextScript.runBytes(jedis, keys, args);

            String[] idParts = chgp.split(":", 2);
            CrawlHostGroup.Builder chg = CrawlHostGroup.newBuilder()
                    .setId(idParts[0])
                    .setPolitenessId(idParts[1]);
            if (result != null) {
                Long count = Longs.tryParse(new String(result));
                if (count != null) {
                    chg.setQueuedUriCount(count);
                }
            }
            return chg.build();
        });
    }
}
