package no.nb.nna.veidemann.frontier.db.script;

import com.google.common.collect.ImmutableList;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_BUSY_KEY;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.CHG_WAIT_KEY;

public class ChgReleaseScript extends RedisJob<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ChgReleaseScript.class);
    final LuaScript chgRealeaseScript;

    public ChgReleaseScript() {
        super("release chg");
        chgRealeaseScript = new LuaScript("chg_release.lua");
    }

    public void run(JedisContext ctx, CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) {
        if (nextFetchDelayMs <= 0) {
            nextFetchDelayMs = 10;
        }
        String chgp = CrawlQueueManager.createChgPolitenessKey(crawlHostGroup);
        String chgpKey = CrawlQueueManager.CHG_PREFIX + chgp;

        long readyTime = System.currentTimeMillis() + nextFetchDelayMs;

        List<String> keys = ImmutableList.of(CHG_BUSY_KEY, CHG_WAIT_KEY, chgpKey);
        List<String> args = ImmutableList.of(String.valueOf(readyTime), chgp);

        execute(ctx, jedis -> {
            chgRealeaseScript.runString(jedis, keys, args);
            return null;
        });
    }
}
