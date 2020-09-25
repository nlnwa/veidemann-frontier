package no.nb.nna.veidemann.frontier.db.script;

import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import no.nb.nna.veidemann.frontier.db.script.NextUriScript.NextUriScriptResult;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Set;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.*;

public class NextUriScript extends RedisJob<NextUriScriptResult> {
    public NextUriScript(JedisPool jedisPool) {
        super(jedisPool, "nexturi");
    }

    public static class NextUriScriptResult {
        public final String id;
        public final String chgp;
        public final String eid;
        public final long sequence;
        public final long fetchTime;
        public final FutureOptional<QueuedUri> future;

        public NextUriScriptResult(String id, String chgp, String eid, long sequence, long fetchTime) {
            this.id = id;
            this.chgp = chgp;
            this.eid = eid;
            this.sequence = sequence;
            this.fetchTime = fetchTime;
            this.future = null;
        }

        public NextUriScriptResult(FutureOptional<QueuedUri> future) {
            this.id = null;
            this.chgp = null;
            this.eid = null;
            this.sequence = 0;
            this.fetchTime = 0;
            this.future = future;
        }
    }

    public NextUriScriptResult run(CrawlHostGroup crawlHostGroup) {
        return execute(jedis -> {
            String chg = createChgPolitenessKey(crawlHostGroup);
            // Find the crawl execution with the highest score
            Set<Tuple> mResult = jedis.zrevrangeByScoreWithScores(UCHG + chg, "+inf", "-inf", 0, 1);
            if (mResult.isEmpty()) {
                return new NextUriScriptResult(FutureOptional.empty());
            }
            double maxScore = mResult.iterator().next().getScore();

            // Choose weighted random crawl execution
            Set<String> eResult = jedis.zrangeByScore(UCHG + chg, String.valueOf(Math.random() * maxScore), "+inf", 0, 1);
            if (eResult.isEmpty()) {
                return new NextUriScriptResult(FutureOptional.empty());
            }
            String eid = eResult.iterator().next();

            // Get first URI for crawl execution
            Set<String> uResult = jedis.zrange(UEID + chg + ":" + eid, 0, 0);
            if (uResult.isEmpty()) {
                return new NextUriScriptResult(FutureOptional.empty());
            }
            String[] uEidValParts = uResult.iterator().next().split(":", 3);
            long nextUrlSequence = Long.parseLong(uEidValParts[0].trim());
            long nextUrlEarliestFetch = Long.parseLong(uEidValParts[1].trim());
            String nextUrl = uEidValParts[2];

            // Check if URI is fetchable now or in the future
            if ((System.currentTimeMillis() / 1000) < nextUrlEarliestFetch) {
                OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochSecond((int) nextUrlEarliestFetch), ZoneId.systemDefault());
                return new NextUriScriptResult(FutureOptional.emptyUntil(odt));
            } else {
                // A fetchable URI was found, return it
                return new NextUriScriptResult(
                        nextUrl,
                        chg,
                        eid,
                        nextUrlSequence,
                        nextUrlEarliestFetch);
            }
        });
    }
}
