package no.nb.nna.veidemann.frontier.db.script;

import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import no.nb.nna.veidemann.frontier.db.script.NextUriScript.NextUriScriptResult;
import redis.clients.jedis.Tuple;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Set;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.UCHG;
import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.UEID;

public class NextUriScript extends RedisJob<NextUriScriptResult> {
    public NextUriScript() {
        super("nextUriScript");
    }

    public static class NextUriScriptResult {
        public final String id;
        public final String chgId;
        public final String eid;
        public final long sequence;
        public final long fetchTime;
        public final FutureOptional<QueuedUri> future;

        public NextUriScriptResult(String id, String chgId, String eid, long sequence, long fetchTime) {
            this.id = id;
            this.chgId = chgId;
            this.eid = eid;
            this.sequence = sequence;
            this.fetchTime = fetchTime;
            this.future = null;
        }

        public NextUriScriptResult(FutureOptional<QueuedUri> future) {
            this.id = null;
            this.chgId = null;
            this.eid = null;
            this.sequence = 0;
            this.fetchTime = 0;
            this.future = future;
        }
    }

    public NextUriScriptResult run(JedisContext ctx, CrawlHostGroup crawlHostGroup) {
        return execute(ctx, jedis -> {
            String chgId = crawlHostGroup.getId();
            // Find the crawl execution with the highest score
            Set<Tuple> mResult = jedis.zrevrangeByScoreWithScores(UCHG + chgId, "+inf", "-inf", 0, 1);
            if (mResult.isEmpty()) {
                return new NextUriScriptResult(FutureOptional.empty());
            }
            double maxScore = mResult.iterator().next().getScore();

            // Choose weighted random crawl execution
            String key = UCHG + chgId;
            String minScore = String.valueOf(Math.random() * maxScore);
            Long matchCount = jedis.zcount(key, minScore, "+inf");
            long offset = (int) (Math.random() * (matchCount -1));
            Set<String> eResult = jedis.zrangeByScore(key, minScore, "+inf", (int) offset, 1);
            if (eResult.isEmpty()) {
                return new NextUriScriptResult(FutureOptional.empty());
            }
            String eid = eResult.iterator().next();

            // Get first URI for crawl execution
            Set<String> uResult = jedis.zrange(UEID + chgId + ":" + eid, 0, 0);
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
                        chgId,
                        eid,
                        nextUrlSequence,
                        nextUrlEarliestFetch);
            }
        });
    }
}
