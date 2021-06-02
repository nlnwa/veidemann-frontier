package no.nb.nna.veidemann.frontier.db.script;

import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Codec for encoding/decoding between CrawlHostGroup protobufs and list which is used by redis.
 */
public class CrawlHostGroupCodec {
    // Lover limit for time between pageloads from this CrawlHostGroup.
    private static final String MIN_TIME_BETWEEN_PAGE_LOAD_MS = "mi";
    // Upper limit for time between pageloads from this CrawlHostGroup.
    // This is the upper limit for calculation of dealy time, but actual time might be higher depending on
    // the harvesters capacity.
    private static final String MAX_TIME_BETWEEN_PAGE_LOAD_MS = "ma";
    // The fetch time of the URI is multiplied with this value to get the delay time before fetching the next URI.
    // If min_time_between_page_load_ms and/or max_time_between_page_load_ms are set, then those values are used as
    // the upper/lower limits for delay.
    // If delay_factor is unset or zero, then a delay_facor of one is assumed. If delay_factor is negative,
    // a delay_factor of zero is assumed.
    private static final String DELAY_FACTOR = "df";
    // The maximum number of retries before giving up fetching a uri.
    private static final String MAX_RETRIES = "mr";
    // The minimum time before a failed page load is retried.
    private static final String RETRY_DELAY_SECONDS = "rd";
    // The number of queued Uri's belonging to this CrawlHostGroup
    private static final String QUEUED_URI_COUNT = "qc";
    // If this CrawlHostGroup is busy, this field contains the id of the uri currently beeing fetched.
    private static final String CURRENT_URI_ID = "u";
    // Token to guard against two harvesters responding to the same request.
    private static final String SESSION_TOKEN = "st";
    // The time when frontier sent a PageHarvestSpec to a harvester.
    private static final String FETCH_START_TIME_STAMP = "ts";

    private CrawlHostGroupCodec() {
    }

    public static List<String> encodeList(CrawlHostGroup chg) {
        List<String> encoded = new ArrayList<>();

        encoded.add(MIN_TIME_BETWEEN_PAGE_LOAD_MS);
        encoded.add(String.valueOf(chg.getMinTimeBetweenPageLoadMs()));

        encoded.add(MAX_TIME_BETWEEN_PAGE_LOAD_MS);
        encoded.add(String.valueOf(chg.getMaxTimeBetweenPageLoadMs()));

        encoded.add(DELAY_FACTOR);
        encoded.add(String.valueOf(chg.getDelayFactor()));

        encoded.add(MAX_RETRIES);
        encoded.add(String.valueOf(chg.getMaxRetries()));

        encoded.add(RETRY_DELAY_SECONDS);
        encoded.add(String.valueOf(chg.getRetryDelaySeconds()));

        // Do not encode QUEUED_URI_COUNT (qc) since that value is manipulated with Redis HINCR
        // encoded.add(QUEUED_URI_COUNT);
        // encoded.add(String.valueOf(chg.getQueuedUriCount()));

        encoded.add(CURRENT_URI_ID);
        encoded.add(chg.getCurrentUriId());

        encoded.add(SESSION_TOKEN);
        encoded.add(chg.getSessionToken());

        encoded.add(FETCH_START_TIME_STAMP);
        encoded.add(Long.toString(Timestamps.toMillis(chg.getFetchStartTimeStamp())));

        return encoded;
    }

    public static Map<String, String> encodeMap(CrawlHostGroup chg) {
        Map<String, String> encoded = new HashMap<>();

        encoded.put(MIN_TIME_BETWEEN_PAGE_LOAD_MS, String.valueOf(chg.getMinTimeBetweenPageLoadMs()));
        encoded.put(MAX_TIME_BETWEEN_PAGE_LOAD_MS, String.valueOf(chg.getMaxTimeBetweenPageLoadMs()));
        encoded.put(DELAY_FACTOR, String.valueOf(chg.getDelayFactor()));
        encoded.put(MAX_RETRIES, String.valueOf(chg.getMaxRetries()));
        encoded.put(RETRY_DELAY_SECONDS, String.valueOf(chg.getRetryDelaySeconds()));
        // Do not encode QUEUED_URI_COUNT (qc) since that value is manipulated with Redis HINCR
        // encoded.put(QUEUED_URI_COUNT, String.valueOf(chg.getQueuedUriCount()));
        encoded.put(CURRENT_URI_ID, chg.getCurrentUriId());
        encoded.put(SESSION_TOKEN, chg.getSessionToken());
        encoded.put(FETCH_START_TIME_STAMP, Long.toString(Timestamps.toMillis(chg.getFetchStartTimeStamp())));

        return encoded;
    }

    public static CrawlHostGroup decode(String chgId, List<String> encoded) {
        CrawlHostGroup.Builder chg = CrawlHostGroup.newBuilder().setId(chgId);
        for (Iterator<String> it = encoded.iterator(); it.hasNext(); ) {
            String field = it.next();
            switch (field) {
                case MIN_TIME_BETWEEN_PAGE_LOAD_MS:
                    chg.setMinTimeBetweenPageLoadMs(Long.parseLong(it.next()));
                    break;
                case MAX_TIME_BETWEEN_PAGE_LOAD_MS:
                    chg.setMaxTimeBetweenPageLoadMs(Long.parseLong(it.next()));
                    break;
                case DELAY_FACTOR:
                    chg.setDelayFactor(Float.parseFloat(it.next()));
                    break;
                case MAX_RETRIES:
                    chg.setMaxRetries(Integer.parseInt(it.next()));
                    break;
                case RETRY_DELAY_SECONDS:
                    chg.setRetryDelaySeconds(Integer.parseInt(it.next()));
                    break;
                case QUEUED_URI_COUNT:
                    chg.setQueuedUriCount(Long.parseLong(it.next()));
                    break;
                case CURRENT_URI_ID:
                    chg.setCurrentUriId(it.next());
                    break;
                case SESSION_TOKEN:
                    chg.setSessionToken(it.next());
                    break;
                case FETCH_START_TIME_STAMP:
                    long ts = Long.parseLong(it.next());
                    if (ts > 0) {
                        chg.setFetchStartTimeStamp(Timestamps.fromMillis(ts));
                    }
                    break;
                default:
            }
        }
        return chg.build();
    }

    public static CrawlHostGroup decode(String chgId, Map<String, String> encoded) {
        CrawlHostGroup.Builder chg = CrawlHostGroup.newBuilder().setId(chgId);
        chg.setMinTimeBetweenPageLoadMs(Long.parseLong(encoded.getOrDefault(MIN_TIME_BETWEEN_PAGE_LOAD_MS, "0")));
        chg.setMaxTimeBetweenPageLoadMs(Long.parseLong(encoded.getOrDefault(MAX_TIME_BETWEEN_PAGE_LOAD_MS, "0")));
        chg.setDelayFactor(Float.parseFloat(encoded.getOrDefault(DELAY_FACTOR, "0")));
        chg.setMaxRetries(Integer.parseInt(encoded.getOrDefault(MAX_RETRIES, "0")));
        chg.setRetryDelaySeconds(Integer.parseInt(encoded.getOrDefault(RETRY_DELAY_SECONDS, "0")));
        chg.setQueuedUriCount(Long.parseLong(encoded.getOrDefault(QUEUED_URI_COUNT, "0")));
        chg.setCurrentUriId(encoded.getOrDefault(CURRENT_URI_ID, ""));
        chg.setSessionToken(encoded.getOrDefault(SESSION_TOKEN, ""));
        long ts = Long.parseLong(encoded.getOrDefault(FETCH_START_TIME_STAMP, "0"));
        if (ts > 0) {
            chg.setFetchStartTimeStamp(Timestamps.fromMillis(ts));
        }
        return chg.build();
    }
}
