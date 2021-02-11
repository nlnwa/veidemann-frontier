package no.nb.nna.veidemann.frontier.db.script;

import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;

import java.util.Map;

import static no.nb.nna.veidemann.frontier.db.CrawlQueueManager.JOB_EXECUTION_PREFIX;

public class JobExecutionGetScript extends RedisJob<JobExecutionStatus> {
    public JobExecutionGetScript() {
        super("jobExecutionGetScript");
    }

    public JobExecutionStatus run(JedisContext ctx, String jobExecutionId) {
        return execute(ctx, jedis -> {
            String key = JOB_EXECUTION_PREFIX + jobExecutionId;

            JobExecutionStatusRedisMapper m = new JobExecutionStatusRedisMapper(jedis.hgetAll(key));

            return m.toJobExecutionStatus(jobExecutionId);
        });
    }

    private static class JobExecutionStatusRedisMapper {
        private Map<String, String> values;

        public JobExecutionStatusRedisMapper(Map<String, String> values) {
            this.values = values;
        }

        private long getAsLong(String field) {
            return Long.parseLong(values.getOrDefault(field, "0"));
        }

        private int getAsInt(String field) {
            return Integer.parseInt(values.getOrDefault(field, "0"));
        }

        public JobExecutionStatus toJobExecutionStatus(String jobExecutionId) {
            JobExecutionStatus.Builder jes = JobExecutionStatus.newBuilder()
                    .setId(jobExecutionId)
                    .setDocumentsCrawled(getAsLong("documentsCrawled"))
                    .setDocumentsDenied(getAsLong("documentsDenied"))
                    .setDocumentsFailed(getAsLong("documentsFailed"))
                    .setDocumentsOutOfScope(getAsLong("documentsOutOfScope"))
                    .setDocumentsRetried(getAsLong("documentsRetried"))
                    .setUrisCrawled(getAsLong("urisCrawled"))
                    .setBytesCrawled(getAsLong("bytesCrawled"));

            for (CrawlExecutionStatus.State s : CrawlExecutionStatus.State.values()) {
                jes.putExecutionsState(s.name(), getAsInt(s.name()));
            }
            return jes.build();
        }
    }
}
