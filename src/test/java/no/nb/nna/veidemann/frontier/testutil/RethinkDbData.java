package no.nb.nna.veidemann.frontier.testutil;

import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.rethinkdb.RethinkDB.r;

public class RethinkDbData {
    final RethinkDbConnection conn;

    public RethinkDbData(RethinkDbConnection conn) {
        this.conn = conn;
    }

    public List<QueuedUri> getQueuedUris() throws DbQueryException, DbConnectionException {
        try (Cursor<Map<String, Object>> cursor = conn.exec(r.table(Tables.URI_QUEUE.name))) {
            return (List<QueuedUri>) cursor.toList().stream()
                    .map(v -> ProtoUtils.rethinkToProto(v, QueuedUri.class))
                    .collect(Collectors.toList());
        }
    }

    public Map<String, CrawlExecutionStatus> getCrawlExecutionStatuses() throws DbQueryException, DbConnectionException {
        try (Cursor<Map<String, Object>> cursor = conn.exec(r.table(Tables.EXECUTIONS.name))) {
            return cursor.toList().stream()
                    .map(v -> ProtoUtils.rethinkToProto(v, CrawlExecutionStatus.class))
                    .collect(Collectors.toMap(o -> o.getId(), o -> o));
        }
    }

    public Map<String, JobExecutionStatus> getJobExecutionStatuses() throws DbQueryException, DbConnectionException {
        try (Cursor<Map<String, Object>> cursor = conn.exec(r.table(Tables.JOB_EXECUTIONS.name))) {
            return cursor.toList().stream()
                    .map(v -> ProtoUtils.rethinkToProto(v, JobExecutionStatus.class))
                    .collect(Collectors.toMap(o -> o.getId(), o -> o));
        }
    }
}
