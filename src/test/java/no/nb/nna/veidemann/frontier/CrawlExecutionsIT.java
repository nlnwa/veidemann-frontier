/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.frontier;

import com.google.protobuf.Timestamp;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.frontier.v1.Cookie;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.RethinkDbExecutionsAdapter;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.StatusWrapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CrawlExecutionsIT {

    public static RethinkDbConnection conn;
    public static RethinkDbExecutionsAdapter executionsAdapter;
    public static Frontier frontier;

    static RethinkDB r = RethinkDB.r;

    @BeforeClass
    public static void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        String redisHost = System.getProperty("redis.host");
        int redisPort = Integer.parseInt(System.getProperty("redis.port"));

        if (!DbService.isConfigured()) {
            CommonSettings settings = new CommonSettings();
            DbService.configure(new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword(""));
        }

        try {
            DbService.getInstance().getDbInitializer().delete();
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
        DbService.getInstance().getDbInitializer().initialize();

        executionsAdapter = (RethinkDbExecutionsAdapter) DbService.getInstance().getExecutionsAdapter();
        conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
        frontier = new Frontier(new JedisPool(redisHost, redisPort), null, null, null, null);
    }

    @AfterClass
    public static void shutdown() {
        DbService.getInstance().close();
    }

    @Before
    public void cleanDb() throws DbException {
        for (Tables table : Tables.values()) {
            if (table != Tables.SYSTEM) {
                try {
                    conn.exec("delete", r.table(table.name).delete());
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    @Test
    public void createCrawlExecutionStatus() throws DbException {
        JobExecutionStatus jes1 = executionsAdapter.createJobExecutionStatus("jobId1");
        CrawlExecutionStatus ces1 = frontier.createCrawlExecutionStatus("jobId1", jes1.getId(), "seed1");
        assertThat(ces1.getId()).isNotEmpty();
        assertThat(ces1.getJobId()).isEqualTo("jobId1");
        assertThat(ces1.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(ces1.getSeedId()).isEqualTo("seed1");
        assertThat(ces1.getState()).isEqualTo(CrawlExecutionStatus.State.CREATED);
        assertThat(ces1.hasLastChangeTime()).isTrue();
        assertThat(ces1.hasCreatedTime()).isTrue();
        assertThat(ces1.hasStartTime()).isFalse();
        assertThat(ces1.hasEndTime()).isFalse();
        assertThat(ces1.getDocumentsCrawled()).isEqualTo(0);
        assertThat(ces1.getDocumentsDenied()).isEqualTo(0);
        assertThat(ces1.getDocumentsFailed()).isEqualTo(0);
        assertThat(ces1.getDocumentsOutOfScope()).isEqualTo(0);
        assertThat(ces1.getDocumentsRetried()).isEqualTo(0);
        assertThat(ces1.getUrisCrawled()).isEqualTo(0);
        assertThat(ces1.getBytesCrawled()).isEqualTo(0);
        assertThat(ces1.getCurrentUriIdList()).isEmpty();
    }

    @Test
    public void updateCrawlExecutionStatus() throws DbException {
        JobExecutionStatus jes1 = executionsAdapter.createJobExecutionStatus("jobId1");
        CrawlExecutionStatus ces1 = frontier.createCrawlExecutionStatus("jobId1", jes1.getId(), "seed1");

        StatusWrapper.getStatusWrapper(frontier, ces1)
                .incrementDocumentsCrawled()
                .incrementDocumentsDenied(1)
                .incrementDocumentsFailed()
                .incrementDocumentsOutOfScope()
                .incrementDocumentsRetried()
                .incrementUrisCrawled(1)
                .incrementBytesCrawled(1)
                .saveStatus();
        CrawlExecutionStatus res = executionsAdapter.getCrawlExecutionStatus(ces1.getId());
        JobExecutionStatus jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.SLEEPING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isFalse();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(1);
        assertThat(res.getDocumentsDenied()).isEqualTo(1);
        assertThat(res.getDocumentsFailed()).isEqualTo(1);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(res.getDocumentsRetried()).isEqualTo(1);
        assertThat(res.getUrisCrawled()).isEqualTo(1);
        assertThat(res.getBytesCrawled()).isEqualTo(1);
        assertThat(res.getCurrentUriIdList()).isEmpty();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(1);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(1);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(1);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(1);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(1);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(1);

        QueuedUri qUri1 = QueuedUri.newBuilder()
                .setId("qUri1")
                .setUri("http://www.example.com")
                .addCookies(Cookie.getDefaultInstance())
                .build();
        StatusWrapper.getStatusWrapper(frontier, ces1)
                .addCurrentUri(qUri1)
                .saveStatus();
        res = executionsAdapter.getCrawlExecutionStatus(ces1.getId());
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FETCHING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(1);
        assertThat(res.getDocumentsDenied()).isEqualTo(1);
        assertThat(res.getDocumentsFailed()).isEqualTo(1);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(res.getDocumentsRetried()).isEqualTo(1);
        assertThat(res.getUrisCrawled()).isEqualTo(1);
        assertThat(res.getBytesCrawled()).isEqualTo(1);
        assertThat(res.getCurrentUriIdList()).containsExactly(qUri1.getId());
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(1);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(1);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(1);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(1);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(1);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(1);

        QueuedUri qUri2 = QueuedUri.newBuilder()
                .setId("qUri2")
                .setUri("http://www.example.com")
                .addCookies(Cookie.getDefaultInstance())
                .setExecutionId("foo")
                .build();
        StatusWrapper.getStatusWrapper(frontier, ces1)
                .addCurrentUri(qUri2)
                .saveStatus();
        res = executionsAdapter.getCrawlExecutionStatus(ces1.getId());
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FETCHING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(1);
        assertThat(res.getDocumentsDenied()).isEqualTo(1);
        assertThat(res.getDocumentsFailed()).isEqualTo(1);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(res.getDocumentsRetried()).isEqualTo(1);
        assertThat(res.getUrisCrawled()).isEqualTo(1);
        assertThat(res.getBytesCrawled()).isEqualTo(1);
        assertThat(res.getCurrentUriIdList()).containsExactly(qUri1.getId(), qUri2.getId());
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(1);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(1);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(1);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(1);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(1);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(1);

        StatusWrapper.getStatusWrapper(frontier, ces1)
                .removeCurrentUri(qUri1)
                .incrementDocumentsCrawled()
                .incrementDocumentsDenied(2)
                .incrementDocumentsFailed()
                .incrementDocumentsOutOfScope()
                .incrementDocumentsRetried()
                .incrementUrisCrawled(6)
                .incrementBytesCrawled(7)
                .saveStatus();
        res = executionsAdapter.getCrawlExecutionStatus(ces1.getId());
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FETCHING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(2);
        assertThat(res.getDocumentsDenied()).isEqualTo(3);
        assertThat(res.getDocumentsFailed()).isEqualTo(2);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(res.getDocumentsRetried()).isEqualTo(2);
        assertThat(res.getUrisCrawled()).isEqualTo(7);
        assertThat(res.getBytesCrawled()).isEqualTo(8);
        assertThat(res.getCurrentUriIdList()).containsExactly(qUri2.getId());
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(2);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(2);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        StatusWrapper.getStatusWrapper(frontier, ces1)
                .removeCurrentUri(qUri2)
                .saveStatus();
        res = executionsAdapter.getCrawlExecutionStatus(ces1.getId());
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.SLEEPING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(2);
        assertThat(res.getDocumentsDenied()).isEqualTo(3);
        assertThat(res.getDocumentsFailed()).isEqualTo(2);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(res.getDocumentsRetried()).isEqualTo(2);
        assertThat(res.getUrisCrawled()).isEqualTo(7);
        assertThat(res.getBytesCrawled()).isEqualTo(8);
        assertThat(res.getCurrentUriIdList()).isEmpty();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(2);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(2);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        assertThatThrownBy(() -> StatusWrapper.getStatusWrapper(frontier, ces1)
                .setState(CrawlExecutionStatus.State.CREATED)
                .saveStatus()).isInstanceOf(IllegalArgumentException.class);

        StatusWrapper.getStatusWrapper(frontier, ces1)
                .setEndState(CrawlExecutionStatus.State.FINISHED)
                .saveStatus();
        res = executionsAdapter.getCrawlExecutionStatus(ces1.getId());
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FINISHED);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isTrue();
        assertThat(res.getDocumentsCrawled()).isEqualTo(2);
        assertThat(res.getDocumentsDenied()).isEqualTo(3);
        assertThat(res.getDocumentsFailed()).isEqualTo(2);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(res.getDocumentsRetried()).isEqualTo(2);
        assertThat(res.getUrisCrawled()).isEqualTo(7);
        assertThat(res.getBytesCrawled()).isEqualTo(8);
        assertThat(res.getCurrentUriIdList()).isEmpty();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.FINISHED);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(1);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isTrue();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(2);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(2);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        Timestamp ts = res.getEndTime();
        StatusWrapper.getStatusWrapper(frontier, ces1)
                .setEndState(CrawlExecutionStatus.State.ABORTED_MANUAL)
                .saveStatus();
        res = executionsAdapter.getCrawlExecutionStatus(ces1.getId());
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FINISHED);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isTrue();
        assertThat(res.getEndTime()).isEqualTo(ts);
        assertThat(res.getDocumentsCrawled()).isEqualTo(2);
        assertThat(res.getDocumentsDenied()).isEqualTo(3);
        assertThat(res.getDocumentsFailed()).isEqualTo(2);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(res.getDocumentsRetried()).isEqualTo(2);
        assertThat(res.getUrisCrawled()).isEqualTo(7);
        assertThat(res.getBytesCrawled()).isEqualTo(8);
        assertThat(res.getCurrentUriIdList()).isEmpty();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.FINISHED);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(1);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isTrue();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(2);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(2);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        CrawlExecutionStatus ces2 = frontier.createCrawlExecutionStatus("jobId1", jes1.getId(), "seed1");
        StatusWrapper.getStatusWrapper(frontier, ces2)
                .setState(CrawlExecutionStatus.State.FINISHED)
                .saveStatus();
        res = executionsAdapter.getCrawlExecutionStatus(ces2.getId());
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces2.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FINISHED);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.FINISHED);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(1);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isTrue();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(2);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(2);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        ts = res.getEndTime();
        assertThat(ts).isEqualTo(Timestamp.getDefaultInstance());
        StatusWrapper.getStatusWrapper(frontier, ces2)
                .setEndState(CrawlExecutionStatus.State.ABORTED_MANUAL)
                .saveStatus();
        res = executionsAdapter.getCrawlExecutionStatus(ces2.getId());
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces2.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FINISHED);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasEndTime()).isTrue();
        assertThat(res.getEndTime()).isNotEqualTo(ts);
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.FINISHED);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(2);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isTrue();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(2);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(2);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(2);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);
    }

//    @Test
//    public void testPaused() throws DbException {
//        assertThat(executionsAdapter.getDesiredPausedState()).isFalse();
//        assertThat(executionsAdapter.isPaused()).isFalse();
//
//        assertThat(executionsAdapter.setDesiredPausedState(true)).isFalse();
//
//        assertThat(executionsAdapter.getDesiredPausedState()).isTrue();
//        assertThat(executionsAdapter.isPaused()).isTrue();
//
//        assertThat(executionsAdapter.setDesiredPausedState(true)).isTrue();
//        assertThat(executionsAdapter.isPaused()).isTrue();
//
//        assertThat(executionsAdapter.getDesiredPausedState()).isTrue();
//
//        assertThat(executionsAdapter.setDesiredPausedState(false)).isTrue();
//        assertThat(executionsAdapter.isPaused()).isFalse();
//
//        CrawlHostGroup chg = CrawlHostGroup.newBuilder().setId("chg").setBusy(true).build();
//        conn.exec("db-save",
//                r.table(Tables.CRAWL_HOST_GROUP.name).insert(ProtoUtils.protoToRethink(chg)).optArg("conflict", "replace"));
//        assertThat(executionsAdapter.getDesiredPausedState()).isFalse();
//        assertThat(executionsAdapter.isPaused()).isFalse();
//
//        assertThat(executionsAdapter.setDesiredPausedState(true)).isFalse();
//        assertThat(executionsAdapter.isPaused()).isFalse();
//
//        chg = chg.toBuilder().setBusy(false).build();
//        conn.exec("db-save",
//                r.table(Tables.CRAWL_HOST_GROUP.name).insert(ProtoUtils.protoToRethink(chg)).optArg("conflict", "replace"));
//
//        assertThat(executionsAdapter.getDesiredPausedState()).isTrue();
//        assertThat(executionsAdapter.isPaused()).isTrue();
//    }

}