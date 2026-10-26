package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.deid.KeyDelivery.KeyHandle;

/**
 * D9 (plan mục 4.4): không có tiến trình nền; xem phiếu đang chạy thì hỏi trạng thái job, ghi vào phiếu, và khi job xong thì thu hồi
 * (xoá) Secret key đúng một lần. Lỗi hỏi trạng thái hay thu hồi không bao giờ làm hỏng việc xem phiếu.
 */
class JobProgressRefresherTest {

  private static final Instant NOW = Instant.parse("2026-10-23T10:00:00Z");

  private final List<String> events = new CopyOnWriteArrayList<>();
  private DeidRequestRegistry registry;
  private volatile JobState reported = JobState.RUNNING;
  private volatile RuntimeException statusFailure;
  private volatile RuntimeException releaseFailure;
  private JobProgressRefresher refresher;

  @BeforeEach
  void setUp() {
    Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
    registry = new DeidRequestRegistry(100, clock);
    JobSubmitter submitter = new JobSubmitter() {
      @Override
      public JobRef submit(DeidRequest request, String keySecretName) {
        throw new AssertionError("refresh must never submit");
      }

      @Override
      public JobState status(DeidRequest request, JobRef ref) {
        events.add("status");
        if (statusFailure != null) {
          throw statusFailure;
        }
        return reported;
      }
    };
    KeyDelivery keyDelivery = new KeyDelivery() {
      @Override
      public KeyHandle deliver(DeidRequest request) {
        throw new AssertionError("refresh must never deliver");
      }

      @Override
      public void release(DeidRequest request) {
        events.add("release");
        if (releaseFailure != null) {
          throw releaseFailure;
        }
      }
    };
    refresher = new JobProgressRefresher(registry, submitter, keyDelivery, clock);
  }

  private DeidRequest submitted() {
    DeidRequest created = registry.create(new NewRequest("team-a", "alice", "demo_db", "users_cdr",
        List.of(new ColumnSpec("msisdn", Mode.HASH)), "deid_dag", "purpose", new SourceFile("f.json", "ab".repeat(32))));
    String code = created.requestCode();
    registry.transition(code, Set.of(State.PENDING_APPROVAL), r -> r.approved("admin", NOW, "demo_db", "t_deid_1"));
    return registry.transition(code, Set.of(State.APPROVED),
        r -> r.submitted(new JobRef(JobRef.Kind.AIRFLOW_DAG_RUN, r.jobName()), 3L));
  }

  @Test
  @DisplayName("refresh moves JOB_SUBMITTED to RUNNING and does not release the key yet")
  void refresh_running_updatesState() {
    DeidRequest r = submitted();

    DeidRequest out = refresher.refresh(r);

    assertThat(out.state()).isEqualTo(State.RUNNING);
    assertThat(out.jobState()).isEqualTo(JobState.RUNNING);
    assertThat(registry.getByCode(r.requestCode()).state()).isEqualTo(State.RUNNING);
    assertThat(events).containsExactly("status");
  }

  @Test
  @DisplayName("refresh records SUCCEEDED with the finish time and releases the key exactly once")
  void refresh_succeeded_releasesOnce() {
    DeidRequest r = submitted();
    reported = JobState.SUCCEEDED;

    DeidRequest out = refresher.refresh(r);
    DeidRequest again = refresher.refresh(out);

    assertThat(out.state()).isEqualTo(State.SUCCEEDED);
    assertThat(out.jobFinishedAt()).isEqualTo(NOW);
    assertThat(again.state()).isEqualTo(State.SUCCEEDED);
    assertThat(events).containsExactly("status", "release");
  }

  @Test
  @DisplayName("refresh records FAILED and releases the key")
  void refresh_failed_releases() {
    DeidRequest r = submitted();
    reported = JobState.FAILED;

    assertThat(refresher.refresh(r).state()).isEqualTo(State.FAILED);
    assertThat(events).containsExactly("status", "release");
  }

  @Test
  @DisplayName("refresh of a job Airflow no longer knows sets jobState UNKNOWN, keeps the state and releases the key")
  void refresh_unknown_keepsStateAndReleases() {
    DeidRequest r = submitted();
    reported = JobState.UNKNOWN;

    DeidRequest out = refresher.refresh(r);
    DeidRequest again = refresher.refresh(out);

    assertThat(out.state()).isEqualTo(State.JOB_SUBMITTED);
    assertThat(out.jobState()).isEqualTo(JobState.UNKNOWN);
    assertThat(again.jobState()).isEqualTo(JobState.UNKNOWN);
    assertThat(events).containsExactly("status", "release");
  }

  @Test
  @DisplayName("refresh keeps the state and does not release when asking for the status fails")
  void refresh_statusFails_keepsState() {
    DeidRequest r = submitted();
    statusFailure = new JobSubmitException("airflow unavailable");

    DeidRequest out = refresher.refresh(r);

    assertThat(out.state()).isEqualTo(State.JOB_SUBMITTED);
    assertThat(events).containsExactly("status");
  }

  @Test
  @DisplayName("refresh still records the final state when releasing the key fails")
  void refresh_releaseFails_stateStillRecorded() {
    DeidRequest r = submitted();
    reported = JobState.SUCCEEDED;
    releaseFailure = new KeyDeliveryException("kubernetes unavailable");

    DeidRequest out = refresher.refresh(r);

    assertThat(out.state()).isEqualTo(State.SUCCEEDED);
    assertThat(registry.getByCode(r.requestCode()).state()).isEqualTo(State.SUCCEEDED);
  }

  @Test
  @DisplayName("refresh does not ask about requests that are not running a job")
  void refresh_otherStates_noCalls() {
    DeidRequest pending = registry.create(new NewRequest("team-a", "alice", "demo_db", "t2",
        List.of(new ColumnSpec("msisdn", Mode.HASH)), "deid_dag", "purpose", new SourceFile("f.json", "cd".repeat(32))));

    assertThat(refresher.refresh(pending)).isSameAs(pending);
    assertThat(events).isEmpty();
  }

  @Test
  @DisplayName("a disabled refresher returns the request untouched")
  void disabled_returnsSame() {
    DeidRequest r = submitted();

    assertThat(JobProgressRefresher.disabled().refresh(r)).isSameAs(r);
  }

  @Test
  @DisplayName("when another view already recorded the final state, this one neither overwrites it nor releases again")
  void refresh_lostRace_noSecondRelease() {
    DeidRequest r = submitted();
    registry.transition(r.requestCode(), Set.of(State.JOB_SUBMITTED), x -> x.jobProgress(JobState.SUCCEEDED, NOW));
    reported = JobState.FAILED;

    DeidRequest out = refresher.refresh(r);

    assertThat(out.state()).isEqualTo(State.SUCCEEDED);
    assertThat(events).doesNotContain("release");
  }
}
