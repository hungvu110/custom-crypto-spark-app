package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.deid.KeyDelivery.KeyHandle;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * D5: nghiệp vụ phía admin của flow phiếu khử định danh (plan mục 4.3): duyệt, từ chối, gửi lại. Key và job nằm sau hai giao
 * diện nên test dùng đối tượng giả đếm lời gọi, không cần Vault, Kubernetes hay Airflow.
 */
class DeidAdminServiceTest {

  private static final Instant DAY_1 = Instant.parse("2026-10-23T08:00:00Z");
  private static final Instant DAY_2 = Instant.parse("2026-10-24T08:00:00Z");

  /** Đồng hồ chỉnh được, để thử việc chốt tên bảng đích lúc duyệt chứ không tính lại. */
  private static final class MutableClock extends Clock {
    private volatile Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    void set(Instant instant) {
      this.now = instant;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }

  /** Giao key giả: ghi lại thứ tự lời gọi vào {@code events}, có thể được cấu hình để lỗi hoặc chậm. */
  private static final class FakeKeyDelivery implements KeyDelivery {
    private final List<String> events;
    volatile RuntimeException failure;
    volatile long delayMillis;
    volatile Long version = 3L;

    FakeKeyDelivery(List<String> events) {
      this.events = events;
    }

    @Override
    public KeyHandle deliver(DeidRequest request) {
      events.add("deliver");
      sleep(delayMillis);
      if (failure != null) {
        throw failure;
      }
      return new KeyHandle("deid-key-" + request.requestCode().substring(4), version);
    }

    @Override
    public void release(DeidRequest request) {
      events.add("release");
    }
  }

  private static final class FakeSubmitter implements JobSubmitter {
    private final List<String> events;
    final List<String> secretNames = new CopyOnWriteArrayList<>();
    volatile RuntimeException failure;

    FakeSubmitter(List<String> events) {
      this.events = events;
    }

    @Override
    public JobRef submit(DeidRequest request, String keySecretName) {
      events.add("submit");
      secretNames.add(keySecretName);
      if (failure != null) {
        throw failure;
      }
      return new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-" + request.requestCode().substring(4));
    }

    @Override
    public JobState status(DeidRequest request, JobRef ref) {
      return JobState.RUNNING;
    }
  }

  private final List<String> events = new CopyOnWriteArrayList<>();
  private MutableClock clock;
  private DeidRequestRegistry registry;
  private FakeKeyDelivery keyDelivery;
  private FakeSubmitter submitter;
  private DeidAdminService service;
  private ListAppender<ILoggingEvent> audit;
  private Logger auditLogger;

  @BeforeEach
  void setUp() {
    events.clear();
    clock = new MutableClock(DAY_1);
    registry = new DeidRequestRegistry(100, clock);
    keyDelivery = new FakeKeyDelivery(events);
    submitter = new FakeSubmitter(events);
    service = new DeidAdminService(DeidProperties.defaults(), registry, keyDelivery, submitter, clock);
    audit = new ListAppender<>();
    audit.start();
    auditLogger = (Logger) LoggerFactory.getLogger("AUDIT");
    auditLogger.setLevel(Level.INFO);
    auditLogger.addAppender(audit);
  }

  @AfterEach
  void tearDown() {
    auditLogger.detachAppender(audit);
  }

  private static void sleep(long millis) {
    if (millis > 0) {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private DeidRequest pending(String team, String table) {
    return registry.create(new NewRequest(team, "user-of-" + team, "demo_db", table,
        List.of(new ColumnSpec("msisdn", Mode.HASH), new ColumnSpec("imei", Mode.HASH)), null, "ly do rieng tu",
        new SourceFile("phieu.json", "ab".repeat(32))));
  }

  private DeidRequest pending(String table) {
    return pending("team-a", table);
  }

  private void cancel(DeidRequest r) {
    registry.transitionForTeam(r.team(), r.requestCode(), Set.of(State.PENDING_APPROVAL), DeidRequest::cancelled);
  }

  private List<String> auditLines() {
    return audit.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
  }

  private static void assertApiError(Throwable thrown, HttpStatus status, String error) {
    assertThat(thrown).isInstanceOfSatisfying(DakApiException.class, e -> {
      assertThat(e.status()).isEqualTo(status);
      assertThat(e.error()).isEqualTo(error);
    });
  }

  @Nested
  @DisplayName("duyệt")
  class Approve {

    @Test
    @DisplayName("duyệt thành công: lấy key rồi gửi job theo đúng thứ tự, phiếu thành JOB_SUBMITTED với jobRef và keyVersion")
    void approve_deliversKeyThenSubmits() {
      DeidRequest created = pending("users_cdr");

      DeidRequest result = service.approve(created.requestCode());

      assertThat(events).containsExactly("deliver", "submit");
      assertThat(submitter.secretNames).containsExactly("deid-key-2026-000001");
      assertThat(result.state()).isEqualTo(State.JOB_SUBMITTED);
      assertThat(result.decidedBy()).isEqualTo("admin");
      assertThat(result.decidedAt()).isEqualTo(DAY_1);
      assertThat(result.jobRef()).isEqualTo(new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-2026-000001"));
      assertThat(result.keyVersion()).isEqualTo(3L);
      assertThat(result.submitError()).isNull();
      assertThat(registry.getByCode(created.requestCode())).isEqualTo(result);
    }

    @Test
    @DisplayName("bảng đích: cùng database nguồn theo mặc định, tên theo mẫu có ngày duyệt và số cuối của mã phiếu")
    void approve_choosesTheOutputTable() {
      DeidRequest created = pending("users_cdr");

      DeidRequest result = service.approve(created.requestCode());

      assertThat(result.outputDatabase()).isEqualTo("demo_db");
      assertThat(result.outputTable()).isEqualTo("users_cdr_deid_20261023_1");
      assertThat(result.outputTable()).isNotEqualTo(result.table());
    }

    @Test
    @DisplayName("output-database cấu hình thì dùng database đó; mẫu tên tuỳ chỉnh được áp dụng")
    void approve_honoursConfiguredOutputDatabaseAndTemplate() {
      DeidProperties deid = new DeidProperties(true, null, 20, 65536, "analytics", "{table}_x_{seq}", null, null, null, null);
      DeidAdminService custom = new DeidAdminService(deid, registry, keyDelivery, submitter, clock);
      pending("a_table");
      DeidRequest second = pending("users_cdr");

      DeidRequest result = custom.approve(second.requestCode());

      assertThat(result.outputDatabase()).isEqualTo("analytics");
      assertThat(result.outputTable()).isEqualTo("users_cdr_x_2");
    }

    @Test
    @DisplayName("{seq} bỏ số 0 đứng đầu của mã phiếu")
    void approve_seqHasNoLeadingZeros() {
      DeidRequest created = null;
      for (int i = 0; i < 12; i++) {
        created = pending("t" + i);
      }

      assertThat(service.approve(created.requestCode()).outputTable()).isEqualTo("t11_deid_20261023_12");
    }

    @Test
    @DisplayName("duyệt lặp lại phiếu đã duyệt trả lại phiếu hiện tại, không giao key và không gửi job lần hai")
    void approve_isIdempotent() {
      DeidRequest created = pending("users_cdr");
      DeidRequest first = service.approve(created.requestCode());

      DeidRequest again = service.approve(created.requestCode());

      assertThat(again).isEqualTo(first);
      assertThat(events).containsExactly("deliver", "submit");
    }

    @Test
    @DisplayName("duyệt phiếu đã qua duyệt ở bất kỳ trạng thái nào (SUBMIT_FAILED, đang chạy, xong) đều trả lại phiếu, không làm gì thêm")
    void approve_afterAnyLaterState_returnsTheCurrentRequest() {
      keyDelivery.failure = new KeyDeliveryException("table key not found");
      DeidRequest failed = service.approve(pending("t1").requestCode());
      keyDelivery.failure = null;
      DeidRequest running = service.approve(pending("t2").requestCode());
      registry.transition(running.requestCode(), Set.of(State.JOB_SUBMITTED), r -> r.jobProgress(JobState.RUNNING, DAY_1));
      DeidRequest done = service.approve(pending("t3").requestCode());
      registry.transition(done.requestCode(), Set.of(State.JOB_SUBMITTED), r -> r.jobProgress(JobState.SUCCEEDED, DAY_1));
      events.clear();

      assertThat(service.approve(failed.requestCode()).state()).isEqualTo(State.SUBMIT_FAILED);
      assertThat(service.approve(running.requestCode()).state()).isEqualTo(State.RUNNING);
      assertThat(service.approve(done.requestCode()).state()).isEqualTo(State.SUCCEEDED);
      assertThat(events).isEmpty();
    }

    @Test
    @DisplayName("duyệt phiếu đã huỷ hoặc đã từ chối bị 409, không giao key, không gửi job")
    void approve_cancelledOrRejected_isRejected() {
      DeidRequest cancelled = pending("t1");
      cancel(cancelled);
      DeidRequest rejected = pending("t2");
      service.reject(rejected.requestCode(), "khong");

      assertApiError(catchThrowable(() -> service.approve(cancelled.requestCode())), HttpStatus.CONFLICT, "invalid_state");
      assertApiError(catchThrowable(() -> service.approve(rejected.requestCode())), HttpStatus.CONFLICT, "invalid_state");
      assertThat(events).isEmpty();
      assertThat(registry.getByCode(cancelled.requestCode()).state()).isEqualTo(State.CANCELLED);
    }

    @Test
    @DisplayName("mã không tồn tại hoặc sai định dạng thì 404")
    void approve_unknownOrMalformed_isNotFound() {
      for (String bad : List.of("PYC-2026-999999", "", "x", "pyc-2026-000001", "PYC-2026-000001\n")) {
        assertApiError(catchThrowable(() -> service.approve(bad)), HttpStatus.NOT_FOUND, "not_found");
      }
      assertThat(events).isEmpty();
    }

    @Test
    @DisplayName("giao key lỗi: phiếu vào SUBMIT_FAILED kèm thông điệp cố định, không gửi job, không có gì để thu hồi")
    void approve_keyDeliveryFails() {
      keyDelivery.failure = new KeyDeliveryException("table key not found");
      DeidRequest created = pending("users_cdr");

      DeidRequest result = service.approve(created.requestCode());

      assertThat(result.state()).isEqualTo(State.SUBMIT_FAILED);
      assertThat(result.submitError()).isEqualTo("table key not found");
      assertThat(result.outputTable()).isEqualTo("users_cdr_deid_20261023_1");
      assertThat(events).containsExactly("deliver");
    }

    @Test
    @DisplayName("gửi job lỗi: thu hồi key đã giao rồi vào SUBMIT_FAILED kèm thông điệp cố định")
    void approve_submitFails_releasesTheKey() {
      submitter.failure = new JobSubmitException("kubernetes unavailable");
      DeidRequest created = pending("users_cdr");

      DeidRequest result = service.approve(created.requestCode());

      assertThat(result.state()).isEqualTo(State.SUBMIT_FAILED);
      assertThat(result.submitError()).isEqualTo("kubernetes unavailable");
      assertThat(events).containsExactly("deliver", "submit", "release");
    }

    @Test
    @DisplayName("lỗi bất ngờ không được lộ vào phiếu: thông điệp chung, không chứa nội dung exception")
    void approve_unexpectedFailure_isGeneric() {
      keyDelivery.failure = new IllegalStateException("boom with SECRET-VALUE inside");
      DeidRequest first = service.approve(pending("t1").requestCode());
      keyDelivery.failure = null;
      submitter.failure = new IllegalArgumentException("kaboom SECRET-VALUE");
      DeidRequest second = service.approve(pending("t2").requestCode());

      assertThat(first.state()).isEqualTo(State.SUBMIT_FAILED);
      assertThat(first.submitError()).isEqualTo("internal error while delivering the key");
      assertThat(second.state()).isEqualTo(State.SUBMIT_FAILED);
      assertThat(second.submitError()).isEqualTo("internal error while submitting the job");
      assertThat(first.submitError() + second.submitError()).doesNotContain("SECRET");
    }

    @Test
    @DisplayName("noop không có keyVersion: phiếu không hiện keyVersion")
    void approve_withoutKeyVersion_leavesItAbsent() {
      keyDelivery.version = null;

      DeidRequest result = service.approve(pending("users_cdr").requestCode());

      assertThat(result.state()).isEqualTo(State.JOB_SUBMITTED);
      assertThat(result.keyVersion()).isNull();
    }
  }

  @Nested
  @DisplayName("gửi lại")
  class RetrySubmit {

    @Test
    @DisplayName("sau SUBMIT_FAILED: gửi lại lấy key và gửi job lần nữa, xoá lỗi cũ, giữ nguyên bảng đích đã chốt")
    void retry_afterFailure_succeeds_andKeepsTheOutputTable() {
      keyDelivery.failure = new KeyDeliveryException("table key not found");
      DeidRequest failed = service.approve(pending("users_cdr").requestCode());
      keyDelivery.failure = null;
      clock.set(DAY_2);

      DeidRequest result = service.retrySubmit(failed.requestCode());

      assertThat(result.state()).isEqualTo(State.JOB_SUBMITTED);
      assertThat(result.submitError()).isNull();
      assertThat(result.outputTable()).isEqualTo("users_cdr_deid_20261023_1");
      assertThat(result.decidedAt()).isEqualTo(DAY_1);
      assertThat(events).containsExactly("deliver", "deliver", "submit");
    }

    @Test
    @DisplayName("gửi lại mà vẫn lỗi thì ở lại SUBMIT_FAILED với lỗi mới, và có thể gửi lại lần nữa")
    void retry_failingAgain_staysFailed() {
      keyDelivery.failure = new KeyDeliveryException("table key not found");
      DeidRequest failed = service.approve(pending("users_cdr").requestCode());
      keyDelivery.failure = new KeyDeliveryException("table key has unsupported format");

      DeidRequest again = service.retrySubmit(failed.requestCode());
      keyDelivery.failure = null;
      DeidRequest finallyOk = service.retrySubmit(failed.requestCode());

      assertThat(again.state()).isEqualTo(State.SUBMIT_FAILED);
      assertThat(again.submitError()).isEqualTo("table key has unsupported format");
      assertThat(finallyOk.state()).isEqualTo(State.JOB_SUBMITTED);
    }

    @Test
    @DisplayName("chỉ gửi lại được phiếu SUBMIT_FAILED; trạng thái khác thì 409 và không làm gì")
    void retry_inOtherStates_isRejected() {
      DeidRequest pending = pending("t1");
      DeidRequest submitted = service.approve(pending("t2").requestCode());
      DeidRequest cancelled = pending("t3");
      cancel(cancelled);
      events.clear();

      for (String code : List.of(pending.requestCode(), submitted.requestCode(), cancelled.requestCode())) {
        assertApiError(catchThrowable(() -> service.retrySubmit(code)), HttpStatus.CONFLICT, "invalid_state");
      }
      assertThat(events).isEmpty();
    }

    @Test
    @DisplayName("mã không tồn tại hoặc sai định dạng thì 404")
    void retry_unknownOrMalformed_isNotFound() {
      assertApiError(catchThrowable(() -> service.retrySubmit("PYC-2026-999999")), HttpStatus.NOT_FOUND, "not_found");
      assertApiError(catchThrowable(() -> service.retrySubmit("nope")), HttpStatus.NOT_FOUND, "not_found");
    }
  }

  @Nested
  @DisplayName("từ chối")
  class Reject {

    @Test
    @DisplayName("từ chối ghi lý do đã strip, người từ chối và giờ; không động tới key hay job")
    void reject_recordsTheReason() {
      DeidRequest created = pending("users_cdr");

      DeidRequest result = service.reject(created.requestCode(), "  thieu can cu phap ly  ");

      assertThat(result.state()).isEqualTo(State.REJECTED);
      assertThat(result.reasonText()).isEqualTo("thieu can cu phap ly");
      assertThat(result.decidedBy()).isEqualTo("admin");
      assertThat(result.decidedAt()).isEqualTo(DAY_1);
      assertThat(result.outputTable()).isNull();
      assertThat(events).isEmpty();
    }

    @Test
    @DisplayName("lý do bắt buộc: thiếu, rỗng, chỉ khoảng trắng hoặc quá 500 ký tự đều 400 invalid_request và phiếu không đổi")
    void reject_requiresAReason() {
      DeidRequest created = pending("users_cdr");

      for (String bad : new String[] {null, "", "   ", "x".repeat(501)}) {
        assertApiError(catchThrowable(() -> service.reject(created.requestCode(), bad)), HttpStatus.BAD_REQUEST,
            "invalid_request");
      }
      assertThat(registry.getByCode(created.requestCode()).state()).isEqualTo(State.PENDING_APPROVAL);
      assertThat(service.reject(created.requestCode(), "x".repeat(500)).reasonText()).hasSize(500);
    }

    @Test
    @DisplayName("chỉ từ chối được phiếu đang chờ duyệt: đã duyệt, đã huỷ, đã từ chối đều 409")
    void reject_inOtherStates_isRejected() {
      DeidRequest approved = service.approve(pending("t1").requestCode());
      DeidRequest cancelled = pending("t2");
      cancel(cancelled);
      DeidRequest rejected = pending("t3");
      service.reject(rejected.requestCode(), "lan dau");

      for (String code : List.of(approved.requestCode(), cancelled.requestCode(), rejected.requestCode())) {
        assertApiError(catchThrowable(() -> service.reject(code, "lan hai")), HttpStatus.CONFLICT, "invalid_state");
      }
      assertThat(registry.getByCode(rejected.requestCode()).reasonText()).isEqualTo("lan dau");
    }

    @Test
    @DisplayName("mã không tồn tại hoặc sai định dạng thì 404")
    void reject_unknownOrMalformed_isNotFound() {
      assertApiError(catchThrowable(() -> service.reject("PYC-2026-999999", "x")), HttpStatus.NOT_FOUND, "not_found");
      assertApiError(catchThrowable(() -> service.reject("nope", "x")), HttpStatus.NOT_FOUND, "not_found");
    }
  }

  @Nested
  @DisplayName("xem và liệt kê (admin thấy mọi team)")
  class Viewing {

    @Test
    @DisplayName("get theo mã không phân biệt team; mã lạ hoặc sai định dạng 404")
    void get_ignoresTeam() {
      DeidRequest created = pending("team-b", "users_cdr");

      assertThat(service.get(created.requestCode())).isEqualTo(created);
      assertApiError(catchThrowable(() -> service.get("PYC-2026-999999")), HttpStatus.NOT_FOUND, "not_found");
      assertApiError(catchThrowable(() -> service.get("../x")), HttpStatus.NOT_FOUND, "not_found");
    }

    @Test
    @DisplayName("danh sách gồm phiếu của mọi team, mới nhất trước, lọc theo state, phân trang bằng cursor")
    void list_acrossTeams() {
      DeidRequest a = pending("team-a", "t1");
      DeidRequest b = pending("team-b", "t2");
      DeidRequest c = pending("team-a", "t3");
      service.reject(b.requestCode(), "khong");

      DeidRequestService.Page all = service.list(null, null, null);
      DeidRequestService.Page rejected = service.list("rejected", null, null);
      DeidRequestService.Page page1 = service.list(null, "2", null);
      DeidRequestService.Page page2 = service.list(null, "2", page1.nextCursor());

      assertThat(all.requests()).extracting(DeidRequest::requestCode)
          .containsExactly(c.requestCode(), b.requestCode(), a.requestCode());
      assertThat(rejected.requests()).extracting(DeidRequest::requestCode).containsExactly(b.requestCode());
      assertThat(page1.requests()).extracting(DeidRequest::requestCode).containsExactly(c.requestCode(), b.requestCode());
      assertThat(page1.nextCursor()).isEqualTo(b.requestCode());
      assertThat(page2.requests()).extracting(DeidRequest::requestCode).containsExactly(a.requestCode());
      assertThat(page2.nextCursor()).isNull();
    }

    @Test
    @DisplayName("tham số danh sách sai bị 400 invalid_request")
    void list_invalidParameters() {
      for (Callable<?> bad : List.<Callable<?>>of(() -> service.list("BOGUS", null, null), () -> service.list(null, "0", null),
          () -> service.list(null, "101", null), () -> service.list(null, "x", null),
          () -> service.list(null, null, "nope"), () -> service.list(null, null, "PYC-2026-999999"))) {
        assertApiError(catchThrowable(bad::call), HttpStatus.BAD_REQUEST, "invalid_request");
      }
    }
  }

  @Nested
  @DisplayName("đồng thời")
  class Concurrency {

    @Test
    @DisplayName("nhiều lời gọi duyệt cùng lúc: key được giao đúng một lần, job được gửi đúng một lần, không lời gọi nào lỗi")
    void concurrentApprovals_deliverAndSubmitOnce() throws Exception {
      keyDelivery.delayMillis = 60;
      DeidRequest created = pending("users_cdr");
      List<Callable<DeidRequest>> tasks = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        tasks.add(() -> service.approve(created.requestCode()));
      }

      List<DeidRequest> results = runConcurrently(tasks);

      assertThat(events).containsExactly("deliver", "submit");
      assertThat(results).hasSize(8).allSatisfy(r -> assertThat(r.state()).isIn(State.APPROVED, State.JOB_SUBMITTED));
      assertThat(registry.getByCode(created.requestCode()).state()).isEqualTo(State.JOB_SUBMITTED);
    }

    @Test
    @DisplayName("duyệt và huỷ cùng lúc: đúng một bên thắng; huỷ thắng thì không giao key, duyệt thắng thì phiếu không thể thành CANCELLED")
    void approveVersusCancel_exactlyOneWins() throws Exception {
      int rounds = 40;
      for (int round = 0; round < rounds; round++) {
        events.clear();
        DeidRequest created = pending("team-a", "race" + round);
        List<Callable<DeidRequest>> tasks = List.of(
            () -> swallowConflict(() -> service.approve(created.requestCode())),
            () -> swallowConflict(() -> registry.transitionForTeam("team-a", created.requestCode(),
                Set.of(State.PENDING_APPROVAL), DeidRequest::cancelled)));

        runConcurrently(tasks);

        State finalState = registry.getByCode(created.requestCode()).state();
        if (finalState == State.CANCELLED) {
          assertThat(events).as("cancel won, so no key and no job").isEmpty();
        } else {
          assertThat(finalState).isEqualTo(State.JOB_SUBMITTED);
          assertThat(events).containsExactly("deliver", "submit");
        }
      }
    }
  }

  @Nested
  @DisplayName("audit (API_CONTRACT mục 9)")
  class Audit {

    @Test
    @DisplayName("duyệt ghi: PENDING_APPROVAL→APPROVED, KEY_DELIVERY, APPROVED→JOB_SUBMITTED; actor=admin; không có lý do hay tên Secret")
    void approve_isAudited() {
      DeidRequest created = pending("users_cdr");
      audit.list.clear();

      service.approve(created.requestCode());

      List<String> lines = auditLines();
      assertThat(lines).hasSize(3);
      assertThat(lines.get(0)).startsWith("DEID_TRANSITION").contains("from=PENDING_APPROVAL").contains("to=APPROVED")
          .contains("actor=admin").contains("team=team-a").contains("table=users_cdr").contains("columns=msisdn,imei")
          .contains("result=OK");
      assertThat(lines.get(1)).startsWith("KEY_DELIVERY").contains("requestCode=" + created.requestCode())
          .contains("keyVersion=3").contains("result=OK");
      assertThat(lines.get(2)).startsWith("DEID_TRANSITION").contains("from=APPROVED").contains("to=JOB_SUBMITTED");
      for (String line : lines) {
        assertThat(line).doesNotContain("deid-key-").doesNotContain("ly do rieng tu");
      }
    }

    @Test
    @DisplayName("giao key lỗi ghi KEY_DELIVERY result=ERROR và chuyển APPROVED→SUBMIT_FAILED kèm submitError cố định")
    void keyFailure_isAudited() {
      keyDelivery.failure = new KeyDeliveryException("table key not found");
      DeidRequest created = pending("users_cdr");
      audit.list.clear();

      service.approve(created.requestCode());

      List<String> lines = auditLines();
      assertThat(lines.stream().anyMatch(l -> l.startsWith("KEY_DELIVERY") && l.contains("result=ERROR"))).isTrue();
      assertThat(lines.stream().anyMatch(l -> l.contains("to=SUBMIT_FAILED") && l.contains("submitError=table key not found")))
          .isTrue();
    }

    @Test
    @DisplayName("từ chối ghi chuyển PENDING_APPROVAL→REJECTED và KHÔNG ghi lý do (văn bản tự do)")
    void reject_isAudited_withoutTheReason() {
      DeidRequest created = pending("users_cdr");
      audit.list.clear();

      service.reject(created.requestCode(), "ly do cong khai khong duoc vao audit");

      assertThat(auditLines()).hasSize(1);
      assertThat(auditLines().get(0)).contains("to=REJECTED").contains("actor=admin")
          .doesNotContain("ly do cong khai");
    }

    @Test
    @DisplayName("thao tác bị từ chối ghi result=ERROR kèm mã lỗi; duyệt lặp lại thành công không ghi thêm")
    void rejections_areAudited_idempotentRepeatIsNot() {
      DeidRequest created = pending("users_cdr");
      service.approve(created.requestCode());
      audit.list.clear();

      service.approve(created.requestCode());
      catchThrowable(() -> service.reject(created.requestCode(), "x"));
      catchThrowable(() -> service.approve("PYC-2026-999999"));

      assertThat(auditLines()).hasSize(2);
      assertThat(auditLines().get(0)).contains("action=reject").contains("result=ERROR").contains("error=invalid_state")
          .contains("actor=admin");
      assertThat(auditLines().get(1)).contains("action=approve").contains("error=not_found");
    }
  }

  /** Chạy thao tác; thua cuộc đua (409) thì trả null thay vì ném, để thử cuộc đua mà không làm hỏng test. */
  private static DeidRequest swallowConflict(Supplier<DeidRequest> action) {
    try {
      return action.get();
    } catch (DakApiException e) {
      assertThat(e.status()).isEqualTo(HttpStatus.CONFLICT);
      return null;
    }
  }

  private static <T> List<T> runConcurrently(List<Callable<T>> tasks) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(Math.max(2, tasks.size()));
    try {
      CountDownLatch ready = new CountDownLatch(tasks.size());
      CountDownLatch go = new CountDownLatch(1);
      List<Future<T>> futures = new ArrayList<>();
      for (Callable<T> task : tasks) {
        futures.add(pool.submit(() -> {
          ready.countDown();
          go.await();
          return task.call();
        }));
      }
      assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
      go.countDown();
      List<T> results = new ArrayList<>();
      for (Future<T> future : futures) {
        results.add(future.get(20, TimeUnit.SECONDS));
      }
      return results;
    } finally {
      pool.shutdownNow();
    }
  }
}
