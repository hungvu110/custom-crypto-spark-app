package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * D2 của flow phiếu khử định danh: registry trong bộ nhớ, thread-safe. Kiểm mã phiếu, tra cứu theo team, giới hạn phiếu
 * đang hoạt động, chống trùng và chuyển trạng thái nguyên tử (compare-and-set), theo plan mục 4.2.
 */
class DeidRequestRegistryTest {

  private static final Instant NOW = Instant.parse("2026-10-23T08:00:00Z");
  private static final JobRef JOB = new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-2026-000001");

  private static DeidRequestRegistry registry(int maxActivePerTeam) {
    return new DeidRequestRegistry(maxActivePerTeam, Clock.fixed(NOW, ZoneOffset.UTC));
  }

  private static NewRequest request(String team, String table, String... columns) {
    List<ColumnSpec> specs = new ArrayList<>();
    for (String column : columns) {
      specs.add(new ColumnSpec(column, Mode.HASH));
    }
    return new NewRequest(team, "user-of-" + team, "demo_db", table, specs, "deid_dag", "thu nghiem",
        new SourceFile("phieu.json", "ab".repeat(32)));
  }

  private static NewRequest request(String team, String table) {
    return request(team, table, "msisdn", "imei");
  }

  private static void assertApiError(Throwable thrown, HttpStatus status, String error) {
    assertThat(thrown).isInstanceOfSatisfying(DakApiException.class, e -> {
      assertThat(e.status()).isEqualTo(status);
      assertThat(e.error()).isEqualTo(error);
    });
  }

  private static DeidRequest approve(DeidRequestRegistry registry, String code) {
    return registry.transition(code, Set.of(State.PENDING_APPROVAL),
        r -> r.approved("admin", NOW, "demo_db", "out_" + code.toLowerCase().replace('-', '_')));
  }

  @Nested
  @DisplayName("tạo phiếu và tra cứu")
  class CreateAndLookup {

    @Test
    @DisplayName("mã phiếu PYC-<năm>-<6 số> tăng dần theo đồng hồ, phiếu mới ở PENDING_APPROVAL")
    void create_assignsSequentialCodes_andStartsPending() {
      DeidRequestRegistry registry = registry(20);

      DeidRequest first = registry.create(request("team-a", "t1"));
      DeidRequest second = registry.create(request("team-a", "t2"));

      assertThat(first.requestCode()).isEqualTo("PYC-2026-000001");
      assertThat(second.requestCode()).isEqualTo("PYC-2026-000002");
      assertThat(first.state()).isEqualTo(State.PENDING_APPROVAL);
      assertThat(first.createdAt()).isEqualTo(NOW);
      assertThat(first.team()).isEqualTo("team-a");
      assertThat(first.createdBy()).isEqualTo("user-of-team-a");
    }

    @Test
    @DisplayName("tra cứu theo (team, mã): đúng team thấy phiếu, team khác không")
    void find_isScopedByTeam() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));

      assertThat(registry.find("team-a", created.requestCode())).contains(created);
      assertThat(registry.find("team-b", created.requestCode())).isEmpty();
      assertThat(registry.find("team-a", "PYC-2026-999999")).isEmpty();
      assertThat(registry.get("team-a", created.requestCode())).isEqualTo(created);
    }

    @Test
    @DisplayName("phiếu của team khác và phiếu không tồn tại trả đúng cùng một lỗi (không lộ phiếu có tồn tại)")
    void get_foreignAndUnknownAreIndistinguishable() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));

      Throwable foreign = catchThrowable(() -> registry.get("team-b", created.requestCode()));
      Throwable unknown = catchThrowable(() -> registry.get("team-b", "PYC-2026-999999"));

      assertApiError(foreign, HttpStatus.NOT_FOUND, "not_found");
      assertApiError(unknown, HttpStatus.NOT_FOUND, "not_found");
      assertThat(foreign.getMessage()).isEqualTo(unknown.getMessage());
      assertThat(((DakApiException) foreign).internalReason()).isEqualTo(((DakApiException) unknown).internalReason());
    }

    @Test
    @DisplayName("tra cứu theo mã không phân biệt team (cho admin)")
    void findByCode_ignoresTeam() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));

      assertThat(registry.findByCode(created.requestCode())).contains(created);
      assertThat(registry.getByCode(created.requestCode())).isEqualTo(created);
      assertThat(registry.findByCode("PYC-2026-999999")).isEmpty();
      assertApiError(catchThrowable(() -> registry.getByCode("PYC-2026-999999")), HttpStatus.NOT_FOUND, "not_found");
    }

    @Test
    @DisplayName("danh sách của team: mới nhất trước và chỉ gồm phiếu của team đó")
    void listForTeam_newestFirst_onlyOwnTeam() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest a1 = registry.create(request("team-a", "t1"));
      registry.create(request("team-b", "t1"));
      DeidRequest a2 = registry.create(request("team-a", "t2"));

      assertThat(registry.listForTeam("team-a")).containsExactly(a2, a1);
      assertThat(registry.listForTeam("team-c")).isEmpty();
    }

    @Test
    @DisplayName("danh sách tất cả phiếu: mới nhất trước và không sửa được từ bên ngoài")
    void listAll_newestFirst_andImmutable() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest a = registry.create(request("team-a", "t1"));
      DeidRequest b = registry.create(request("team-b", "t1"));

      List<DeidRequest> all = registry.listAll();

      assertThat(all).containsExactly(b, a);
      assertThatThrownBy(() -> all.add(a)).isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Nested
  @DisplayName("giới hạn phiếu đang hoạt động và chống trùng")
  class LimitsAndDuplicates {

    @Test
    @DisplayName("vượt giới hạn phiếu đang hoạt động của team thì 429, team khác không bị ảnh hưởng")
    void create_overActiveLimit_isRejected_perTeam() {
      DeidRequestRegistry registry = registry(2);
      registry.create(request("team-a", "t1"));
      registry.create(request("team-a", "t2"));

      assertApiError(catchThrowable(() -> registry.create(request("team-a", "t3"))),
          HttpStatus.TOO_MANY_REQUESTS, "too_many_active_requests");
      assertThat(registry.create(request("team-b", "t3")).team()).isEqualTo("team-b");
      assertThat(registry.listForTeam("team-a")).hasSize(2);
    }

    @Test
    @DisplayName("huỷ hoặc từ chối phiếu giải phóng một suất")
    void finishedRequests_freeTheSlot() {
      DeidRequestRegistry registry = registry(2);
      DeidRequest first = registry.create(request("team-a", "t1"));
      DeidRequest second = registry.create(request("team-a", "t2"));

      registry.transitionForTeam("team-a", first.requestCode(), Set.of(State.PENDING_APPROVAL), DeidRequest::cancelled);
      assertThat(registry.create(request("team-a", "t3")).state()).isEqualTo(State.PENDING_APPROVAL);

      registry.transition(second.requestCode(), Set.of(State.PENDING_APPROVAL),
          r -> r.rejected("admin", NOW, "khong"));
      assertThat(registry.create(request("team-a", "t4")).state()).isEqualTo(State.PENDING_APPROVAL);
    }

    @Test
    @DisplayName("mọi trạng thái chưa cuối đều chiếm suất, kể cả JOB_SUBMITTED; SUCCEEDED thì không")
    void onlyActiveStatesCountTowardsTheLimit() {
      DeidRequestRegistry registry = registry(1);
      DeidRequest r = registry.create(request("team-a", "t1"));
      approve(registry, r.requestCode());
      registry.transition(r.requestCode(), Set.of(State.APPROVED), x -> x.submitted(JOB, 1L));
      assertApiError(catchThrowable(() -> registry.create(request("team-a", "t2"))),
          HttpStatus.TOO_MANY_REQUESTS, "too_many_active_requests");

      registry.transition(r.requestCode(), Set.of(State.JOB_SUBMITTED), x -> x.jobProgress(JobState.SUCCEEDED, NOW));

      assertThat(registry.create(request("team-a", "t2")).state()).isEqualTo(State.PENDING_APPROVAL);
    }

    @Test
    @DisplayName("trùng (team, database, table, tập cột) với phiếu đang hoạt động thì 409 kèm mã phiếu cũ")
    void create_duplicateOfActive_isRejected_withExistingCode() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest existing = registry.create(request("team-a", "t1", "msisdn", "imei"));

      Throwable thrown = catchThrowable(() -> registry.create(request("team-a", "t1", "msisdn", "imei")));

      assertApiError(thrown, HttpStatus.CONFLICT, "duplicate_request");
      assertThat(thrown.getMessage()).contains(existing.requestCode());
      assertThat(registry.listForTeam("team-a")).hasSize(1);
    }

    @Test
    @DisplayName("trùng không phụ thuộc thứ tự cột")
    void create_duplicateIgnoresColumnOrder() {
      DeidRequestRegistry registry = registry(20);
      registry.create(request("team-a", "t1", "msisdn", "imei"));

      assertApiError(catchThrowable(() -> registry.create(request("team-a", "t1", "imei", "msisdn"))),
          HttpStatus.CONFLICT, "duplicate_request");
    }

    @Test
    @DisplayName("khác tập cột, khác bảng, khác database hoặc khác team thì không phải trùng")
    void create_differentRequests_areNotDuplicates() {
      DeidRequestRegistry registry = registry(20);
      registry.create(request("team-a", "t1", "msisdn", "imei"));

      assertThat(registry.create(request("team-a", "t1", "msisdn"))).isNotNull();
      assertThat(registry.create(request("team-a", "t2", "msisdn", "imei"))).isNotNull();
      assertThat(registry.create(request("team-b", "t1", "msisdn", "imei"))).isNotNull();
      NewRequest otherDb = new NewRequest("team-a", "u", "other_db", "t1",
          List.of(new ColumnSpec("msisdn", Mode.HASH), new ColumnSpec("imei", Mode.HASH)), null, "p",
          new SourceFile("f.json", "00"));
      assertThat(registry.create(otherDb)).isNotNull();
    }

    @Test
    @DisplayName("phiếu đã huỷ hoặc bị từ chối không chặn phiếu mới giống hệt")
    void create_afterCancelOrReject_isAllowed() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest first = registry.create(request("team-a", "t1"));
      registry.transitionForTeam("team-a", first.requestCode(), Set.of(State.PENDING_APPROVAL), DeidRequest::cancelled);
      DeidRequest second = registry.create(request("team-a", "t1"));
      registry.transition(second.requestCode(), Set.of(State.PENDING_APPROVAL), r -> r.rejected("admin", NOW, "khong"));

      assertThat(registry.create(request("team-a", "t1")).state()).isEqualTo(State.PENDING_APPROVAL);
    }
  }

  @Nested
  @DisplayName("chuyển trạng thái nguyên tử")
  class Transitions {

    @Test
    @DisplayName("chuyển hợp lệ được áp dụng, lưu lại và trả bản mới")
    void transition_valid_appliesAndStores() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));

      DeidRequest approved = approve(registry, created.requestCode());

      assertThat(approved.state()).isEqualTo(State.APPROVED);
      assertThat(registry.getByCode(created.requestCode())).isEqualTo(approved);
      assertThat(registry.get("team-a", created.requestCode())).isEqualTo(approved);
    }

    @Test
    @DisplayName("sai trạng thái hiện tại thì 409 invalid_state, nêu trạng thái hiện tại và không đổi gì")
    void transition_wrongCurrentState_isInvalidState() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));
      DeidRequest cancelled = registry.transitionForTeam("team-a", created.requestCode(),
          Set.of(State.PENDING_APPROVAL), DeidRequest::cancelled);

      Throwable thrown = catchThrowable(() -> approve(registry, created.requestCode()));

      assertApiError(thrown, HttpStatus.CONFLICT, "invalid_state");
      assertThat(thrown.getMessage()).contains("CANCELLED");
      assertThat(registry.getByCode(created.requestCode())).isEqualTo(cancelled);
    }

    @Test
    @DisplayName("mã không tồn tại thì 404")
    void transition_unknownCode_isNotFound() {
      DeidRequestRegistry registry = registry(20);

      assertApiError(catchThrowable(() -> registry.transition("PYC-2026-999999", Set.of(State.PENDING_APPROVAL),
          DeidRequest::cancelled)), HttpStatus.NOT_FOUND, "not_found");
    }

    @Test
    @DisplayName("hàm đổi trả về cạnh không hợp lệ bị từ chối và trạng thái cũ được giữ")
    void transition_illegalResultEdge_isRejected_stateKept() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));
      DeidRequest bogus = copyWithState(created, State.SUCCEEDED);

      assertThatThrownBy(() -> registry.transition(created.requestCode(), Set.of(State.PENDING_APPROVAL), r -> bogus))
          .isInstanceOf(IllegalStateException.class);

      assertThat(registry.getByCode(created.requestCode())).isEqualTo(created);
    }

    @Test
    @DisplayName("hàm đổi trả về phiếu khác (khác mã hoặc khác team) bị từ chối")
    void transition_resultOfAnotherRequest_isRejected() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));
      DeidRequest otherCode = DeidRequest.created("PYC-2026-777777", NOW, request("team-a", "t1")).cancelled();
      DeidRequest otherTeam = DeidRequest.created(created.requestCode(), NOW, request("team-b", "t1")).cancelled();

      assertThatThrownBy(() -> registry.transition(created.requestCode(), Set.of(State.PENDING_APPROVAL), r -> otherCode))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> registry.transition(created.requestCode(), Set.of(State.PENDING_APPROVAL), r -> otherTeam))
          .isInstanceOf(IllegalStateException.class);
      assertThat(registry.getByCode(created.requestCode())).isEqualTo(created);
    }

    @Test
    @DisplayName("cập nhật không đổi trạng thái (vd jobState UNKNOWN) được phép")
    void transition_sameStateUpdate_isAllowed() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));
      approve(registry, created.requestCode());
      registry.transition(created.requestCode(), Set.of(State.APPROVED), r -> r.submitted(JOB, 1L));

      DeidRequest updated = registry.transition(created.requestCode(), Set.of(State.JOB_SUBMITTED),
          r -> r.jobProgress(JobState.UNKNOWN, NOW));

      assertThat(updated.state()).isEqualTo(State.JOB_SUBMITTED);
      assertThat(updated.jobState()).isEqualTo(JobState.UNKNOWN);
    }

    @Test
    @DisplayName("team khác chuyển trạng thái phiếu thì 404 và phiếu không đổi")
    void transitionForTeam_otherTeam_isNotFound_andUnchanged() {
      DeidRequestRegistry registry = registry(20);
      DeidRequest created = registry.create(request("team-a", "t1"));

      assertApiError(catchThrowable(() -> registry.transitionForTeam("team-b", created.requestCode(),
          Set.of(State.PENDING_APPROVAL), DeidRequest::cancelled)), HttpStatus.NOT_FOUND, "not_found");

      assertThat(registry.getByCode(created.requestCode())).isEqualTo(created);
    }

    @Test
    @DisplayName("vòng đời đầy đủ: duyệt, gửi job lỗi, retry, gửi job được, chạy, xong")
    void fullLifecycle_throughTransitions() {
      DeidRequestRegistry registry = registry(20);
      String code = registry.create(request("team-a", "t1")).requestCode();

      approve(registry, code);
      registry.transition(code, Set.of(State.APPROVED), r -> r.submitFailed("table key not found"));
      registry.transition(code, Set.of(State.SUBMIT_FAILED), DeidRequest::retrying);
      registry.transition(code, Set.of(State.APPROVED), r -> r.submitted(JOB, 2L));
      registry.transition(code, Set.of(State.JOB_SUBMITTED, State.RUNNING), r -> r.jobProgress(JobState.RUNNING, NOW));
      DeidRequest done = registry.transition(code, Set.of(State.JOB_SUBMITTED, State.RUNNING),
          r -> r.jobProgress(JobState.SUCCEEDED, NOW));

      assertThat(done.state()).isEqualTo(State.SUCCEEDED);
      assertThat(done.keyVersion()).isEqualTo(2L);
      assertThat(done.submitError()).isNull();
    }
  }

  @Nested
  @DisplayName("đồng thời")
  class Concurrency {

    @Test
    @DisplayName("duyệt, huỷ và từ chối cùng lúc: đúng một thao tác thắng, các thao tác còn lại nhận 409")
    void approveCancelReject_exactlyOneWins() throws Exception {
      DeidRequestRegistry registry = registry(20);
      String code = registry.create(request("team-a", "t1")).requestCode();
      List<Callable<String>> tasks = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        tasks.add(() -> attempt(() -> approve(registry, code), "APPROVED"));
        tasks.add(() -> attempt(() -> registry.transitionForTeam("team-a", code, Set.of(State.PENDING_APPROVAL),
            DeidRequest::cancelled), "CANCELLED"));
        tasks.add(() -> attempt(() -> registry.transition(code, Set.of(State.PENDING_APPROVAL),
            r -> r.rejected("admin", NOW, "khong")), "REJECTED"));
      }

      List<String> results = runConcurrently(tasks);

      List<String> winners = results.stream().filter(x -> x != null).toList();
      assertThat(winners).hasSize(1);
      assertThat(registry.getByCode(code).state().name()).isEqualTo(winners.get(0));
    }

    @Test
    @DisplayName("tạo phiếu đồng thời không vượt giới hạn phiếu đang hoạt động và mã phiếu không trùng")
    void concurrentCreate_respectsActiveLimit_andCodesAreUnique() throws Exception {
      DeidRequestRegistry registry = registry(5);
      List<Callable<String>> tasks = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        String table = "t" + i;
        tasks.add(() -> attempt(() -> registry.create(request("team-a", table)), table));
      }

      List<String> results = runConcurrently(tasks);

      assertThat(results.stream().filter(x -> x != null)).hasSize(5);
      List<DeidRequest> stored = registry.listForTeam("team-a");
      assertThat(stored).hasSize(5);
      Set<String> codes = new HashSet<>();
      stored.forEach(r -> codes.add(r.requestCode()));
      assertThat(codes).hasSize(5);
    }

    @Test
    @DisplayName("tạo cùng một phiếu đồng thời chỉ tạo được một")
    void concurrentIdenticalCreate_createsExactlyOne() throws Exception {
      DeidRequestRegistry registry = registry(20);
      List<Callable<String>> tasks = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        tasks.add(() -> attempt(() -> registry.create(request("team-a", "t1")), "created"));
      }

      List<String> results = runConcurrently(tasks);

      assertThat(results.stream().filter(x -> x != null)).hasSize(1);
      assertThat(registry.listForTeam("team-a")).hasSize(1);
    }
  }

  /** Chạy thao tác; thành công thì trả {@code label}, thua (409/429) thì trả null, lỗi khác thì ném. */
  private static String attempt(Supplier<?> action, String label) {
    try {
      action.get();
      return label;
    } catch (DakApiException e) {
      assertThat(e.status()).isIn(HttpStatus.CONFLICT, HttpStatus.TOO_MANY_REQUESTS);
      return null;
    }
  }

  private static List<String> runConcurrently(List<Callable<String>> tasks) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(Math.max(2, tasks.size()));
    try {
      CountDownLatch ready = new CountDownLatch(tasks.size());
      CountDownLatch go = new CountDownLatch(1);
      List<Future<String>> futures = new ArrayList<>();
      for (Callable<String> task : tasks) {
        futures.add(pool.submit(() -> {
          ready.countDown();
          go.await();
          return task.call();
        }));
      }
      assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
      go.countDown();
      List<String> results = new ArrayList<>();
      for (Future<String> future : futures) {
        results.add(future.get(10, TimeUnit.SECONDS));
      }
      return results;
    } finally {
      pool.shutdownNow();
    }
  }

  /** Dựng phiếu giống {@code r} nhưng ở trạng thái tuỳ ý, để thử việc registry chặn cạnh không hợp lệ. */
  private static DeidRequest copyWithState(DeidRequest r, State state) {
    return new DeidRequest(r.requestCode(), state, r.database(), r.table(), r.columns(), r.dagId(), r.purpose(), r.env(), r.team(),
        r.createdBy(), r.createdAt(), r.sourceFile(), r.decidedAt(), r.decidedBy(), r.reasonText(), r.outputDatabase(),
        r.outputTable(), r.keyVersion(), r.jobRef(), r.submitError(), r.jobState(), r.jobFinishedAt());
  }
}
