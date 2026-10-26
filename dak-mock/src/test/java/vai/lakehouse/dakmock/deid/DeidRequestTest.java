package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;
import vai.lakehouse.dakmock.deid.DeidRequest.State;

/**
 * D2 của flow phiếu khử định danh (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.2 và 5): phiếu là record bất biến,
 * máy trạng thái chỉ cho đúng các cạnh ghi ở docs/dak-deid-flow/README.md mục 2.
 */
class DeidRequestTest {

  private static final Instant T0 = Instant.parse("2026-10-23T08:00:00Z");
  private static final Instant T1 = Instant.parse("2026-10-23T09:00:00Z");
  private static final JobRef JOB = new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-2026-000001");

  /** Các cạnh hợp lệ theo README mục 2, cộng cạnh nội bộ SUBMIT_FAILED → APPROVED của {@code retry-submit} (plan mục 4.3). */
  private static final Map<State, Set<State>> EDGES = Map.of(
      State.PENDING_APPROVAL, EnumSet.of(State.CANCELLED, State.REJECTED, State.APPROVED),
      State.APPROVED, EnumSet.of(State.JOB_SUBMITTED, State.SUBMIT_FAILED),
      State.SUBMIT_FAILED, EnumSet.of(State.APPROVED, State.JOB_SUBMITTED),
      State.JOB_SUBMITTED, EnumSet.of(State.RUNNING, State.SUCCEEDED, State.FAILED),
      State.RUNNING, EnumSet.of(State.SUCCEEDED, State.FAILED),
      State.REJECTED, EnumSet.noneOf(State.class),
      State.CANCELLED, EnumSet.noneOf(State.class),
      State.SUCCEEDED, EnumSet.noneOf(State.class),
      State.FAILED, EnumSet.noneOf(State.class));

  static NewRequest newRequest() {
    return new NewRequest("team-a", "hungvt", "demo_db", "users_cdr",
        List.of(new ColumnSpec("msisdn", Mode.HASH), new ColumnSpec("imei", Mode.HASH)),
        "deid_pseudonymize_table", "thu nghiem", new SourceFile("phieu.json", "ab".repeat(32)));
  }

  static DeidRequest created() {
    return DeidRequest.created("PYC-2026-000001", T0, newRequest());
  }

  static Stream<Arguments> everyStatePair() {
    Stream.Builder<Arguments> pairs = Stream.builder();
    for (State from : State.values()) {
      for (State to : State.values()) {
        pairs.add(Arguments.of(from, to, EDGES.get(from).contains(to)));
      }
    }
    return pairs.build();
  }

  @ParameterizedTest(name = "{0} -> {1} cho phép: {2}")
  @MethodSource("everyStatePair")
  @DisplayName("máy trạng thái chỉ cho đúng các cạnh đã tài liệu hoá, không có cạnh tự quay về chính nó")
  void stateMachine_allowsExactlyTheDocumentedEdges(State from, State to, boolean allowed) {
    assertThat(from.canMoveTo(to)).isEqualTo(allowed);
  }

  @Test
  @DisplayName("trạng thái cuối là REJECTED, CANCELLED, SUCCEEDED, FAILED; còn lại là đang hoạt động")
  void finalAndActiveStates_areComplementary() {
    Set<State> finals = EnumSet.of(State.REJECTED, State.CANCELLED, State.SUCCEEDED, State.FAILED);

    for (State state : State.values()) {
      assertThat(state.isFinal()).as("%s isFinal", state).isEqualTo(finals.contains(state));
      assertThat(state.isActive()).as("%s isActive", state).isEqualTo(!finals.contains(state));
    }
  }

  @Test
  @DisplayName("phiếu mới ở PENDING_APPROVAL, mang đúng dữ liệu người dùng gửi và chưa có trường của các bước sau")
  void created_startsPendingWithGivenFieldsOnly() {
    DeidRequest r = created();

    assertThat(r.requestCode()).isEqualTo("PYC-2026-000001");
    assertThat(r.state()).isEqualTo(State.PENDING_APPROVAL);
    assertThat(r.team()).isEqualTo("team-a");
    assertThat(r.createdBy()).isEqualTo("hungvt");
    assertThat(r.createdAt()).isEqualTo(T0);
    assertThat(r.database()).isEqualTo("demo_db");
    assertThat(r.table()).isEqualTo("users_cdr");
    assertThat(r.columns()).extracting(ColumnSpec::column).containsExactly("msisdn", "imei");
    assertThat(r.dagId()).isEqualTo("deid_pseudonymize_table");
    assertThat(r.purpose()).isEqualTo("thu nghiem");
    assertThat(r.sourceFile().name()).isEqualTo("phieu.json");
    assertThat(r.decidedAt()).isNull();
    assertThat(r.decidedBy()).isNull();
    assertThat(r.reasonText()).isNull();
    assertThat(r.outputDatabase()).isNull();
    assertThat(r.outputTable()).isNull();
    assertThat(r.keyVersion()).isNull();
    assertThat(r.jobRef()).isNull();
    assertThat(r.submitError()).isNull();
    assertThat(r.jobState()).isNull();
    assertThat(r.jobFinishedAt()).isNull();
  }

  @Test
  @DisplayName("danh sách cột được sao chép và không sửa được từ bên ngoài")
  void created_columnsAreDefensivelyCopiedAndImmutable() {
    List<ColumnSpec> columns = new ArrayList<>(List.of(new ColumnSpec("msisdn", Mode.HASH)));
    DeidRequest r = DeidRequest.created("PYC-2026-000001", T0, new NewRequest("team-a", "hungvt", "demo_db", "t",
        columns, null, "p", new SourceFile("f.json", "00")));

    columns.add(new ColumnSpec("imei", Mode.HASH));

    assertThat(r.columns()).hasSize(1);
    assertThatThrownBy(() -> r.columns().add(new ColumnSpec("x", Mode.HASH)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @DisplayName("duyệt trả bản mới ở APPROVED với người duyệt và bảng đích, bản gốc không đổi")
  void approved_returnsNewCopy_originalUntouched() {
    DeidRequest original = created();

    DeidRequest approved = original.approved("admin", T1, "demo_db", "users_cdr_deid_20261023_1");

    assertThat(approved).isNotSameAs(original);
    assertThat(original.state()).isEqualTo(State.PENDING_APPROVAL);
    assertThat(original.outputTable()).isNull();
    assertThat(approved.state()).isEqualTo(State.APPROVED);
    assertThat(approved.decidedBy()).isEqualTo("admin");
    assertThat(approved.decidedAt()).isEqualTo(T1);
    assertThat(approved.outputDatabase()).isEqualTo("demo_db");
    assertThat(approved.outputTable()).isEqualTo("users_cdr_deid_20261023_1");
    assertThat(approved.requestCode()).isEqualTo(original.requestCode());
    assertThat(approved.team()).isEqualTo(original.team());
  }

  @Test
  @DisplayName("từ chối ghi lý do và người từ chối")
  void rejected_recordsReason() {
    DeidRequest rejected = created().rejected("admin", T1, "khong du ly do");

    assertThat(rejected.state()).isEqualTo(State.REJECTED);
    assertThat(rejected.reasonText()).isEqualTo("khong du ly do");
    assertThat(rejected.decidedBy()).isEqualTo("admin");
    assertThat(rejected.decidedAt()).isEqualTo(T1);
    assertThat(rejected.outputTable()).isNull();
  }

  @Test
  @DisplayName("huỷ chuyển sang CANCELLED")
  void cancelled_movesToCancelled() {
    assertThat(created().cancelled().state()).isEqualTo(State.CANCELLED);
  }

  @Test
  @DisplayName("gửi job thành công ghi jobRef và version key, xoá lỗi cũ")
  void submitted_recordsJobRefAndKeyVersion_andClearsError() {
    DeidRequest failed = created().approved("admin", T1, "demo_db", "t_out").submitFailed("table key not found");

    DeidRequest submitted = failed.submitted(JOB, 3L);

    assertThat(failed.submitError()).isEqualTo("table key not found");
    assertThat(submitted.state()).isEqualTo(State.JOB_SUBMITTED);
    assertThat(submitted.jobRef()).isEqualTo(JOB);
    assertThat(submitted.keyVersion()).isEqualTo(3L);
    assertThat(submitted.submitError()).isNull();
  }

  @Test
  @DisplayName("gửi job lỗi vào SUBMIT_FAILED, retry quay về APPROVED và xoá lỗi")
  void submitFailed_thenRetrying_clearsError() {
    DeidRequest approved = created().approved("admin", T1, "demo_db", "t_out");

    DeidRequest failed = approved.submitFailed("vault unavailable");
    DeidRequest retried = failed.retrying();

    assertThat(failed.state()).isEqualTo(State.SUBMIT_FAILED);
    assertThat(failed.submitError()).isEqualTo("vault unavailable");
    assertThat(retried.state()).isEqualTo(State.APPROVED);
    assertThat(retried.submitError()).isNull();
    assertThat(retried.outputTable()).isEqualTo("t_out");
  }

  @Test
  @DisplayName("hàm chuyển trạng thái từ chối cạnh không hợp lệ")
  void helpers_rejectIllegalEdges() {
    assertThatThrownBy(() -> created().submitted(JOB, 1L)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> created().cancelled().approved("admin", T1, "d", "t"))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> created().rejected("admin", T1, "x").cancelled())
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> created().retrying()).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> created().submitFailed("x")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("tiến độ job: RUNNING giữ chưa có giờ kết thúc, lặp lại RUNNING không phải lỗi")
  void jobProgress_running() {
    DeidRequest submitted = created().approved("admin", T1, "d", "t").submitted(JOB, 1L);

    DeidRequest running = submitted.jobProgress(JobState.RUNNING, T1);
    DeidRequest again = running.jobProgress(JobState.RUNNING, T1);

    assertThat(running.state()).isEqualTo(State.RUNNING);
    assertThat(running.jobState()).isEqualTo(JobState.RUNNING);
    assertThat(running.jobFinishedAt()).isNull();
    assertThat(again.state()).isEqualTo(State.RUNNING);
  }

  @Test
  @DisplayName("tiến độ job: SUCCEEDED và FAILED là trạng thái cuối và ghi giờ kết thúc")
  void jobProgress_terminalStates() {
    DeidRequest submitted = created().approved("admin", T1, "d", "t").submitted(JOB, 1L);

    DeidRequest succeeded = submitted.jobProgress(JobState.RUNNING, T0).jobProgress(JobState.SUCCEEDED, T1);
    DeidRequest failed = submitted.jobProgress(JobState.FAILED, T1);

    assertThat(succeeded.state()).isEqualTo(State.SUCCEEDED);
    assertThat(succeeded.jobState()).isEqualTo(JobState.SUCCEEDED);
    assertThat(succeeded.jobFinishedAt()).isEqualTo(T1);
    assertThat(failed.state()).isEqualTo(State.FAILED);
    assertThat(failed.jobFinishedAt()).isEqualTo(T1);
  }

  @Test
  @DisplayName("tiến độ job UNKNOWN giữ nguyên trạng thái phiếu, chỉ đổi jobState")
  void jobProgress_unknown_keepsState() {
    DeidRequest submitted = created().approved("admin", T1, "d", "t").submitted(JOB, 1L);

    DeidRequest unknown = submitted.jobProgress(JobState.UNKNOWN, T1);

    assertThat(unknown.state()).isEqualTo(State.JOB_SUBMITTED);
    assertThat(unknown.jobState()).isEqualTo(JobState.UNKNOWN);
    assertThat(unknown.jobFinishedAt()).isNull();
  }

  @Test
  @DisplayName("tiến độ job chỉ áp dụng cho phiếu đã gửi job")
  void jobProgress_requiresSubmittedJob() {
    assertThatThrownBy(() -> created().jobProgress(JobState.RUNNING, T1)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> created().cancelled().jobProgress(JobState.SUCCEEDED, T1))
        .isInstanceOf(IllegalStateException.class);
  }
}
