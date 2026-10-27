package vai.lakehouse.dakmock.deid;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Map;

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
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;
import vai.lakehouse.dakmock.config.DeidProperties.AirflowAuth;
import vai.lakehouse.dakmock.config.DeidProperties.Kubernetes;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * D4: nghiệp vụ của người dùng portal (upload, huỷ, xem, danh sách, audit) trên registry thật, plan mục 4.1, 4.2 và 4.9.
 * Mọi quy tắc nằm ở service nên test ở đây không cần HTTP.
 */
class DeidRequestServiceTest {

  private static final Instant NOW = Instant.parse("2026-10-23T08:00:00Z");
  private static final PortalUser ALICE = new PortalUser("alice", "team-a", null, null, "t".repeat(24));
  private static final PortalUser BOB = new PortalUser("bob", "team-a", null, null, "u".repeat(24));
  private static final PortalUser CAROL = new PortalUser("carol", "team-b", null, null, "v".repeat(24));
  private static final String PURPOSE_TEXT = "ly do rieng tu khong duoc vao audit";

  private DeidRequestRegistry registry;
  private DeidRequestService service;
  private ListAppender<ILoggingEvent> audit;
  private Logger auditLogger;

  @BeforeEach
  void setUp() {
    DeidProperties deid = DeidProperties.defaults();
    registry = new DeidRequestRegistry(50, Clock.fixed(NOW, ZoneOffset.UTC));
    service = new DeidRequestService(deid, new DeidUploadParser(deid), registry);
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

  private static InputStream file(String table, String... columns) {
    List<String> items = new ArrayList<>();
    for (String column : columns) {
      items.add("{\"column\":\"" + column + "\"}");
    }
    String json = "{\"database\":\"demo_db\",\"table\":\"" + table + "\",\"columns\":[" + String.join(",", items)
        + "],\"purpose\":\"" + PURPOSE_TEXT + "\"}";
    return new ByteArrayInputStream(json.getBytes(UTF_8));
  }

  private DeidRequest upload(PortalUser user, String table) {
    return service.upload(user, "phieu.json", file(table, "msisdn", "imei"));
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
  @DisplayName("upload")
  class Upload {

    @Test
    @DisplayName("env người gửi nhập được lưu vào phiếu; phiếu không nhập env thì env rỗng")
    void storesTheEnv() {
      String json = "{\"database\":\"demo_db\",\"table\":\"users_cdr\",\"columns\":[{\"column\":\"msisdn\"}],"
          + "\"env\":{\"OUTPUT_TABLE\":\"users_pseudo\",\"OUTPUT_MODE\":\"append\"},\"purpose\":\"p\"}";

      DeidRequest withEnv = service.upload(ALICE, "f.json", new ByteArrayInputStream(json.getBytes(UTF_8)));
      DeidRequest plain = upload(BOB, "other_table");

      assertThat(withEnv.env()).containsExactly(Map.entry("OUTPUT_MODE", "append"), Map.entry("OUTPUT_TABLE", "users_pseudo"));
      assertThat(plain.env()).isEmpty();
      assertThat(registry.getByCode(withEnv.requestCode()).env()).isEqualTo(withEnv.env());
    }

    @Test
    @DisplayName("audit lúc gửi phiếu ghi tên env và số lượng, không ghi giá trị")
    void auditRecordsEnvNamesButNotValues() {
      String json = "{\"database\":\"demo_db\",\"table\":\"users_cdr\",\"columns\":[{\"column\":\"msisdn\"}],"
          + "\"env\":{\"OUTPUT_TABLE\":\"users_pseudo_zzz\",\"OUTPUT_MODE\":\"append\"},\"purpose\":\"p\"}";

      service.upload(ALICE, "f.json", new ByteArrayInputStream(json.getBytes(UTF_8)));

      String line = auditLines().get(0);
      assertThat(line).contains("envNames=OUTPUT_MODE,OUTPUT_TABLE").contains("envCount=2");
      assertThat(line).doesNotContain("users_pseudo_zzz").doesNotContain("append");
    }

    @Test
    @DisplayName("phiếu không có env thì audit không có envNames")
    void auditHasNoEnvFieldsWithoutEnv() {
      upload(ALICE, "users_cdr");

      assertThat(auditLines().get(0)).doesNotContain("envNames").doesNotContain("envCount");
    }

    @Test
    @DisplayName("env sai thì không tạo phiếu: 400 invalid_file kèm problems của env")
    void invalidEnv_createsNothing() {
      String json = "{\"database\":\"demo_db\",\"table\":\"users_cdr\",\"columns\":[{\"column\":\"msisdn\"}],"
          + "\"env\":{\"DB_NAME\":\"x\"},\"purpose\":\"p\"}";
      int before = registry.listAll().size();

      Throwable thrown = catchThrowable(() -> service.upload(ALICE, "f.json", new ByteArrayInputStream(json.getBytes(UTF_8))));

      assertApiError(thrown, HttpStatus.BAD_REQUEST, "invalid_file");
      assertThat(registry.listAll()).hasSize(before);
    }

    @Test
    @DisplayName("tạo phiếu PENDING_APPROVAL thuộc team của người upload, ghi người upload và file")
    void createsPendingRequestForTheUsersTeam() {
      DeidRequest created = upload(ALICE, "users_cdr");

      assertThat(created.state()).isEqualTo(State.PENDING_APPROVAL);
      assertThat(created.team()).isEqualTo("team-a");
      assertThat(created.createdBy()).isEqualTo("alice");
      assertThat(created.table()).isEqualTo("users_cdr");
      assertThat(created.purpose()).isEqualTo(PURPOSE_TEXT);
      assertThat(created.sourceFile().name()).isEqualTo("phieu.json");
      assertThat(created.sourceFile().sha256()).hasSize(64);
      assertThat(registry.get("team-a", created.requestCode())).isEqualTo(created);
    }

    @Test
    @DisplayName("file lỗi: ném invalid_file và không tạo phiếu")
    void invalidFile_createsNothing() {
      Throwable thrown = catchThrowable(() -> service.upload(ALICE, "x.json",
          new ByteArrayInputStream("{\"database\":\"d\"}".getBytes(UTF_8))));

      assertApiError(thrown, HttpStatus.BAD_REQUEST, "invalid_file");
      assertThat(((DakApiException) thrown).problems()).isNotEmpty();
      assertThat(registry.listAll()).isEmpty();
    }

    @Test
    @DisplayName("upload trùng của cùng team (kể cả người khác) bị từ chối với mã phiếu cũ")
    void duplicate_isRejectedForTheWholeTeam() {
      DeidRequest first = upload(ALICE, "users_cdr");

      Throwable thrown = catchThrowable(() -> upload(BOB, "users_cdr"));

      assertApiError(thrown, HttpStatus.CONFLICT, "duplicate_request");
      assertThat(thrown.getMessage()).contains(first.requestCode());
      assertThat(upload(CAROL, "users_cdr").team()).isEqualTo("team-b");
    }
  }

  @Nested
  @DisplayName("huỷ, xem")
  class CancelAndGet {

    @Test
    @DisplayName("huỷ phiếu chờ duyệt; huỷ lặp lại không phải lỗi và trả lại đúng phiếu đã huỷ")
    void cancel_isIdempotent() {
      DeidRequest created = upload(ALICE, "users_cdr");

      DeidRequest first = service.cancel(ALICE, created.requestCode());
      DeidRequest second = service.cancel(BOB, created.requestCode());

      assertThat(first.state()).isEqualTo(State.CANCELLED);
      assertThat(second).isEqualTo(first);
    }

    @Test
    @DisplayName("huỷ phiếu đã duyệt bị từ chối 409 và phiếu không đổi")
    void cancel_afterApproval_isRejected() {
      DeidRequest created = upload(ALICE, "users_cdr");
      registry.transition(created.requestCode(), Set.of(State.PENDING_APPROVAL),
          r -> r.approved("admin", NOW, "demo_db", "out"));

      Throwable thrown = catchThrowable(() -> service.cancel(ALICE, created.requestCode()));

      assertApiError(thrown, HttpStatus.CONFLICT, "invalid_state");
      assertThat(service.get(ALICE, created.requestCode()).state()).isEqualTo(State.APPROVED);
    }

    @Test
    @DisplayName("team khác không xem, không huỷ được; lỗi giống hệt phiếu không tồn tại")
    void otherTeam_cannotSeeOrCancel_andErrorMatchesUnknown() {
      DeidRequest created = upload(ALICE, "users_cdr");

      Throwable viewForeign = catchThrowable(() -> service.get(CAROL, created.requestCode()));
      Throwable viewUnknown = catchThrowable(() -> service.get(CAROL, "PYC-2026-999999"));
      Throwable cancelForeign = catchThrowable(() -> service.cancel(CAROL, created.requestCode()));

      assertApiError(viewForeign, HttpStatus.NOT_FOUND, "not_found");
      assertApiError(cancelForeign, HttpStatus.NOT_FOUND, "not_found");
      assertThat(viewForeign.getMessage()).isEqualTo(viewUnknown.getMessage());
      assertThat(service.get(ALICE, created.requestCode()).state()).isEqualTo(State.PENDING_APPROVAL);
    }

    @Test
    @DisplayName("mã sai định dạng trả cùng lỗi 404 như mã không tồn tại")
    void malformedCode_isNotFound() {
      for (String bad : List.of("", "x", "PYC-1", "pyc-2026-000001", "PYC-2026-0000011", "../etc", "PYC-2026-000001\n")) {
        assertApiError(catchThrowable(() -> service.get(ALICE, bad)), HttpStatus.NOT_FOUND, "not_found");
        assertApiError(catchThrowable(() -> service.cancel(ALICE, bad)), HttpStatus.NOT_FOUND, "not_found");
      }
    }
  }

  @Nested
  @DisplayName("danh sách của team")
  class Listing {

    private List<DeidRequest> create(PortalUser user, int count) {
      List<DeidRequest> created = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        created.add(upload(user, "t" + i));
      }
      return created;
    }

    private List<String> newestFirstCodes(List<DeidRequest> oldestFirst) {
      List<String> codes = new ArrayList<>(oldestFirst.stream().map(DeidRequest::requestCode).toList());
      Collections.reverse(codes);
      return codes;
    }

    @Test
    @DisplayName("mặc định mới nhất trước, chỉ gồm phiếu của team, không có nextCursor khi hết")
    void defaultListing() {
      List<DeidRequest> mine = create(ALICE, 3);
      upload(CAROL, "other");

      DeidRequestService.Page page = service.list(BOB, null, null, null);

      assertThat(page.requests()).extracting(DeidRequest::requestCode)
          .containsExactlyElementsOf(newestFirstCodes(mine));
      assertThat(page.nextCursor()).isNull();
    }

    @Test
    @DisplayName("lọc theo trạng thái, không phân biệt hoa thường")
    void filterByState() {
      List<DeidRequest> mine = create(ALICE, 3);
      service.cancel(ALICE, mine.get(1).requestCode());

      assertThat(service.list(ALICE, "CANCELLED", null, null).requests()).extracting(DeidRequest::requestCode)
          .containsExactly(mine.get(1).requestCode());
      assertThat(service.list(ALICE, "pending_approval", null, null).requests()).hasSize(2);
      assertThat(service.list(ALICE, "SUCCEEDED", null, null).requests()).isEmpty();
    }

    @Test
    @DisplayName("phân trang bằng limit và cursor: các trang nối nhau đủ, không lặp, trang cuối không có nextCursor")
    void paging() {
      List<String> newestFirst = newestFirstCodes(create(ALICE, 5));

      DeidRequestService.Page page1 = service.list(ALICE, null, "2", null);
      DeidRequestService.Page page2 = service.list(ALICE, null, "2", page1.nextCursor());
      DeidRequestService.Page page3 = service.list(ALICE, null, "2", page2.nextCursor());

      assertThat(page1.requests()).extracting(DeidRequest::requestCode).containsExactlyElementsOf(newestFirst.subList(0, 2));
      assertThat(page1.nextCursor()).isEqualTo(newestFirst.get(1));
      assertThat(page2.requests()).extracting(DeidRequest::requestCode).containsExactlyElementsOf(newestFirst.subList(2, 4));
      assertThat(page3.requests()).extracting(DeidRequest::requestCode).containsExactly(newestFirst.get(4));
      assertThat(page3.nextCursor()).isNull();
    }

    @Test
    @DisplayName("trang đúng bằng số phiếu còn lại thì không có nextCursor")
    void exactFit_hasNoNextCursor() {
      create(ALICE, 4);

      assertThat(service.list(ALICE, null, "4", null).nextCursor()).isNull();
      assertThat(service.list(ALICE, null, "3", null).nextCursor()).isNotNull();
    }

    @Test
    @DisplayName("cursor vẫn dùng được khi phiếu làm cursor không còn khớp bộ lọc trạng thái")
    void cursorSurvivesFilterChange() {
      List<String> newestFirst = newestFirstCodes(create(ALICE, 4));
      DeidRequestService.Page first = service.list(ALICE, null, "2", null);
      service.cancel(ALICE, first.nextCursor());

      DeidRequestService.Page rest = service.list(ALICE, "PENDING_APPROVAL", "10", first.nextCursor());

      assertThat(first.nextCursor()).isEqualTo(newestFirst.get(1));
      assertThat(rest.requests()).extracting(DeidRequest::requestCode)
          .containsExactly(newestFirst.get(2), newestFirst.get(3));
    }

    @Test
    @DisplayName("tham số sai bị 400 invalid_request: limit ngoài 1..100 hoặc không phải số, state lạ, cursor lạ hoặc của team khác")
    void invalidParameters() {
      create(ALICE, 1);
      DeidRequest foreign = upload(CAROL, "other");

      for (String badLimit : List.of("0", "101", "-1", "abc", "1.5", "")) {
        assertApiError(catchThrowable(() -> service.list(ALICE, null, badLimit, null)), HttpStatus.BAD_REQUEST,
            "invalid_request");
      }
      assertApiError(catchThrowable(() -> service.list(ALICE, "BOGUS", null, null)), HttpStatus.BAD_REQUEST,
          "invalid_request");
      assertApiError(catchThrowable(() -> service.list(ALICE, null, null, "not-a-code")), HttpStatus.BAD_REQUEST,
          "invalid_request");
      // Cursor là mã phiếu của team khác: cùng lỗi như cursor không tồn tại, không lộ phiếu có tồn tại.
      Throwable foreignCursor = catchThrowable(() -> service.list(ALICE, null, null, foreign.requestCode()));
      Throwable unknownCursor = catchThrowable(() -> service.list(ALICE, null, null, "PYC-2026-999999"));
      assertApiError(foreignCursor, HttpStatus.BAD_REQUEST, "invalid_request");
      assertApiError(unknownCursor, HttpStatus.BAD_REQUEST, "invalid_request");
      assertThat(foreignCursor.getMessage()).isEqualTo(unknownCursor.getMessage());
    }

    @Test
    @DisplayName("limit biên 1 và 100 được chấp nhận")
    void limitBoundaries() {
      create(ALICE, 2);

      assertThat(service.list(ALICE, null, "1", null).requests()).hasSize(1);
      assertThat(service.list(ALICE, null, "100", null).requests()).hasSize(2);
    }
  }

  @Nested
  @DisplayName("dagId được phép")
  class AllowedDags {

    @Test
    @DisplayName("submitter không phải airflow thì danh sách rỗng")
    void emptyWhenNotAirflow() {
      assertThat(service.allowedDagIds()).isEmpty();
    }

    @Test
    @DisplayName("submitter airflow trả đúng allowed-dag-ids, không sửa được từ ngoài")
    void airflowReturnsConfiguredList() {
      DeidProperties deid = new DeidProperties(true, null, 20, 65536, null, null, Submitter.AIRFLOW,
          new Kubernetes(null, "ns", null),
          new Airflow("http://airflow:8080", List.of("deid_a", "deid_b"), AirflowAuth.BEARER, null, null, null, null, "t"),
          List.of(ALICE));
      DeidRequestService airflowService = new DeidRequestService(deid, new DeidUploadParser(deid), registry);

      List<String> dags = airflowService.allowedDagIds();

      assertThat(dags).containsExactly("deid_a", "deid_b");
      assertThatThrownBy(() -> dags.add("x")).isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Nested
  @DisplayName("audit (plan mục 4.9)")
  class Audit {

    @Test
    @DisplayName("upload ghi một dòng DEID_TRANSITION có người upload, team, bảng, cột và SHA-256, không có purpose")
    void upload_isAudited_withoutPurpose() {
      DeidRequest created = upload(ALICE, "users_cdr");

      List<String> lines = auditLines();
      assertThat(lines).hasSize(1);
      String line = lines.get(0);
      assertThat(line).startsWith("DEID_TRANSITION").contains("requestCode=" + created.requestCode())
          .contains("from=-").contains("to=PENDING_APPROVAL").contains("actor=alice").contains("team=team-a")
          .contains("database=demo_db").contains("table=users_cdr").contains("columns=msisdn,imei")
          .contains("fileSha256=" + created.sourceFile().sha256()).contains("result=OK");
      assertThat(line).doesNotContain(PURPOSE_TEXT);
    }

    @Test
    @DisplayName("huỷ ghi dòng chuyển PENDING_APPROVAL sang CANCELLED; huỷ lặp lại không ghi thêm")
    void cancel_isAudited_once() {
      DeidRequest created = upload(ALICE, "users_cdr");
      audit.list.clear();

      service.cancel(BOB, created.requestCode());
      service.cancel(BOB, created.requestCode());

      assertThat(auditLines()).hasSize(1);
      assertThat(auditLines().get(0)).contains("from=PENDING_APPROVAL").contains("to=CANCELLED").contains("actor=bob")
          .contains("requestCode=" + created.requestCode()).contains("result=OK");
    }

    @Test
    @DisplayName("thao tác bị từ chối ghi result=ERROR kèm mã lỗi và người thực hiện")
    void rejections_areAudited_withErrorCode() {
      upload(ALICE, "users_cdr");
      audit.list.clear();

      catchThrowable(() -> upload(BOB, "users_cdr"));
      catchThrowable(() -> service.upload(BOB, "x.json", new ByteArrayInputStream("nope".getBytes(UTF_8))));
      catchThrowable(() -> service.cancel(CAROL, "PYC-2026-999999"));

      assertThat(auditLines()).hasSize(3);
      assertThat(auditLines().get(0)).contains("result=ERROR").contains("error=duplicate_request").contains("actor=bob");
      assertThat(auditLines().get(1)).contains("result=ERROR").contains("error=invalid_file").contains("actor=bob");
      assertThat(auditLines().get(2)).contains("result=ERROR").contains("error=not_found").contains("actor=carol");
    }

    @Test
    @DisplayName("dòng audit không chứa xuống dòng và không lặp lại mã phiếu sai định dạng người dùng gửi")
    void auditLines_neverEchoUserSuppliedCodes() {
      catchThrowable(() -> service.cancel(ALICE, "PYC-2026-000001\r\nFAKE_AUDIT_LINE result=OK"));

      assertThat(auditLines()).hasSize(1);
      assertThat(auditLines().get(0)).doesNotContain("\n").doesNotContain("\r").doesNotContain("FAKE_AUDIT_LINE")
          .contains("requestCode=-");
    }
  }
}
