package vai.lakehouse.dakmock.deid;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;
import vai.lakehouse.dakmock.config.DeidProperties.AirflowAuth;
import vai.lakehouse.dakmock.config.DeidProperties.Kubernetes;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.web.DakApiException;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/**
 * D3 của flow phiếu khử định danh (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.1, API_CONTRACT.md mục 2): đọc file
 * upload thành {@link DeidUpload}, kiểm nghiêm ngặt, gom mọi lỗi nội dung một lượt, không bao giờ lặp lại nội dung file người
 * dùng trong thông điệp lỗi.
 */
class DeidUploadParserTest {

  private static final String VALID = """
      {"database":"Demo_DB","table":"Users_CDR",
       "columns":[{"column":"MSISDN","mode":"hash"},{"column":"imei"}],
       "dagId":"deid_a","purpose":"  thu nghiem  "}""";

  private static DeidProperties noop() {
    return DeidProperties.defaults();
  }

  private static DeidProperties airflow(String... dagIds) {
    return new DeidProperties(true, null, 20, 65536, null, null, Submitter.AIRFLOW, new Kubernetes(null, "ns", null),
        new Airflow("http://airflow:8080", List.of(dagIds), AirflowAuth.BEARER, null, null, null, null, "t"), null);
  }

  private static DeidProperties withMaxBytes(int maxBytes) {
    return new DeidProperties(true, null, 20, maxBytes, null, null, null, null, null, null);
  }

  private static InputStream stream(String s) {
    return new ByteArrayInputStream(s.getBytes(UTF_8));
  }

  private static DeidUpload parse(DeidProperties deid, String json) {
    return new DeidUploadParser(deid).parse("phieu.json", stream(json));
  }

  private static DakApiException rejected(DeidProperties deid, String json) {
    Throwable thrown = catchThrowable(() -> parse(deid, json));
    assertThat(thrown).isInstanceOf(DakApiException.class);
    DakApiException e = (DakApiException) thrown;
    assertThat(e.status()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(e.error()).isEqualTo("invalid_file");
    return e;
  }

  /** Dựng object JSON từ các cặp tên, giá trị (giá trị là mảnh JSON thô); giá trị null thì bỏ trường. */
  private static String doc(String... pairs) {
    List<String> fields = new ArrayList<>();
    for (int i = 0; i < pairs.length; i += 2) {
      if (pairs[i + 1] != null) {
        fields.add("\"" + pairs[i] + "\":" + pairs[i + 1]);
      }
    }
    return "{" + String.join(",", fields) + "}";
  }

  /** Object hợp lệ; {@code overrides} là cặp (tên trường, mảnh JSON) thay thế hoặc thêm; mảnh null thì bỏ trường. */
  private static String validDoc(String... overrides) {
    List<String> pairs = new ArrayList<>(List.of("database", "\"demo_db\"", "table", "\"users_cdr\"",
        "columns", "[{\"column\":\"msisdn\"}]", "dagId", "\"deid_a\"", "purpose", "\"thu nghiem\""));
    for (int i = 0; i < overrides.length; i += 2) {
      int at = pairs.indexOf(overrides[i]);
      if (at >= 0) {
        pairs.set(at + 1, overrides[i + 1]);
      } else {
        pairs.add(overrides[i]);
        pairs.add(overrides[i + 1]);
      }
    }
    return doc(pairs.toArray(String[]::new));
  }

  private static void assertProblems(DakApiException e, String field, String code) {
    assertThat(e.problems()).extracting(Problem::field, Problem::code).containsExactly(tuple(field, code));
  }

  private static String columns(int count) {
    List<String> items = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      items.add("{\"column\":\"c" + i + "\"}");
    }
    return "[" + String.join(",", items) + "]";
  }

  @Nested
  @DisplayName("file hợp lệ")
  class ValidFile {

    @Test
    @DisplayName("chuẩn hoá chữ thường, mode không phân biệt hoa thường, purpose được strip, ghi tên file và SHA-256")
    void parsesAndNormalises() throws Exception {
      DeidUpload upload = parse(airflow("deid_a"), VALID);

      assertThat(upload.database()).isEqualTo("demo_db");
      assertThat(upload.table()).isEqualTo("users_cdr");
      assertThat(upload.columns()).containsExactly(new ColumnSpec("msisdn", Mode.HASH), new ColumnSpec("imei", Mode.HASH));
      assertThat(upload.dagId()).isEqualTo("deid_a");
      assertThat(upload.purpose()).isEqualTo("thu nghiem");
      assertThat(upload.sourceFile().name()).isEqualTo("phieu.json");
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(VALID.getBytes(UTF_8));
      assertThat(upload.sourceFile().sha256()).isEqualTo(HexFormat.of().formatHex(digest));
    }

    @Test
    @DisplayName("không khai mode thì mặc định HASH")
    void missingModeDefaultsToHash() {
      assertThat(parse(noop(), validDoc()).columns()).containsExactly(new ColumnSpec("msisdn", Mode.HASH));
    }

    @Test
    @DisplayName("gộp cột trùng không phân biệt hoa thường, giữ cột xuất hiện đầu")
    void duplicateColumnsAreMerged() {
      DeidUpload upload = parse(noop(),
          validDoc("columns", "[{\"column\":\"MSISDN\"},{\"column\":\"imei\"},{\"column\":\"msisdn\"}]"));

      assertThat(upload.columns()).extracting(ColumnSpec::column).containsExactly("msisdn", "imei");
    }

    @Test
    @DisplayName("giới hạn 100 cột tính trên số phần tử trong file; đúng 100 được chấp nhận")
    void exactlyOneHundredColumnsAreAccepted() {
      assertThat(parse(noop(), validDoc("columns", columns(100))).columns()).hasSize(100);
    }

    @Test
    @DisplayName("purpose đúng 500 ký tự được chấp nhận")
    void purposeOfFiveHundredCharactersIsAccepted() {
      assertThat(parse(noop(), validDoc("purpose", "\"" + "x".repeat(500) + "\"")).purpose()).hasSize(500);
    }

    @Test
    @DisplayName("khi submitter không phải airflow, dagId được phép khai nhưng bị bỏ qua (kể cả giá trị lạ)")
    void dagIdIsIgnoredWhenNotAirflow() {
      assertThat(parse(noop(), validDoc("dagId", "\"a/b\"")).dagId()).isNull();
      assertThat(parse(noop(), validDoc("dagId", "123")).dagId()).isNull();
      assertThat(parse(noop(), validDoc("dagId", null)).dagId()).isNull();
    }

    @Test
    @DisplayName("dagId nằm trong danh sách cho phép được chấp nhận, phân biệt hoa thường")
    void dagIdInAllowlistIsAccepted() {
      assertThat(parse(airflow("deid_a", "Deid.B"), validDoc("dagId", "\"Deid.B\"")).dagId()).isEqualTo("Deid.B");
    }

    @Test
    @DisplayName("file đúng bằng giới hạn kích thước được chấp nhận")
    void fileOfExactlyMaxBytesIsAccepted() {
      String base = validDoc();
      String padded = base + " ".repeat(300 - base.length());

      assertThat(padded.getBytes(UTF_8)).hasSize(300);
      assertThat(parse(withMaxBytes(300), padded).table()).isEqualTo("users_cdr");
    }
  }

  @Nested
  @DisplayName("cả file hỏng (problems rỗng)")
  class WholeFileRejected {

    private void assertWholeFileRejected(DeidProperties deid, String json) {
      assertThat(rejected(deid, json).problems()).isEmpty();
    }

    @Test
    @DisplayName("không phải JSON, JSON cụt, file rỗng")
    void notJson() {
      assertWholeFileRejected(noop(), "not json at all");
      assertWholeFileRejected(noop(), "{\"database\":");
      assertWholeFileRejected(noop(), "");
      assertWholeFileRejected(noop(), "   \n ");
    }

    @Test
    @DisplayName("gốc không phải object")
    void rootIsNotAnObject() {
      assertWholeFileRejected(noop(), "[]");
      assertWholeFileRejected(noop(), "\"text\"");
      assertWholeFileRejected(noop(), "42");
      assertWholeFileRejected(noop(), "null");
    }

    @Test
    @DisplayName("dữ liệu thừa sau JSON")
    void trailingTokens() {
      assertWholeFileRejected(noop(), validDoc() + " {}");
      assertWholeFileRejected(noop(), validDoc() + "x");
    }

    @Test
    @DisplayName("khoá JSON trùng (kể cả lồng trong cột)")
    void duplicateKeys() {
      assertWholeFileRejected(noop(), "{\"database\":\"a\",\"database\":\"b\",\"table\":\"t\"}");
      assertWholeFileRejected(noop(), validDoc("columns", "[{\"column\":\"a\",\"column\":\"b\"}]"));
    }

    @Test
    @DisplayName("lồng quá sâu")
    void nestedTooDeep() {
      assertWholeFileRejected(noop(), "{\"database\":" + "[".repeat(50) + "]".repeat(50) + "}");
    }

    @Test
    @DisplayName("không phải UTF-8 hợp lệ")
    void invalidUtf8() {
      byte[] bytes = {'{', '"', (byte) 0xFF, '"', ':', '1', '}'};

      Throwable thrown = catchThrowable(() -> new DeidUploadParser(noop()).parse("f.json", new ByteArrayInputStream(bytes)));

      assertThat(thrown).isInstanceOf(DakApiException.class);
      assertThat(((DakApiException) thrown).error()).isEqualTo("invalid_file");
      assertThat(((DakApiException) thrown).problems()).isEmpty();
    }

    @Test
    @DisplayName("quá giới hạn kích thước chỉ một byte")
    void oneByteOverTheLimit() {
      String base = validDoc();
      String padded = base + " ".repeat(301 - base.length());

      assertThat(padded.getBytes(UTF_8)).hasSize(301);
      assertWholeFileRejected(withMaxBytes(300), padded);
    }

    @Test
    @DisplayName("nguồn vô tận: chỉ đọc tối đa max+1 byte rồi từ chối")
    void readsAtMostMaxPlusOneByte() {
      AtomicLong consumed = new AtomicLong();
      InputStream endless = new InputStream() {
        @Override
        public int read() {
          consumed.incrementAndGet();
          return ' ';
        }

        @Override
        public int read(byte[] b, int off, int len) {
          Arrays.fill(b, off, off + len, (byte) ' ');
          consumed.addAndGet(len);
          return len;
        }
      };

      Throwable thrown = catchThrowable(() -> new DeidUploadParser(withMaxBytes(300)).parse("f.json", endless));

      assertThat(thrown).isInstanceOf(DakApiException.class);
      assertThat(consumed.get()).isLessThanOrEqualTo(301);
    }

    @Test
    @DisplayName("lỗi đọc luồng thành invalid_file, không lộ thông điệp của IOException")
    void readFailure() {
      InputStream broken = new InputStream() {
        @Override
        public int read() throws IOException {
          throw new IOException("disk /secret/path exploded");
        }
      };

      Throwable thrown = catchThrowable(() -> new DeidUploadParser(noop()).parse("f.json", broken));

      assertThat(thrown).isInstanceOf(DakApiException.class);
      DakApiException e = (DakApiException) thrown;
      assertThat(e.error()).isEqualTo("invalid_file");
      assertThat(e.getMessage()).doesNotContain("secret");
      assertThat(e.internalReason()).doesNotContain("secret");
    }

    @Test
    @DisplayName("thông điệp lỗi không lặp lại nội dung file")
    void messageDoesNotEchoContent() {
      DakApiException e = rejected(noop(), "{\"database\":\"SECRET-VALUE\"");

      assertThat(e.getMessage()).doesNotContain("SECRET");
      assertThat(e.internalReason()).doesNotContain("SECRET");
    }
  }

  @Nested
  @DisplayName("lỗi nội dung, gom một lượt")
  class ContentProblems {

    @Test
    @DisplayName("database thiếu, sai ký tự, quá dài hoặc không phải chuỗi")
    void database() {
      assertProblems(rejected(noop(), validDoc("database", null)), "database", "invalid_table");
      assertProblems(rejected(noop(), validDoc("database", "\"demo-db\"")), "database", "invalid_table");
      assertProblems(rejected(noop(), validDoc("database", "\"" + "a".repeat(129) + "\"")), "database", "invalid_table");
      assertProblems(rejected(noop(), validDoc("database", "\"\"")), "database", "invalid_table");
      assertProblems(rejected(noop(), validDoc("database", "123")), "database", "invalid_table");
      assertProblems(rejected(noop(), validDoc("database", "null")), "database", "invalid_table");
    }

    @Test
    @DisplayName("table thiếu, sai ký tự hoặc không phải chuỗi")
    void table() {
      assertProblems(rejected(noop(), validDoc("table", null)), "table", "invalid_table");
      assertProblems(rejected(noop(), validDoc("table", "\"a b\"")), "table", "invalid_table");
      assertProblems(rejected(noop(), validDoc("table", "[\"t\"]")), "table", "invalid_table");
    }

    @Test
    @DisplayName("columns thiếu, không phải mảng, rỗng hoặc quá 100 phần tử")
    void columnsArray() {
      assertProblems(rejected(noop(), validDoc("columns", null)), "columns", "invalid_column");
      assertProblems(rejected(noop(), validDoc("columns", "\"msisdn\"")), "columns", "invalid_column");
      assertProblems(rejected(noop(), validDoc("columns", "{}")), "columns", "invalid_column");
      assertProblems(rejected(noop(), validDoc("columns", "[]")), "columns", "invalid_column");
      assertProblems(rejected(noop(), validDoc("columns", columns(101))), "columns", "invalid_column");
    }

    @Test
    @DisplayName("phần tử cột không phải object, thiếu tên, tên sai hoặc không phải chuỗi")
    void columnElements() {
      assertProblems(rejected(noop(), validDoc("columns", "[\"msisdn\"]")), "columns[0]", "invalid_column");
      assertProblems(rejected(noop(), validDoc("columns", "[{\"mode\":\"HASH\"}]")), "columns[0].column", "invalid_column");
      assertProblems(rejected(noop(), validDoc("columns", "[{\"column\":\"a b\"}]")), "columns[0].column", "invalid_column");
      assertProblems(rejected(noop(), validDoc("columns", "[{\"column\":7}]")), "columns[0].column", "invalid_column");
      assertProblems(rejected(noop(), validDoc("columns", "[{\"column\":\"ok\"},{\"column\":\"a-b\"}]")),
          "columns[1].column", "invalid_column");
    }

    @Test
    @DisplayName("mode không hợp lệ hoặc ngoài allowed-modes bị từ chối")
    void mode() {
      for (String bad : List.of("\"ENCRYPT\"", "\"MAPPING\"", "\"FOO\"", "5", "\"\"")) {
        assertProblems(rejected(noop(), validDoc("columns", "[{\"column\":\"a\",\"mode\":" + bad + "}]")),
            "columns[0].mode", "invalid_mode");
      }
    }

    @Test
    @DisplayName("mode null được coi là không khai, tức HASH")
    void nullModeIsTreatedAsMissing() {
      assertThat(parse(noop(), validDoc("columns", "[{\"column\":\"a\",\"mode\":null}]")).columns())
          .containsExactly(new ColumnSpec("a", Mode.HASH));
    }

    @Test
    @DisplayName("trường lạ trong phần tử cột bị từ chối")
    void unknownFieldInsideColumn() {
      assertProblems(rejected(noop(), validDoc("columns", "[{\"column\":\"a\"},{\"column\":\"b\",\"extra\":1}]")),
          "columns[1].extra", "invalid_file");
    }

    @Test
    @DisplayName("purpose thiếu, rỗng sau strip, quá 500 ký tự hoặc không phải chuỗi")
    void purpose() {
      assertProblems(rejected(noop(), validDoc("purpose", null)), "purpose", "invalid_purpose");
      assertProblems(rejected(noop(), validDoc("purpose", "\"   \"")), "purpose", "invalid_purpose");
      assertProblems(rejected(noop(), validDoc("purpose", "\"" + "x".repeat(501) + "\"")), "purpose", "invalid_purpose");
      assertProblems(rejected(noop(), validDoc("purpose", "12")), "purpose", "invalid_purpose");
    }

    @Test
    @DisplayName("trường lạ ở gốc bị từ chối, không bỏ qua im lặng")
    void unknownTopLevelField() {
      assertProblems(rejected(noop(), validDoc("extra", "1")), "extra", "invalid_file");
    }

    @Test
    @DisplayName("tên trường lạ được làm sạch trước khi đưa vào thông điệp (không ký tự điều khiển, tối đa 64 ký tự)")
    void unknownFieldNameIsSanitised() {
      DakApiException e = rejected(noop(), validDoc("ex\\ntra\\u0000" + "y".repeat(100), "1"));

      assertThat(e.problems()).hasSize(1);
      assertThat(e.problems().get(0).field()).matches("[A-Za-z0-9_.\\-]{1,64}");
      assertThat(e.problems().get(0).message()).doesNotContain("\n");
    }

    @Test
    @DisplayName("dagId thiếu, không phải chuỗi, ký tự lạ hoặc ngoài danh sách khi submitter là airflow")
    void dagIdWhenAirflow() {
      DeidProperties airflow = airflow("deid_a", "deid_b");
      assertProblems(rejected(airflow, validDoc("dagId", null)), "dagId", "invalid_dag");
      assertProblems(rejected(airflow, validDoc("dagId", "5")), "dagId", "invalid_dag");
      assertProblems(rejected(airflow, validDoc("dagId", "\"../x\"")), "dagId", "invalid_dag");
      assertProblems(rejected(airflow, validDoc("dagId", "\"a/b\"")), "dagId", "invalid_dag");
      assertProblems(rejected(airflow, validDoc("dagId", "\"a?b\"")), "dagId", "invalid_dag");
      assertProblems(rejected(airflow, validDoc("dagId", "\"DEID_A\"")), "dagId", "invalid_dag");
      assertProblems(rejected(airflow, validDoc("dagId", "\"other_dag\"")), "dagId", "invalid_dag");
    }

    @Test
    @DisplayName("mọi dagId bị từ chối dùng cùng một thông điệp (không dò được DAG nào tồn tại)")
    void disallowedDagIdsShareOneMessage() {
      DeidProperties airflow = airflow("deid_a");

      String notAllowed = rejected(airflow, validDoc("dagId", "\"deid_exists_on_airflow\"")).problems().get(0).message();
      String badFormat = rejected(airflow, validDoc("dagId", "\"../etc\"")).problems().get(0).message();
      String wrongType = rejected(airflow, validDoc("dagId", "7")).problems().get(0).message();

      assertThat(notAllowed).isEqualTo(badFormat).isEqualTo(wrongType);
      assertThat(notAllowed).doesNotContain("deid_exists_on_airflow").doesNotContain("etc");
    }

    @Test
    @DisplayName("nhiều lỗi cùng lúc được gom đủ, đúng thứ tự trường, kèm số lượng trong thông điệp")
    void collectsEveryProblemInOrder() {
      String json = doc("database", "\"bad db\"", "table", "\"users_cdr\"",
          "columns", "[{\"column\":\"ok\"},{\"column\":\"no good\"},{\"column\":\"x\",\"mode\":\"MAPPING\"}]",
          "dagId", "\"nope\"", "purpose", "\"\"", "zzz", "1", "aaa", "2");

      DakApiException e = rejected(airflow("deid_a"), json);

      assertThat(e.problems()).extracting(Problem::field, Problem::code).containsExactly(
          tuple("database", "invalid_table"),
          tuple("columns[1].column", "invalid_column"),
          tuple("columns[2].mode", "invalid_mode"),
          tuple("dagId", "invalid_dag"),
          tuple("purpose", "invalid_purpose"),
          tuple("zzz", "invalid_file"),
          tuple("aaa", "invalid_file"));
      assertThat(e.getMessage()).isEqualTo("The uploaded request has 7 problems");
    }

    @Test
    @DisplayName("đúng một lỗi thì thông điệp dùng số ít")
    void singularMessage() {
      assertThat(rejected(noop(), validDoc("purpose", null)).getMessage()).isEqualTo("The uploaded request has 1 problem");
    }

    @Test
    @DisplayName("mọi thông điệp lỗi là chuỗi cố định, không lặp lại giá trị người dùng nhập")
    void problemMessagesDoNotEchoValues() {
      DakApiException e = rejected(airflow("deid_a"), doc("database", "\"leak db!\"", "table", "\"leak tbl!\"",
          "columns", "[{\"column\":\"leak col!\",\"mode\":\"leakmode\"}]", "dagId", "\"leak/dag\"",
          "purpose", "\"" + "leak purpose ".repeat(80) + "\""));

      // database, table, tên cột và mode của cùng một phần tử cột (2 lỗi), dagId, purpose.
      assertThat(e.problems()).hasSize(6);
      for (Problem p : e.problems()) {
        assertThat(p.message()).doesNotContainIgnoringCase("leak");
      }
      assertThat(e.getMessage()).doesNotContainIgnoringCase("leak");
    }
  }

  @Nested
  @DisplayName("làm sạch tên file")
  class FileName {

    @Test
    @DisplayName("bỏ phần đường dẫn kiểu Unix và Windows")
    void stripsPaths() {
      assertThat(DeidUploadParser.sanitizeFileName("../../etc/passwd")).isEqualTo("passwd");
      assertThat(DeidUploadParser.sanitizeFileName("C:\\Users\\x\\phieu.json")).isEqualTo("phieu.json");
    }

    @Test
    @DisplayName("bỏ ký tự điều khiển")
    void stripsControlCharacters() {
      assertThat(DeidUploadParser.sanitizeFileName("a\nb\u0000c\r.json")).isEqualTo("abc.json");
    }

    @Test
    @DisplayName("bỏ ký tự định dạng như bidi override (dùng để giả mạo đuôi file khi hiển thị)")
    void stripsFormatCharacters() {
      assertThat(DeidUploadParser.sanitizeFileName("phieu‮fdp.json")).isEqualTo("phieufdp.json");
      assertThat(DeidUploadParser.sanitizeFileName("a​b.json")).isEqualTo("ab.json");
    }

    @Test
    @DisplayName("cắt còn tối đa 255 ký tự")
    void truncates() {
      assertThat(DeidUploadParser.sanitizeFileName("x".repeat(400))).hasSize(255);
    }

    @Test
    @DisplayName("tên trống, chỉ có dấu chấm hoặc null thì dùng tên mặc định")
    void fallsBackToDefaultName() {
      assertThat(DeidUploadParser.sanitizeFileName(null)).isEqualTo("unnamed");
      assertThat(DeidUploadParser.sanitizeFileName("")).isEqualTo("unnamed");
      assertThat(DeidUploadParser.sanitizeFileName("   ")).isEqualTo("unnamed");
      assertThat(DeidUploadParser.sanitizeFileName("..")).isEqualTo("unnamed");
      assertThat(DeidUploadParser.sanitizeFileName(".")).isEqualTo("unnamed");
      assertThat(DeidUploadParser.sanitizeFileName("dir/")).isEqualTo("unnamed");
    }

    @Test
    @DisplayName("parse ghi tên file đã làm sạch vào sourceFile")
    void parseUsesSanitisedName() {
      DeidUpload upload = new DeidUploadParser(noop()).parse("..\\..\\evil\n.json", stream(validDoc()));

      assertThat(upload.sourceFile().name()).isEqualTo("evil.json");
    }
  }

  @Nested
  @DisplayName("trường env (env của job do người gửi phiếu nhập)")
  class EnvField {

    @Test
    @DisplayName("không khai, null hoặc object rỗng: env rỗng")
    void absentNullOrEmpty_isEmpty() {
      assertThat(parse(noop(), validDoc()).env()).isEmpty();
      assertThat(parse(noop(), validDoc("env", "null")).env()).isEmpty();
      assertThat(parse(noop(), validDoc("env", "{}")).env()).isEmpty();
    }

    @Test
    @DisplayName("env hợp lệ được nhận nguyên giá trị và sắp theo tên; env không còn bị coi là trường lạ")
    void validEnv_isKept() {
      DeidUpload upload = parse(noop(), validDoc("env",
          "{\"OUTPUT_TABLE\":\"users_pseudo\",\"OUTPUT_DB\":\"deid_out\",\"OUTPUT_MODE\":\"append\"}"));

      assertThat(upload.env()).containsExactly(Map.entry("OUTPUT_DB", "deid_out"), Map.entry("OUTPUT_MODE", "append"),
          Map.entry("OUTPUT_TABLE", "users_pseudo"));
    }

    @Test
    @DisplayName("env không phải object: lỗi ở trường env")
    void notAnObject_rejected() {
      for (String bad : List.of("[]", "\"x\"", "7", "true")) {
        assertProblems(rejected(noop(), validDoc("env", bad)), "env", "invalid_env");
      }
    }

    @Test
    @DisplayName("tên lạ hoặc env bị cấm: cùng thông điệp chung, field là env.<TÊN>")
    void unknownOrForbiddenName_rejected() {
      DakApiException e = rejected(noop(), validDoc("env", "{\"FOO\":\"x\",\"DB_NAME\":\"other\"}"));

      assertThat(e.problems()).extracting(Problem::field, Problem::code, Problem::message).containsExactly(
          tuple("env.DB_NAME", "invalid_env", "env name is not allowed"),
          tuple("env.FOO", "invalid_env", "env name is not allowed"));
    }

    @Test
    @DisplayName("giá trị sai quy tắc hoặc không phải chuỗi: lỗi đúng trường, thông điệp không lặp lại giá trị")
    void badValues_rejectedWithoutEchoingThem() {
      DakApiException e = rejected(noop(), validDoc("env", "{\"OUTPUT_MODE\":\"replace-all\",\"OUTPUT_FORMAT\":7}"));

      assertThat(e.problems()).extracting(Problem::field, Problem::message).containsExactly(
          tuple("env.OUTPUT_FORMAT", "value must be a string"),
          tuple("env.OUTPUT_MODE", "value must match overwrite|append"));
      assertThat(e.problems().toString()).doesNotContain("replace-all");
    }

    @Test
    @DisplayName("lỗi env được gom cùng lỗi khác của file trong một lượt")
    void envErrors_areCollectedWithOtherProblems() {
      DakApiException e = rejected(noop(), validDoc("table", "\"bad-table\"", "env", "{\"OUTPUT_DB\":\"A B\"}",
          "purpose", "\"\""));

      assertThat(e.problems()).extracting(Problem::field).containsExactlyInAnyOrder("table", "env.OUTPUT_DB", "purpose");
    }

    @Test
    @DisplayName("OUTPUT_TABLE trùng bảng nguồn bị từ chối (cùng database khai rõ hoặc database đích mặc định)")
    void outputSameAsSource_rejected() {
      DakApiException explicit = rejected(noop(), validDoc("env", "{\"OUTPUT_DB\":\"demo_db\",\"OUTPUT_TABLE\":\"users_cdr\"}"));
      DakApiException implicit = rejected(noop(), validDoc("env", "{\"OUTPUT_TABLE\":\"users_cdr\"}"));

      assertThat(explicit.problems()).extracting(Problem::field, Problem::message)
          .containsExactly(tuple("env.OUTPUT_TABLE", "the output table must differ from the source table"));
      assertProblems(implicit, "env.OUTPUT_TABLE", "invalid_env");
    }

    @Test
    @DisplayName("cùng tên bảng ở database khác hợp lệ, kể cả database đích lấy từ cấu hình")
    void sameTableNameInAnotherDatabase_allowed() {
      DeidProperties configured = new DeidProperties(true, null, 20, 65536, "analytics", null, null, null, null, null);

      assertThat(parse(noop(), validDoc("env", "{\"OUTPUT_DB\":\"other_db\",\"OUTPUT_TABLE\":\"users_cdr\"}")).env())
          .containsEntry("OUTPUT_DB", "other_db");
      assertThat(parse(configured, validDoc("env", "{\"OUTPUT_TABLE\":\"users_cdr\"}")).env())
          .containsEntry("OUTPUT_TABLE", "users_cdr");
    }

    @Test
    @DisplayName("quá số mục tối đa của cấu hình bị từ chối")
    void tooManyEntries_rejected() {
      DeidProperties two = new DeidProperties(true, null, 20, 65536, null, null, null, null, null, null,
          new DeidProperties.JobEnv(2, DeidProperties.defaults().jobEnv().allowed()));

      DakApiException e = rejected(two, validDoc("env",
          "{\"OUTPUT_DB\":\"a\",\"OUTPUT_TABLE\":\"b\",\"OUTPUT_MODE\":\"append\"}"));

      assertProblems(e, "env", "invalid_env");
    }

    @Test
    @DisplayName("khoá env trùng trong JSON bị từ chối như mọi khoá trùng khác")
    void duplicateEnvKey_rejectedAsWholeFile() {
      rejected(noop(), validDoc("env", "{\"OUTPUT_DB\":\"a\",\"OUTPUT_DB\":\"b\"}"));
    }
  }
}
