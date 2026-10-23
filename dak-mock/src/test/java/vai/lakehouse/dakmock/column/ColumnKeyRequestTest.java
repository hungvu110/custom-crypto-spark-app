package vai.lakehouse.dakmock.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.web.DakApiException;

class ColumnKeyRequestTest {

  @Test
  void normalizesToLowerCaseAndMergesDuplicatesKeepingFirstOrder() {
    ColumnKeyRequest req = ColumnKeyRequest.parse("Demo_DB", "Users_CDR", "IMEI,msisdn,MSISDN,imei", "request", "internal");

    assertThat(req.database()).isEqualTo("demo_db");
    assertThat(req.table()).isEqualTo("users_cdr");
    assertThat(req.columns()).containsExactly("imei", "msisdn");
    assertThat(req.onMissing()).isEqualTo(OnMissing.REQUEST);
    assertThat(req.internalTag()).isTrue();
  }

  @Test
  void onMissingDefaultsToNoneAndTagIsOptional() {
    ColumnKeyRequest req = ColumnKeyRequest.parse("demo_db", "users_cdr", "msisdn", null, null);

    assertThat(req.onMissing()).isEqualTo(OnMissing.NONE);
    assertThat(req.internalTag()).isFalse();
  }

  @Test
  void rejectsInvalidTableNames() {
    assertError(() -> ColumnKeyRequest.parse("demo-db", "users_cdr", "msisdn", null, null), "invalid_table");
    assertError(() -> ColumnKeyRequest.parse("demo_db", null, "msisdn", null, null), "invalid_table");
  }

  @Test
  void rejectsMissingEmptyOrInvalidColumns() {
    assertError(() -> ColumnKeyRequest.parse("demo_db", "users_cdr", null, null, null), "invalid_column");
    assertError(() -> ColumnKeyRequest.parse("demo_db", "users_cdr", " ", null, null), "invalid_column");
    assertError(() -> ColumnKeyRequest.parse("demo_db", "users_cdr", "a,,b", null, null), "invalid_column");
    assertError(() -> ColumnKeyRequest.parse("demo_db", "users_cdr", "start-time", null, null), "invalid_column");
  }

  @Test
  void acceptsOneHundredColumnsAndRejectsMoreAfterMerging() {
    String hundred = names(100, "c");
    assertThat(ColumnKeyRequest.parse("demo_db", "t", hundred, null, null).columns()).hasSize(100);
    // 101 tên nhưng có một tên trùng -> sau gộp còn 100, vẫn hợp lệ.
    assertThat(ColumnKeyRequest.parse("demo_db", "t", hundred + ",C0", null, null).columns()).hasSize(100);

    assertThatThrownBy(() -> ColumnKeyRequest.parse("demo_db", "t", names(101, "c"), null, null))
        .isInstanceOfSatisfying(DakApiException.class, e -> {
          assertThat(e.error()).isEqualTo("invalid_column");
          assertThat(e.getMessage()).contains("100 columns");
        });
  }

  @Test
  void rejectsColumnListLongerThan4096Characters() {
    // 40 tên 120 ký tự + 39 dấu phẩy = 4839 ký tự.
    String longNames = IntStream.range(0, 40)
        .mapToObj(i -> String.format("%03d", i) + "x".repeat(117))
        .collect(Collectors.joining(","));

    assertThatThrownBy(() -> ColumnKeyRequest.parse("demo_db", "t", longNames, null, null))
        .isInstanceOfSatisfying(DakApiException.class, e -> {
          assertThat(e.error()).isEqualTo("invalid_column");
          assertThat(e.getMessage()).contains("4096 characters");
        });
  }

  @Test
  void rejectsUnknownOnMissingAndTagValues() {
    assertError(() -> ColumnKeyRequest.parse("demo_db", "t", "a", "always", null), "invalid_on_missing");
    assertError(() -> ColumnKeyRequest.parse("demo_db", "t", "a", null, "external"), "invalid_tag");
    assertError(() -> ColumnKeyRequest.parse("demo_db", "t", "a", null, ""), "invalid_tag");
  }

  private static String names(int n, String prefix) {
    return IntStream.range(0, n).mapToObj(i -> prefix + i).collect(Collectors.joining(","));
  }

  private static void assertError(Runnable call, String error) {
    assertThatThrownBy(call::run)
        .isInstanceOfSatisfying(DakApiException.class, e -> assertThat(e.error()).isEqualTo(error));
  }
}
