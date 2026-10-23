package vai.lakehouse.dakmock.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.web.DakApiException;

class YcRegistryTest {

  private static final ColumnId MSISDN = new ColumnId("demo_db", "users_cdr", "msisdn");
  private static final ColumnId IMEI = new ColumnId("demo_db", "users_cdr", "imei");

  private final YcRegistry registry =
      new YcRegistry(Clock.fixed(Instant.parse("2026-10-22T00:00:00Z"), ZoneOffset.UTC));

  @Test
  void opensOneYcPerColumnAndReturnsTheSameOneOnRetry() {
    YcRegistry.Yc first = registry.open("internal", MSISDN, 50).orElseThrow();
    YcRegistry.Yc again = registry.open("internal", MSISDN, 50).orElseThrow();
    YcRegistry.Yc otherClient = registry.open("internal-2", MSISDN, 50).orElseThrow();

    assertThat(first.requestCode()).isEqualTo("YC-2026-000001");
    assertThat(first.state()).isEqualTo(YcRegistry.State.OPEN);
    assertThat(again.requestCode()).isEqualTo(first.requestCode());
    assertThat(otherClient.requestCode()).isEqualTo(first.requestCode());
    assertThat(registry.list()).hasSize(1);
  }

  @Test
  void limitsOpenYcPerClientAndTable() {
    assertThat(registry.open("internal", MSISDN, 1)).isPresent();
    assertThat(registry.open("internal", IMEI, 1)).isEmpty();
    // Giới hạn theo (client, database, table): bảng khác và client khác không bị ảnh hưởng.
    assertThat(registry.open("internal", new ColumnId("demo_db", "other", "imei"), 1)).isPresent();
    assertThat(registry.open("internal-2", IMEI, 1)).isPresent();
  }

  @Test
  void rejectedYcStaysForTheColumnRegardlessOfClient() {
    YcRegistry.Yc yc = registry.open("internal", MSISDN, 50).orElseThrow();
    registry.reject(yc.requestCode(), "not a sensitive column");

    YcRegistry.Yc afterRotate = registry.open("internal-2", MSISDN, 50).orElseThrow();

    assertThat(afterRotate.state()).isEqualTo(YcRegistry.State.REJECTED);
    assertThat(afterRotate.requestCode()).isEqualTo(yc.requestCode());
    assertThat(afterRotate.reasonText()).isEqualTo("not a sensitive column");
    assertThat(registry.list()).hasSize(1);
  }

  @Test
  void rejectedYcDoesNotCountTowardsTheOpenLimit() {
    YcRegistry.Yc yc = registry.open("internal", MSISDN, 1).orElseThrow();
    registry.reject(yc.requestCode(), "no");

    assertThat(registry.open("internal", IMEI, 1)).isPresent();
  }

  @Test
  void removingAFulfilledYcFreesTheSlot() {
    YcRegistry.Yc yc = registry.open("internal", MSISDN, 1).orElseThrow();

    assertThat(registry.remove(yc.requestCode()).column()).isEqualTo(MSISDN);
    assertThat(registry.find(MSISDN)).isEmpty();
    assertThat(registry.open("internal", IMEI, 1)).isPresent();
  }

  @Test
  void unknownCodesAreNotFound() {
    assertThatThrownBy(() -> registry.reject("YC-2026-999999", "x"))
        .isInstanceOfSatisfying(DakApiException.class, e -> assertThat(e.error()).isEqualTo("not_found"));
    assertThatThrownBy(() -> registry.remove("YC-2026-999999"))
        .isInstanceOfSatisfying(DakApiException.class, e -> assertThat(e.error()).isEqualTo("not_found"));
  }
}
