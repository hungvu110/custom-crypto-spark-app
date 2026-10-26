package vai.lakehouse.dakmock.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import vai.lakehouse.dakmock.config.DakProperties;

/**
 * Kiểm token của endpoint quản trị {@code /mock-admin/**}, dùng chung cho endpoint YC cũ và endpoint phiếu khử định danh.
 * Hành vi giữ nguyên như {@code MockAdminController} trước khi tách: chưa bật, thiếu hoặc sai token đều {@code 404}.
 */
class MockAdminAuthTest {

  private static final String TOKEN = "admin-token-0123456789abcdef";

  private static void assertHidden(Throwable thrown) {
    assertThat(thrown).isInstanceOfSatisfying(DakApiException.class, e -> {
      assertThat(e.status()).isEqualTo(HttpStatus.NOT_FOUND);
      assertThat(e.error()).isEqualTo("not_found");
      assertThat(e.getMessage()).isEqualTo("Unknown endpoint");
    });
  }

  @Test
  @DisplayName("token đúng thì qua")
  void correctToken_passes() {
    assertThatCode(() -> new MockAdminAuth(new DakProperties.Admin(TOKEN)).authorize(TOKEN)).doesNotThrowAnyException();
  }

  @Test
  @DisplayName("thiếu, rỗng, sai, thừa hoặc thiếu ký tự, khác hoa thường đều 404 'Unknown endpoint'")
  void wrongTokens_areHidden() {
    MockAdminAuth auth = new MockAdminAuth(new DakProperties.Admin(TOKEN));

    for (String bad : new String[] {null, "", " ", "nope", TOKEN + "x", TOKEN.substring(1), TOKEN.toUpperCase(), " " + TOKEN,
        "a".repeat(100_000)}) {
      assertHidden(catchThrowable(() -> auth.authorize(bad)));
    }
  }

  @Test
  @DisplayName("chưa cấu hình token quản trị thì mọi lời gọi 404, kể cả token bất kỳ")
  void disabled_isAlwaysHidden() {
    MockAdminAuth disabled = new MockAdminAuth(new DakProperties.Admin(null));

    assertHidden(catchThrowable(() -> disabled.authorize(TOKEN)));
    assertHidden(catchThrowable(() -> disabled.authorize(null)));
    assertHidden(catchThrowable(() -> new MockAdminAuth(new DakProperties.Admin("  ")).authorize("  ")));
  }

  @Test
  @DisplayName("thông điệp lỗi không lặp lại token đã gửi")
  void message_neverEchoesTheToken() {
    Throwable thrown = catchThrowable(() -> new MockAdminAuth(new DakProperties.Admin(TOKEN)).authorize("presented-secret"));

    assertThat(thrown.getMessage()).doesNotContain("presented-secret").doesNotContain(TOKEN);
  }
}
