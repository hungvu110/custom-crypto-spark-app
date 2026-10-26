package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;

/** Tài khoản giả của portal (plan mục 4.8): tra theo token, so sánh chính xác, không lộ token. */
class PortalUsersTest {

  private static final String TOKEN_ALICE = "alice-token-0123456789abcd";
  private static final String TOKEN_BOB = "bob---token-0123456789abcd";

  private static PortalUsers users() {
    return new PortalUsers(deid(true, new PortalUser("alice", "team-a", null, null, TOKEN_ALICE),
        new PortalUser("bob", "team-b", null, null, TOKEN_BOB)));
  }

  private static DeidProperties deid(boolean enabled, PortalUser... users) {
    return new DeidProperties(enabled, null, 20, 65536, null, null, null, null, null, List.of(users));
  }

  @Test
  @DisplayName("mỗi token xác thực đúng tài khoản của nó, kèm team")
  void authenticate_returnsTheMatchingUser() {
    PortalUsers users = users();

    assertThat(users.authenticate(TOKEN_ALICE)).hasValueSatisfying(u -> {
      assertThat(u.username()).isEqualTo("alice");
      assertThat(u.team()).isEqualTo("team-a");
    });
    assertThat(users.authenticate(TOKEN_BOB)).hasValueSatisfying(u -> assertThat(u.team()).isEqualTo("team-b"));
  }

  @Test
  @DisplayName("token sai, thiếu, rỗng, thừa hoặc thiếu một ký tự, khác hoa thường đều bị từ chối")
  void authenticate_rejectsEverythingElse() {
    PortalUsers users = users();

    assertThat(users.authenticate(null)).isEmpty();
    assertThat(users.authenticate("")).isEmpty();
    assertThat(users.authenticate("   ")).isEmpty();
    assertThat(users.authenticate("nope")).isEmpty();
    assertThat(users.authenticate(TOKEN_ALICE + " ")).isEmpty();
    assertThat(users.authenticate(" " + TOKEN_ALICE)).isEmpty();
    assertThat(users.authenticate(TOKEN_ALICE + "x")).isEmpty();
    assertThat(users.authenticate(TOKEN_ALICE.substring(1))).isEmpty();
    assertThat(users.authenticate(TOKEN_ALICE.toUpperCase())).isEmpty();
    assertThat(users.authenticate("a".repeat(100_000))).isEmpty();
  }

  @Test
  @DisplayName("tiền tố chung của hai token không đủ để xác thực")
  void authenticate_commonPrefixIsNotEnough() {
    PortalUsers users = new PortalUsers(deid(true, new PortalUser("alice", "team-a", null, null, "p".repeat(23) + "1"),
        new PortalUser("bob", "team-b", null, null, "p".repeat(23) + "2")));

    assertThat(users.authenticate("p".repeat(23) + "1")).hasValueSatisfying(u -> assertThat(u.username()).isEqualTo("alice"));
    assertThat(users.authenticate("p".repeat(23) + "2")).hasValueSatisfying(u -> assertThat(u.username()).isEqualTo("bob"));
    assertThat(users.authenticate("p".repeat(23) + "3")).isEmpty();
    assertThat(users.authenticate("p".repeat(23))).isEmpty();
  }

  @Test
  @DisplayName("portal chỉ bật khi flow bật và có tài khoản")
  void enabled_requiresFlowAndUsers() {
    assertThat(users().enabled()).isTrue();
    assertThat(new PortalUsers(deid(true)).enabled()).isFalse();
    assertThat(new PortalUsers(deid(false, new PortalUser("alice", "team-a", null, null, TOKEN_ALICE))).enabled()).isFalse();
  }

  @Test
  @DisplayName("khi flow tắt thì không xác thực ai, kể cả token đúng")
  void authenticate_whenDisabled_returnsNothing() {
    PortalUsers off = new PortalUsers(deid(false, new PortalUser("alice", "team-a", null, null, TOKEN_ALICE)));

    assertThat(off.authenticate(TOKEN_ALICE)).isEmpty();
  }

  @Test
  @DisplayName("toString không chứa token")
  void toString_doesNotLeakTokens() {
    assertThat(users().toString()).doesNotContain(TOKEN_ALICE).doesNotContain(TOKEN_BOB);
  }
}
