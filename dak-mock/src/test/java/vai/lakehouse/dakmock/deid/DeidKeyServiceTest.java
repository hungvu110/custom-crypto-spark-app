package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import vai.lakehouse.dakmock.auth.TokenVerifier.Caller;
import vai.lakehouse.dakmock.config.ClientKind;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.key.VaultKeyReader;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;
import vai.lakehouse.dakmock.web.DakApiException;

/** {@code GET /api/v1/keys/deid}: key của phiếu khai thác đã duyệt, lấy theo team trong token (không còn Secret). */
class DeidKeyServiceTest {

  private static final Instant NOW = Instant.parse("2026-11-09T08:00:00Z");
  private static final Caller TEAM = new Caller("realm", "team-a-client", "tenant-a", "ws-a", "team-a", ClientKind.TEAM);
  private static final Caller OTHER_TEAM = new Caller("realm", "team-b-client", "tenant-a", "ws-a", "team-b", ClientKind.TEAM);
  private static final Caller INTERNAL = new Caller("realm", "internal", "tenant-a", null, null, ClientKind.INTERNAL);
  private static final KeyMaterial KEY = new KeyMaterial("9f2c4bAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", 3);

  private DeidRequestRegistry registry;
  private VaultKeyReader vault;
  private DeidKeyService service;

  @BeforeEach
  void setUp() {
    registry = new DeidRequestRegistry(10, Clock.fixed(NOW, ZoneOffset.UTC));
    vault = mock(VaultKeyReader.class);
    when(vault.read(anyString(), anyString(), anyString(), anyString())).thenReturn(KEY);
    service = new DeidKeyService(registry, vault, () -> List.of(new PortalUser("alice", "team-a", "tenant-a", "ws-a", "t")));
  }

  private DeidRequest approved(String team, String table, String... columns) {
    List<ColumnSpec> specs = java.util.Arrays.stream(columns).map(c -> new ColumnSpec(c, Mode.HASH)).toList();
    DeidRequest created = registry.create(new NewRequest(team, "user", "demo_db", table, specs, "deid_dag", "test",
        new SourceFile("f.json", "ab".repeat(32))));
    return registry.transition(created.requestCode(), Set.of(State.PENDING_APPROVAL),
        r -> r.approved("admin", NOW, "demo_db", "out_" + created.requestCode().toLowerCase().replace('-', '_')));
  }

  private static void assertApi(Throwable thrown, HttpStatus status, String error) {
    assertThat(thrown).isInstanceOfSatisfying(DakApiException.class, e -> {
      assertThat(e.status()).isEqualTo(status);
      assertThat(e.error()).isEqualTo(error);
    });
  }

  @Test
  void returnsTheKeyOfTheOnlyApprovedRequestOfTheTeam() {
    DeidRequest request = approved("team-a", "users", "name");

    DeidKeyService.DeidKeyResponse response = service.fetch(TEAM, "demo_db", "users", null);

    assertThat(response.requestCode()).isEqualTo(request.requestCode());
    assertThat(response.keyPrefix()).isEqualTo(KEY.keyPrefix());
    assertThat(response.keyVersion()).isEqualTo(3);
  }

  @Test
  void stillGivesTheKeyWhileTheJobOfTheRequestIsSubmittedOrRunning() {
    DeidRequest request = approved("team-a", "users", "name");
    registry.transition(request.requestCode(), Set.of(State.APPROVED),
        r -> r.submitted(new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-x"), 3L));

    assertThat(service.fetch(TEAM, "demo_db", "users", request.requestCode()).requestCode()).isEqualTo(request.requestCode());
  }

  @Test
  void deniesWhenThereIsNoApprovedRequestForTheTable() {
    assertThatThrownBy(() -> service.fetch(TEAM, "demo_db", "users", null)).satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
  }

  @Test
  void deniesAPendingRequest() {
    registry.create(new NewRequest("team-a", "user", "demo_db", "users", List.of(new ColumnSpec("name", Mode.HASH)),
        "deid_dag", "test", new SourceFile("f.json", "ab".repeat(32))));

    assertThatThrownBy(() -> service.fetch(TEAM, "demo_db", "users", null)).satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
  }

  @Test
  void deniesAFinishedRequestBecauseItsGrantIsOver() {
    DeidRequest request = approved("team-a", "users", "name");
    registry.transition(request.requestCode(), Set.of(State.APPROVED),
        r -> r.submitted(new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-x"), 3L));
    registry.transition(request.requestCode(), Set.of(State.JOB_SUBMITTED), r -> r.jobProgress(JobState.SUCCEEDED, NOW));

    assertThatThrownBy(() -> service.fetch(TEAM, "demo_db", "users", null)).satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
  }

  @Test
  void neverShowsTheRequestOfAnotherTeam() {
    approved("team-a", "users", "name");

    assertThatThrownBy(() -> service.fetch(OTHER_TEAM, "demo_db", "users", null)).satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
  }

  @Test
  void givesTheKeyToTheInternalClientForARequestOfAnyTeamAndReadsItInTheScopeOfTheOwnerTeam() {
    DeidRequest request = approved("team-a", "users", "name");

    DeidKeyService.DeidKeyResponse response = service.fetch(INTERNAL, "demo_db", "users", request.requestCode());

    assertThat(response.requestCode()).isEqualTo(request.requestCode());
    org.mockito.Mockito.verify(vault).read("tenant-a", "ws-a", "demo_db", "users");
  }

  @Test
  void theInternalClientStillNeedsAnApprovedRequestAndGetsNothingForAnUnknownCode() {
    DeidRequest request = approved("team-a", "users", "name");

    assertThatThrownBy(() -> service.fetch(INTERNAL, "demo_db", "orders", null)).satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
    assertThatThrownBy(() -> service.fetch(INTERNAL, "demo_db", "users", "REQ-2026-999999")).satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
    assertThat(service.fetch(INTERNAL, "demo_db", "users", request.requestCode())).isNotNull();
  }

  @Test
  void aTeamClientStillNeverSeesTheRequestOfAnotherTeamEvenWithItsCode() {
    DeidRequest request = approved("team-a", "users", "name");

    assertThatThrownBy(() -> service.fetch(OTHER_TEAM, "demo_db", "users", request.requestCode()))
        .satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
  }

  @Test
  void answers409WithTheCodesWhenSeveralRequestsMatchAndPicksNone() {
    DeidRequest first = approved("team-a", "users", "name");
    DeidRequest second = approved("team-a", "users", "phone");

    assertThatThrownBy(() -> service.fetch(TEAM, "demo_db", "users", null)).satisfies(e -> {
      assertApi(e, HttpStatus.CONFLICT, "ambiguous_request");
      assertThat(e.getMessage()).contains(first.requestCode(), second.requestCode());
    });
    assertThat(service.fetch(TEAM, "demo_db", "users", second.requestCode()).requestCode()).isEqualTo(second.requestCode());
  }

  @Test
  void deniesARequestCodeThatBelongsToAnotherTable() {
    DeidRequest other = approved("team-a", "orders", "name");
    approved("team-a", "users", "name");

    assertThatThrownBy(() -> service.fetch(TEAM, "demo_db", "users", other.requestCode()))
        .satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
  }

  @Test
  void deniesWhenTheKeyIsNotCreatedInVault() {
    approved("team-a", "users", "name");
    when(vault.read(anyString(), anyString(), anyString(), anyString())).thenThrow(new VaultKeyReader.KeyNotFoundException());

    assertThatThrownBy(() -> service.fetch(TEAM, "demo_db", "users", null)).satisfies(e -> assertApi(e, HttpStatus.FORBIDDEN, "access_denied"));
  }
}
