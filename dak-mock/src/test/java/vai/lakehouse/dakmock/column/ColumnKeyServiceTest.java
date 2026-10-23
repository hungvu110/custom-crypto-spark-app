package vai.lakehouse.dakmock.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.auth.TokenVerifier.Caller;
import vai.lakehouse.dakmock.config.ClientKind;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;

class ColumnKeyServiceTest {

  private static final Caller INTERNAL = new Caller("tenant-a", "dak.internal", "tenant-a", null, null, ClientKind.INTERNAL);

  @Test
  void autoFulfillCreatesTheKeyImmediatelyInsteadOfOpeningAYc() {
    ColumnId id = new ColumnId("demo_db", "t", "a");
    ColumnKeyStore store = mock(ColumnKeyStore.class);
    when(store.read(any())).thenReturn(Optional.empty());
    when(store.create(eq(id), anyString())).thenReturn(new KeyMaterial("auto-key", 1));
    YcRegistry registry = new YcRegistry();
    ColumnKeyService service = new ColumnKeyService(props(true), store, registry);

    ColumnKeyResponse res = service.fetch(INTERNAL, ColumnKeyRequest.parse("demo_db", "t", "a", "request", "internal"), null);

    assertThat(res.complete()).isTrue();
    assertThat(res.columns().get(0).state()).isEqualTo(ColumnKeyResponse.State.READY);
    assertThat(res.columns().get(0).keyPrefix()).isEqualTo("auto-key");
    assertThat(registry.list()).isEmpty();
  }

  @Test
  void autoFulfillDoesNotCreateKeysWhenNotRequested() {
    ColumnKeyStore store = mock(ColumnKeyStore.class);
    when(store.read(any())).thenReturn(Optional.empty());
    ColumnKeyService service = new ColumnKeyService(props(true), store, new YcRegistry());

    ColumnKeyResponse res = service.fetch(INTERNAL, ColumnKeyRequest.parse("demo_db", "t", "a", "none", "internal"), null);

    assertThat(res.columns().get(0).state()).isEqualTo(ColumnKeyResponse.State.MISSING);
    assertThat(res.columns().get(0).reason()).isEqualTo(ColumnKeyResponse.NOT_REQUESTED);
  }

  private static DakProperties props(boolean autoFulfill) {
    DakProperties.Vault vault = new DakProperties.Vault("http://vault", "token", "kv", "base/{database}.{table}",
        "dak/column-keys/{database}/{table}/{column}", "keyPrefix", Duration.ofSeconds(2), Duration.ofSeconds(5));
    return new DakProperties(List.of(), List.of(), List.of(), vault, new DakProperties.Yc(50, autoFulfill),
        new DakProperties.Admin(null), Duration.ofSeconds(60));
  }
}
