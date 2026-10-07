package vai.lakehouse.dakmock.auth;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.nimbusds.jwt.JWTParser;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Xác minh access token theo docs/DAK_API_SPEC.md mục 4:
 * <ol>
 *   <li>Đọc {@code iss} (CHƯA tin) chỉ để tra realm trong registry — không bao giờ dựng URL từ token.</li>
 *   <li>Verify chữ ký bằng JWKS lấy từ {@code jwksUri} của registry, chỉ RS256 (header {@code jku}/{@code x5u} bị bỏ qua).</li>
 *   <li>Kiểm tra {@code exp}/{@code iat} (lệch đồng hồ cho phép), {@code iss}, {@code typ=Bearer}, {@code azp}.</li>
 *   <li>Tra {@code (realm, azp)} trong danh sách client đã đăng ký, mỗi request (khoá client có hiệu lực ngay).</li>
 * </ol>
 */
@Component
public class TokenVerifier {

  /** Danh tính bên gọi sau khi xác minh. */
  public record Caller(String realm, String clientId, String tenant, String workspace, String team) {}

  private record RealmEntry(DakProperties.Realm realm, NimbusJwtDecoder decoder) {}

  private final Map<String, RealmEntry> realmsByIssuer;
  private final Map<String, DakProperties.Client> clientsByKey;

  public TokenVerifier(DakProperties props) {
    this.realmsByIssuer = props.realms().stream()
        .collect(Collectors.toUnmodifiableMap(DakProperties.Realm::issuer,
            r -> new RealmEntry(r, decoder(r, props.clockSkew()))));
    this.clientsByKey = props.clients().stream()
        .collect(Collectors.toUnmodifiableMap(c -> key(c.realm(), c.clientId()), Function.identity()));
  }

  public Caller verify(String authorizationHeader) {
    String token = bearerToken(authorizationHeader);
    RealmEntry entry = realmsByIssuer.get(unverifiedIssuer(token));
    if (entry == null) {
      throw DakApiException.invalidToken("issuer is not in the realm registry");
    }

    Jwt jwt;
    try {
      jwt = entry.decoder().decode(token);
    } catch (BadJwtException e) {
      throw DakApiException.invalidToken("token rejected: " + e.getMessage());
    } catch (JwtException e) {
      // Không tải được JWKS (Keycloak không phản hồi, chưa có khoá trong cache): lỗi hạ tầng, không phải lỗi token.
      throw DakApiException.upstream("cannot load signing keys of realm " + entry.realm().realm() + ": " + e.getMessage());
    }

    String azp = jwt.getClaimAsString("azp");
    String clientId = jwt.getClaimAsString("client_id");
    if (clientId != null && !clientId.equals(azp)) {
      throw DakApiException.invalidToken("client_id does not match azp");
    }
    DakProperties.Client client = clientsByKey.get(key(entry.realm().realm(), azp));
    if (client == null || !client.enabled()) {
      throw DakApiException.invalidToken("client " + azp + " is not registered or is disabled in realm "
          + entry.realm().realm());
    }
    return new Caller(entry.realm().realm(), azp, entry.realm().tenant(), client.workspace(), client.team());
  }

  private static String bearerToken(String header) {
    if (header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7) || header.substring(7).isBlank()) {
      throw DakApiException.invalidToken("missing bearer token");
    }
    return header.substring(7).trim();
  }

  private static String unverifiedIssuer(String token) {
    try {
      String iss = JWTParser.parse(token).getJWTClaimsSet().getIssuer();
      if (iss == null) {
        throw DakApiException.invalidToken("token has no iss");
      }
      return iss;
    } catch (ParseException e) {
      throw DakApiException.invalidToken("token is not a JWT");
    }
  }

  private static NimbusJwtDecoder decoder(DakProperties.Realm realm, Duration skew) {
    // Khoá chỉ lấy từ jwksUri của registry; Nimbus cache JWKS và tải lại khi gặp kid lạ (Keycloak đổi khoá ký).
    NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(realm.jwksUri())
        .jwsAlgorithm(SignatureAlgorithm.RS256)
        .build();
    List<OAuth2TokenValidator<Jwt>> validators = List.of(
        new JwtTimestampValidator(skew),
        new JwtIssuerValidator(realm.issuer()),
        new JwtClaimValidator<Instant>(JwtClaimNames.EXP, Objects::nonNull),
        new JwtClaimValidator<Instant>(JwtClaimNames.IAT, iat -> iat == null || !iat.isAfter(Instant.now().plus(skew))),
        new JwtClaimValidator<String>("typ", "Bearer"::equals),
        new JwtClaimValidator<String>("azp", azp -> azp != null && !azp.isBlank()));
    decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(validators));
    return decoder;
  }

  private static String key(String realm, String clientId) {
    return realm + "\u0000" + clientId;
  }
}
