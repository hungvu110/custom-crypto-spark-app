package vai.lakehouse.dakmock.global;

/** Thân response {@code 200} của {@code GET /api/v1/keys/global} (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md mục 2). */
public record GlobalKeyResponse(String zone, String keyPrefix, long keyVersion) {

  // Không bao giờ in keyPrefix ra log.
  @Override
  public String toString() {
    return "GlobalKeyResponse[zone=" + zone + ", keyPrefix=***, keyVersion=" + keyVersion + "]";
  }
}
