package vai.lakehouse.dakmock.path;

/** Thân response {@code 200} của {@code by-path} (docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md mục 2). Chưa có key thì {@code 204} rỗng. */
public final class PathKeyResponse {

  private PathKeyResponse() {}

  /** {@code 200}. */
  public record Found(String path, String keyPrefix, long keyVersion) {

    // Không bao giờ in keyPrefix ra log.
    @Override
    public String toString() {
      return "Found[path=" + path + ", keyPrefix=***, keyVersion=" + keyVersion + "]";
    }
  }
}
