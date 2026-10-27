package vai.lakehouse.dakmock.path;

import vai.lakehouse.dakmock.column.ColumnKeyRequest;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Tham số đã kiểm của {@code GET /api/v1/keys/by-path} (docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md mục 2). Kiểm TRƯỚC khi
 * xác minh token. DAK lưu và so khớp NGUYÊN CHUỖI đường dẫn (mục 6): mock không chuẩn hoá, kể cả hoa thường.
 */
public record PathKeyRequest(String path) {

  public static final int MAX_PATH_LENGTH = 1024;

  public static PathKeyRequest parse(String path, String tag) {
    if (path == null || path.isBlank() || path.length() > MAX_PATH_LENGTH || path.contains("..")
        || path.chars().anyMatch(Character::isISOControl)) {
      throw DakApiException.invalidPath("path must be non-empty, at most " + MAX_PATH_LENGTH
          + " characters, without '..' or control characters");
    }
    if (!ColumnKeyRequest.INTERNAL_TAG.equals(tag)) {
      throw DakApiException.invalidTag("tag must be 'internal'");
    }
    return new PathKeyRequest(path);
  }
}
