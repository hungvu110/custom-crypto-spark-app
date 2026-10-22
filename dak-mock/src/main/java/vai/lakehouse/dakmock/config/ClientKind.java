package vai.lakehouse.dakmock.config;

/**
 * Loại client đăng ký với mock. {@code TEAM}: client của một team, lấy key theo grant. {@code INTERNAL}: client riêng của luồng
 * nội bộ {@code by-column} (gửi {@code tag=internal}), không thuộc team nào và không cần grant.
 */
public enum ClientKind {
  TEAM,
  INTERNAL
}
