# Tài liệu khử định danh (giả danh hoá)

Hiện thực mục 2.3 "Giả danh hoá" của tài liệu khử định danh: thay số thuê bao / IMEI / mã khách hàng bằng định danh giả
mà vẫn liên kết được bản ghi của cùng một chủ thể.

| Tài liệu | Dành cho ai / khi nào đọc |
| --- | --- |
| [`ALGORITHMS.md`](./ALGORITHMS.md) | **Đọc đầu tiên.** Giải thích cơ chế 3 thuật toán (HASH, ENCRYPT, MAPPING) bằng lời thường và ví dụ thật; kẻ tấn công thử gì; chọn chế độ nào |
| [`LIB_PLAN.md`](./LIB_PLAN.md) | Thiết kế lib `pseudonym-udf`: đối chiếu tài liệu gốc, quyết định thiết kế, cấu trúc package, quản lý khoá, rủi ro |

Code và cách chạy:
- **App khử định danh một bảng** (DB + bảng + cột → bảng đích; bảng env của lib): [`pseudonymize-table-app/README.md`](../../pseudonymize-table-app/README.md)
- Lib: [`pseudonym-udf/`](../../pseudonym-udf)
- Utils kiểm tra bảng đã khử định danh (nguồn ↔ đích): [`pseudonym-verify-utils/README.md`](../../pseudonym-verify-utils/README.md)
