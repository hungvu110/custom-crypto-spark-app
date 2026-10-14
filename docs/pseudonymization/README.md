# Tài liệu khử định danh (giả danh hoá)

Hiện thực mục 2.3 "Giả danh hoá" của tài liệu khử định danh: thay số thuê bao / IMEI / mã khách hàng bằng định danh giả
mà vẫn liên kết được bản ghi của cùng một chủ thể.

| Tài liệu | Dành cho ai / khi nào đọc |
| --- | --- |
| [`ALGORITHMS.md`](./ALGORITHMS.md) | **Đọc đầu tiên.** Giải thích cơ chế 3 thuật toán (HASH, ENCRYPT, MAPPING) bằng lời thường và ví dụ thật; kẻ tấn công thử gì; chọn chế độ nào |
| [`LIB_PLAN.md`](./LIB_PLAN.md) | Thiết kế lib `pseudonym-udf`: đối chiếu tài liệu gốc, quyết định thiết kế, cấu trúc package, quản lý khoá, rủi ro |
| [`VERIFY_APP_PLAN.md`](./VERIFY_APP_PLAN.md) | Thiết kế app `pseudonym-verify-app`: định nghĩa đo được của "khử định danh thành công", bộ check U/P, mutation self-test |

Code và cách chạy:
- **App khử định danh một bảng** (DB + bảng + cột → bảng đích; bảng env của lib): [`pseudonymize-table-app/README.md`](../../pseudonymize-table-app/README.md)
- Lib: [`pseudonym-udf/`](../../pseudonym-udf)
- App kiểm chứng + chạy ở máy local / K8s: [`pseudonym-verify-app/README.md`](../../pseudonym-verify-app/README.md)
