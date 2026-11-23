# Đề nghị đội dak: cho phép phiếu khai bảng không có cột khử định danh (`pseudoColumns=none`)

Ngày: 2026-11-23. Gửi: đội dak. Từ: đội SA.

## 1. Vì sao

Phiếu khai thác dữ liệu VTT của team DS có **18 bảng**. Trong đó 9 bảng danh mục (`d_cust_type`, `d_khdn`, `d_os_type`, …) **không có cột nào cần khử định danh**,
nhưng DS vẫn muốn thấy đủ 18 bảng trong cùng một `OUTPUT_DB` của phiếu. Hiện job và DAG khử định danh **từ chối** `pseudoColumns` rỗng, nên các bảng này chưa khai được.

## 2. Đề nghị với dak

1. **Cho phép phiếu khai một bảng mà không chọn cột khử định danh nào.**
2. Với bảng đó, khi trigger DAG, gửi `conf.pseudoColumns` = đúng chuỗi **`none`** (chữ thường). **Không** để trống, không bỏ trường.
3. Mọi thứ khác **giữ nguyên**: `dag_run_id` (`deid-<năm>-<6 số>[-r<n>][-t<n>]`), `OUTPUT_DB`, `OUTPUT_TABLE`, `INPUT_FROM/TO`, credential. Vẫn một bảng một run.

Ví dụ `conf` của một bảng không có cột:

```json
{
  "requestCode": "REQ-2026-000123",
  "database": "vtt",
  "table": "d_os_type",
  "pseudoColumns": "none",
  "env": { "OUTPUT_DB": "…", "OUTPUT_TABLE": "d_os_type", "REQUEST_CODE": "REQ-2026-000123", "DAK_CLIENT_ID": "…", "DAK_CLIENT_SECRET": "…" }
}
```

Quy ước `none`:

| Giá trị `pseudoColumns` | Kết quả |
| --- | --- |
| `isdn:HASH,customer_id:HASH` | Như hiện tại |
| `none` | Chép bảng nguồn sang bảng đích nguyên trạng, không khử cột nào |
| rỗng, thiếu, hoặc `none` lẫn cột khác (`isdn:HASH,none`) | **Lỗi**, run failed ngay. Cố ý như vậy: quên đặt cột cho bảng nhạy cảm thì không được chép bảng gốc ra |

## 3. SA sẽ làm

- Job chấp nhận `PSEUDO_COLUMNS=none`: ghi bảng đích giống hệt bảng nguồn (cùng cột, kiểu, dữ liệu, partition), in WARN trong log.
- DAG `deid_pseudonymize_table` nhận `pseudoColumns=none`.
- **SA báo khi DAG mới đã lên Airflow. dak chỉ gửi `none` sau khi SA báo**, như đã làm với hậu tố `-t<n>`.

## 4. Lưu ý và câu hỏi cho dak

1. **Run `none` không gọi `GET /api/v1/keys/deid`.** Không có cột thì không cần khóa. dak sẽ **không thấy** lời gọi lấy khóa (và dòng audit tương ứng) cho các run này.
   Nếu dak cần audit hoặc cần cấp khóa cho mọi bảng của phiếu thì báo SA.
2. Phiếu có cho khai bảng không có cột khử định danh không, hay hiện đang bắt ít nhất một cột? Nếu đang bắt thì cần dak sửa kiểm tra này.
3. Quyền đọc và xóa vùng khi hết hạn (`deid_maintenance`) **không đổi**: bảng `none` xử lý như các bảng khác của phiếu.
4. Cả phiếu vẫn dùng chung một key cho các bảng có cột khử (xem [TRA-LOI-DAK-DEID-NHIEU-BANG.md](TRA-LOI-DAK-DEID-NHIEU-BANG.md) mục 5); bảng `none` không ảnh hưởng điều này.

Đề nghị dak trả lời mục 4 (câu 1 và 2) trước khi cập nhật.
