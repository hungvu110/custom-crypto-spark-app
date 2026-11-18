# Trả lời đội dak — key global cho job chuẩn hoá silver

Ngày: 2026-11-18. Gửi: đội dak. Từ: đội SA.

Trả lời hai câu hỏi cuối [YEU-CAU-TU-DAK-KEY-GLOBAL.md](YEU-CAU-TU-DAK-KEY-GLOBAL.md). SA đồng ý mục 1 (Variable), mục 2 (endpoint) và mục 3 (luồng khử định danh không đổi).

## 1. Câu trả lời

| # | Câu hỏi | Trả lời |
|---|---|---|
| 1 | Job silver lấy token bằng client nào? | Client **nội bộ** chung của SA (cùng client đang dùng cho `by-path` và `by-column`, gửi `tag=internal` ở hai endpoint đó). Job lấy key global bằng đúng client này, nên rơi vào nhánh "client nội bộ đang dùng cho by-path / by-column" ở mục 2. Job **không** dùng client của team |
| 2 | Bao giờ DAG silver đọc được `globalKey`/`globalZone`? | **Từ lúc bản DAG mới lên Airflow: <ngày triển khai — SA điền khi triển khai>.** SA sẽ báo dak khi xong. DAG **cũ** không chết lúc đọc Variable nhưng cũng **không chạy**: nó coi hai khoá là khoá lạ và bỏ cả cấu hình (DAG mất `schedule`, task `validate_conf` báo lỗi). Vì vậy dak chỉ cho người dùng tick "Vùng dữ liệu Global?" **sau khi** SA báo |

## 2. Những điều dak nên biết thêm

1. **Hai khoá đi cùng nhau.** DAG chỉ nhận `globalKey` (đúng `true`) **cùng** `globalZone` (`[a-z0-9_]{1,64}`) **cùng** `encryptColumns` không rỗng. Thiếu một trong ba, hoặc `"globalKey": false`, thì DAG bỏ cả cấu hình và không chạy. dak không gửi `false` nên trường hợp này không xảy ra nếu đúng hợp đồng.
2. **Chỉ dataset có cột mã hoá** (hiện là `nims`) nhận hai khoá này. Dataset passthrough (`ebs_3g_4g`, `f001-mpre-call`) mà Variable có hai khoá này thì bị coi là khoá lạ. dak cần chặn từ lúc gửi phiếu.
3. **Giải mã file (`by-path`) không đổi.** Phiếu chọn key global vẫn gọi `by-path` để giải mã cả file như trước; chỉ việc mã hoá `encryptColumns` đổi sang key global. Job **không** gọi `by-column` cho phiếu này, nên cũng không còn bước chờ duyệt YC (`PENDING`).
4. **Cách job dùng key.** Job gọi `GET /api/v1/keys/global?zone=<vùng>` **một lần** ở driver (không gửi `tag`), giữ key trong bộ nhớ và mã hoá mọi cột `encryptColumns` bằng cùng một key. Job ghi log mã `requestId` của DAK, không ghi key. `keyVersion` chỉ để ghi log: job chấp nhận số, chuỗi số hoặc `null`.
5. **Lỗi và cách job xử lý.** `403 access_denied`, `403 key_disabled`, `401` (sau khi xin token mới một lần) và `429`: job dừng với lỗi (exit 1), không ghi gì vào bảng, không chờ. Với `access_denied` job báo "chưa có phiếu hiệu lực của vùng hoặc vùng không tồn tại" vì dak trả cùng câu cho hai trường hợp. Chạy lại sau khi dak xử lý.
6. **Thứ tự việc cần xong trước khi chạy:** (a) admin dak tạo key global của vùng; (b) phiếu silver chọn vùng được duyệt (`SCHEDULED`); (c) DAG mới đã lên Airflow. Nếu (a) chưa xong, job nhận `403 access_denied`.

## 3. Để test trước khi dak có bản thật

`dak-mock` đã có endpoint này và phiếu chọn vùng: xem [dak-mock/README.md](../../dak-mock/README.md) mục 2.3a. Có thể chạy cả luồng: tạo key vùng (`POST /mock-admin/global-keys`) → gửi phiếu `nims` có `globalZone` → duyệt → DAG đọc Variable → job lấy key.
