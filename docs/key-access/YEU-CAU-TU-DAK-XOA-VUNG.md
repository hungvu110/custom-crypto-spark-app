# Yêu cầu từ đội dak: lệnh xoá vùng khai thác, và ba việc liên quan

Ngày: 2026-10-30. Gửi: đội SA / sql-engine.

Tài liệu này hỏi **bốn nhóm việc**. Nhóm 1 là thứ đang chặn chúng tôi viết code; ba nhóm sau nhỏ
hơn nhưng cần biết trước khi chốt thiết kế.

Mỗi câu hỏi có số. Các anh/chị trả lời thẳng dưới từng số là đủ, không cần viết lại tài liệu.

---

## 0. Bối cảnh: vì sao có yêu cầu này

Phiếu yêu cầu khai thác dữ liệu (phiếu dẫn tới job khử định danh, mô tả ở
[API_CONTRACT.md](API_CONTRACT.md)) đang được chúng tôi gộp vào luồng phiếu yêu cầu chung. Thay đổi
về nghiệp vụ, phần ảnh hưởng tới các anh/chị:

| | Trước | Sau |
|---|---|---|
| Thời hạn | Phiếu không có thời hạn | **Phiếu có thời hạn**, hết hạn thì người xin không được khai thác nữa |
| Khoảng ngày dữ liệu | Tuỳ chọn, nằm trong mục nâng cao | **Bắt buộc**: người xin phải khai dữ liệu từ ngày A tới ngày B |
| Gia hạn | Không có | Có — gia hạn **có thể** dẫn tới chạy lại job với khoảng ngày khác |

Việc mới nảy ra: **hết hạn thì vùng khai thác phải bị xoá đi.** Thu hồi quyền ở phía dak không đủ —
bảng đã khử định danh nằm ở vùng dữ liệu bên các anh/chị, dak không với tới được. Nên dak cần một
**lệnh xoá vùng** để gọi khi phiếu hết hạn.

Hiện [API_CONTRACT.md](API_CONTRACT.md) chỉ có đường **chạy** job khử định danh (mục 5.1), không có
đường **xoá** vùng. Đó là nhóm 1 dưới đây.

---

## 1. Lệnh xoá vùng khai thác

Việc dak muốn làm: một job của dak chạy **mỗi ngày một lần vào nửa đêm**, quét các phiếu đã hết
hạn, và với mỗi phiếu hết hạn thì yêu cầu các anh/chị **xoá bảng kết quả** mà job khử định danh đã
tạo ra cho phiếu đó.

dak đang giữ sẵn các thông tin này của mỗi phiếu: `OUTPUT_DB`, `OUTPUT_TABLE` (cặp database/bảng
kết quả, mục 2.2), `dagId`, `dag_run_id`, mã phiếu, và `database`/`table` nguồn.

**1.1** Lệnh xoá vùng là gì — một endpoint HTTP của các anh/chị, hay **một DAG Airflow khác** mà dak
trigger giống cách đang trigger job khử định danh (mục 5.1)? Nếu là DAG thì tên DAG là gì.

**1.2** Payload gồm những gì? Cụ thể: **cặp `OUTPUT_DB` + `OUTPUT_TABLE` của bảng kết quả có đủ
không**, hay còn cần thêm (`dagId`, mã phiếu, `database`/`table` nguồn, định dạng bảng)? Chúng tôi
muốn gửi đúng và đủ, không gửi thừa.

**1.3** Gọi **đồng bộ** (dak gọi, chờ, nhận kết quả xoá xong hay lỗi ngay trong lời gọi), hay
**trigger rồi hỏi trạng thái sau** như job khử định danh (dak trigger, rồi poll `GET .../dagRuns/...`
như mục 8)? Nếu là cách thứ hai, dak đọc trạng thái ở đâu.

**1.4** **Lỗi thì dak biết bằng cách nào?** Phân biệt được ba ca này không:
  - xoá thành công;
  - bảng không tồn tại (đã bị xoá trước đó, hoặc chưa bao giờ được tạo);
  - xoá thất bại thật (quyền, lỗi hạ tầng, bảng đang bị giữ).

Ca thứ hai và thứ ba **phải khác nhau** với dak: ca 2 là xong việc, ca 3 là phải thử lại và báo
động.

**1.5** **Gọi lại nhiều lần có an toàn không (idempotent)?** Ca thật sẽ xảy ra: nửa đêm dak gọi
xoá, mạng lỗi giữa đường, dak không biết đã xoá được hay chưa, hôm sau quét lại và gọi lần nữa.
Lần thứ hai trả về gì?

**1.6** Vùng đã bị **xoá bằng tay** từ trước (người của các anh/chị hoặc người xin tự xoá) thì lệnh
xoá của dak trả về gì — coi như thành công, hay báo lỗi "không tìm thấy"? Liên quan 1.4 ca 2.

**1.7** Xoá bảng kết quả có kéo theo gì khác không — ví dụ bảng đang được một view / job khác dùng,
hay có bản sao ở nơi khác mà lệnh này không với tới. Nếu có thứ gì **còn lại sau khi xoá**, dak cần
biết để nói đúng với người duyệt phiếu: hết hạn nghĩa là gì trên thực tế.

**1.8** Có giới hạn nhịp gọi không? dak gọi dồn một lúc lúc nửa đêm nếu nhiều phiếu hết hạn cùng
ngày. Cần giãn ra, hay gọi tuần tự là được?

---

## 2. Ba env khoảng ngày: môi trường nào đã mở?

[API_CONTRACT.md](API_CONTRACT.md) mục 2.2 ghi: ba env `INPUT_FROM`, `INPUT_TO`, `INPUT_HOURS` cần
bản job `pseudonymize-table-app` **có lọc theo cột `date` và `hour` trên nguồn là bảng**; bản job cũ
**báo lỗi cấu hình (exit 2)**. Tài liệu cũng ghi SA sẽ báo cho dak khi **từng môi trường** đã có job
mới và DAG mới, và dak chỉ mở ba env đó ở môi trường đã nhận thông báo.

Chúng tôi chưa nhận thông báo nào. Vì khoảng ngày A→B giờ thành **thông tin bắt buộc trên phiếu**,
chỗ này thành vấn đề thật: môi trường chưa mở cổng sẽ nhận phiếu hợp lệ ở dak, duyệt xong, trigger
job, rồi **job chết ở Spark với exit 2** — người xin không hiểu vì sao.

**2.1** Từng môi trường (dev/local, staging, production) hiện chạy **bản job nào** — bản có lọc
`date`/`hour`, hay bản cũ?

**2.2** Môi trường nào **chưa** có bản mới thì dự kiến khi nào có?

**2.3** Trong lúc chờ: dak nên **chặn ở phía dak** (không cho gửi phiếu ở môi trường chưa mở), hay
các anh/chị muốn job nhận env rồi bỏ qua thay vì exit 2? Chúng tôi nghiêng về chặn ở dak — hỏi để
chắc không làm trái ý các anh/chị.

---

## 3. Chạy lại job cho cùng một phiếu (gia hạn)

Nghiệp vụ mới: phiếu hết hạn, vùng bị xoá (nhóm 1), người xin **gia hạn** và khai một khoảng ngày
mới. Khi đó dak phải **chạy lại job khử định danh cho cùng một phiếu**, với khoảng ngày khác.

Vướng ở quy ước mã dag run. [API_CONTRACT.md](API_CONTRACT.md) mục 7.1 ghi: `dag_run_id` suy ra từ
mã phiếu (`deid-<năm>-<6 số>`), **cũng là tên SparkApplication**, và Airflow trả `409` thì dak hiểu
là "đã gửi rồi, dùng lại tham chiếu cũ". Quy ước đó giả định **một phiếu một lượt chạy** — giờ
không còn đúng.

**3.1** dak dự định thêm **hậu tố lượt chạy** vào mã: `deid-2026-000007-r2`, `-r3`, … cho lượt thứ
hai, thứ ba. Vẫn hợp DNS-1123, vẫn suy ra được từ mã phiếu. **Cách này có ổn với phía các anh/chị
không** (Airflow, tên SparkApplication, log, truy vết)?

**3.2** Nếu không ổn thì các anh/chị muốn phân biệt lượt chạy bằng cách nào?

**3.3** Chạy lại vào **cùng cặp `OUTPUT_DB`/`OUTPUT_TABLE` cũ** sau khi bảng đã bị xoá (nhóm 1) —
job tự tạo lại bảng bình thường, hay cần làm gì trước?

**3.4** Ca ngược: người xin gia hạn **trước khi hết hạn**, vùng **vẫn còn dữ liệu**. Phía dak đã
chốt: ca này **không chạy lại job**, chỉ đẩy ngày hết hạn ở dak, nên không có gì gửi xuống các
anh/chị. Nêu ra để các anh/chị biết là chúng tôi **không** chạy job lần hai lên một vùng đang có
dữ liệu.

---

## 4. Một điểm nhỏ trong tài liệu

**4.1** [API_CONTRACT.md](API_CONTRACT.md) mục 2.1 có ghi: *"Giao diện nên cho người dùng chọn
`dagId` từ danh sách được phép thay vì tự gõ (bản mock: `GET /mock-portal/deid-dags`)."*

Bản mock có đường đó, nhưng **bản thật thì chưa** — nên hiện giao diện dak phải để người dùng gõ
tay `dagId`, sai chính tả chỉ lộ ra sau khi admin đã duyệt. Có đường thật để dak lấy danh sách DAG
được phép không? Nếu không có thì dak sẽ tự cấu hình danh sách ở phía mình và bỏ ô nhập khỏi màn
người gửi — chỉ muốn chắc là không bỏ sót đường có sẵn.

---

## Tóm tắt: dak đang chờ gì

| Nhóm | Chặn việc gì ở dak | Mức |
|---|---|---|
| 1. Lệnh xoá vùng (1.1–1.8) | Viết job quét hạn — **không có cái này thì không code được** | Chặn |
| 2. Môi trường đã mở ba env (2.1–2.3) | Bật khoảng ngày thành bắt buộc | Chặn ở staging/production |
| 3. Mã lượt chạy khi gia hạn (3.1–3.3) | Chạy lại job lần hai | Cần trước khi code gia hạn |
| 4. Danh sách `dagId` (4.1) | Thiết kế màn gửi phiếu | Nhỏ |
