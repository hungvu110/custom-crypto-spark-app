# key-prefix-lib

Lấy key cho các lib crypto (`column-crypto-lib`, `cdr-crypto-udf`, `pseudonym-udf`). Chỉ **driver** gọi; executor nhận key qua plan
hoặc closure của UDF. Cách cấu hình (sql-engine, SparkApplication): [docs/key-access/KEY_PREFIX_SOURCES.md](../docs/key-access/KEY_PREFIX_SOURCES.md).

Ba loại nguồn key:

| Loại             | API                                                         | Nguồn                                    | Factory                  |
| ---------------- | ----------------------------------------------------------- | ---------------------------------------- | ------------------------ |
| Key cả **bảng**  | `PrefixSource.read(datasetName): String`                    | `file`, `vault`, `file,vault`, `dak`     | `PrefixSourceFactory`    |
| Key theo **cột** | `ColumnKeySource.readColumns(datasetName, columns, intent)` | chỉ `dak` (`GET /api/v1/keys/by-column`) | `ColumnKeySourceFactory` |
| Key theo **đường dẫn** | `PathKeySource.readPath(path): PathKeyResult`         | chỉ `dak` (`GET /api/v1/keys/by-path`)   | `PathKeySourceFactory`   |

Hợp đồng `by-path` và `by-column`: [docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md](../docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md) (bản
mới nhất của DAK). `readPath` nhận đường dẫn **tương đối theo gốc của nguồn** và gửi ở dạng chuẩn `PathKeySource.canonical` (không `/` đầu/cuối,
không `//`, giữ hoa thường; `relativeTo(path, root)` lấy phần sau gốc) vì DAK so khớp nguyên chuỗi
([TRA-LOI-DAK-BY-PATH.md](../docs/key-access/TRA-LOI-DAK-BY-PATH.md)); trả `Available(PathKey)` khi DAK có key (`200`) và `NotAvailable(requestCode, state)` khi DAK không có key (`204` rỗng, không mở YC — đã thống
nhất với DAK) — không phải lỗi. `202` (cách cũ: DAK mở YC) là lỗi, để không đọc file có thể đã mã hoá như plaintext. Dùng bởi [silver-ingest-app](../silver-ingest-app/README.md) (nguồn VTNET khi file có mã hoá).

## Cấu trúc thư mục

Source chia thư mục theo chức năng, nhưng **mọi class vẫn ở package `vai.lakehouse.keyprefix`** (Scala không bắt buộc thư mục khớp
package). Nhờ vậy các module khác và tài liệu import `vai.lakehouse.keyprefix.X` không đổi khi file di chuyển giữa các thư mục. Thư mục
`src/test` phản chiếu đúng cấu trúc này.

```text
src/main/scala/vai/lakehouse/keyprefix/
├── config/    cách đọc cấu hình
├── table/     key cả bảng
├── column/    key theo cột
├── dak/       phần DAK dùng chung cho cả hai loại key
└── http/      gửi HTTP
```

| Thư mục   | File                                   | Nội dung                                                                                           |
| --------- | -------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `config/` | `ConfigSource.scala`                   | `ConfigSource` (tên logic → tên vật lý), `SparkConfSource`, `ChainedConfigSource`                  |
|           | `EnvConfigSource.scala`                | Ánh xạ tên logic → biến môi trường (`CRYPTO_*`, `VAULT_*`, `DAK_*`)                                 |
|           | `Settings.scala`                       | Đọc/kiểm từng giá trị: bắt buộc, URL `https`, pattern, `cacheTtlSeconds`. Lỗi không in lại giá trị |
| `table/`  | `PrefixSource.scala`                   | `PrefixSource`, `FilePrefixSource`, `ChainedPrefixSource`, `CachedPrefixSource`                    |
|           | `PrefixSourceFactory.scala`            | `Keys` (mọi tên logic), dựng nguồn theo `source`, cấm ghép `dak` với nguồn khác                    |
|           | `PrefixFiles.scala`                    | Đọc file prefix, chặn path traversal                                                               |
|           | `VaultPrefixSource.scala`              | Vault KV v2 (Kubernetes auth hoặc token tĩnh)                                                      |
|           | `DakPrefixSource.scala`                | `GET /api/v1/keys/{database}/{table}`                                                              |
| `column/` | `ColumnKeySource.scala`                | `ColumnKeySource`, `KeyIntent` (`onMissing`), `CachedColumnKeySource`                              |
|           | `ColumnKeysUnavailableException.scala` | Lỗi khi có cột không `READY`, kèm gợi ý theo `state`                                               |
|           | `ColumnKeySourceFactory.scala`         | Dựng nguồn key cột, chỉ `source=dak`                                                               |
|           | `DakColumnKeySource.scala`             | `GET /api/v1/keys/by-column`: chia lô, parse, "tất cả hoặc không"                                  |
|           | `ColumnBatches.scala`                  | Chia lô 100 cột / 4096 ký tự (tính cả dấu phẩy)                                                    |
|           | `WaitingColumnKeySource.scala`         | Chờ admin DAK duyệt yêu cầu key (cột `PENDING`) bằng exponential backoff; `PendingWait` (`dak.pendingWait.*`, mặc định tắt) |
| `path/`   | `PathKeySource.scala`                  | `PathKeySource` (+ `canonical`, `relativeTo`), `PathKey` (che key khi in), `PathKeyResult`          |
|           | `DakPathKeySource.scala`               | `GET /api/v1/keys/by-path`: kiểm đường dẫn, `200` → key, `204` → không có key, `202` → lỗi        |
|           | `PathKeySourceFactory.scala`           | Dựng nguồn key theo đường dẫn, chỉ `source=dak`, không cache                                       |
| `dak/`    | `DakConfig.scala`                      | `DakConfig`, `DakSettings` (dựng cấu hình + token provider từ `dak.*`)                             |
|           | `DakNames.scala`                       | `DakTableRef`, `DakColumnRef`: kiểm và chuẩn hoá chữ thường tên gửi lên DAK                        |
|           | `DakClient.scala`                      | GET có Bearer, `X-Request-Id`, `X-Dak-Caller`; 401 thì xin token mới và thử lại 1 lần               |
|           | `KeycloakTokenProvider.scala`          | Token `client_credentials`, cache tới gần hết hạn, dùng chung trong tiến trình                     |
| `http/`   | `JsonHttp.scala`                       | JDK `HttpClient` + Jackson; lỗi mạng không lộ token/body                                           |

Thư mục test: `config/`, `table/`, `column/`, `path/`, `dak/` như trên, và `support/` (`StubHttpServer`: Keycloak/Vault/DAK giả bằng HTTP
server cục bộ).

Quy ước chung: thông báo lỗi không bao giờ chứa token, secret, key hay body thô của server; chỉ mã lỗi, `requestId` và tên
bảng/cột.

## Thêm một nguồn mới

1. Cài đặt `PrefixSource` (key bảng, vào `table/`) hoặc `ColumnKeySource` (key cột, vào `column/`).
2. Thêm tên logic của cấu hình vào `PrefixSourceFactory.Keys` và tên biến môi trường vào `EnvConfigSource`.
3. Đọc cấu hình bằng `Settings` để có cùng cách báo lỗi.
4. Nối vào `PrefixSourceFactory.build` hoặc `ColumnKeySourceFactory.create`.

## Build và test

```bash
mvn -pl key-prefix-lib test          # Keycloak, Vault, DAK giả bằng HTTP server cục bộ (StubHttpServer)
mvn -pl key-prefix-lib -amd test     # kèm mọi module dùng lib này
```

Test của `by-column` dùng đúng các body mà [dak-mock](../dak-mock/README.md) trả về.
