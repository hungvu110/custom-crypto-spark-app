# VTNET EBS-4G: đọc landing ở HKH, ghi silver ở HLA

DAG `silver_ingest_vtnet_ebs_3g_4g` nạp landing EBS-4G (cụm HDFS **HKH**) vào `silver.vtnet_ebs_3g_4g` (HMS và HDFS của **HLA**). Job Spark chạy ở
namespace HLA (`vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy`) với keytab `presto` của HLA, như mọi bảng VTT, và **đọc HKH bằng một delegation token
của HKH** lấy bằng tay, đặt trong Secret `hkh-hdfs-token` của namespace đó. DAG chỉ có `validate_conf` rồi `ingest`.

Dữ liệu là CSV có dòng tiêu đề `imsi,lac,sac,eci,event_time,event_id,event_result`, nằm trong thư mục `dt=YYYYMMDD/batch=HHMM/`. Chỉ `imsi` được VTNET
mã hoá, bằng thư viện `PassTranformer`. Job giải mã `imsi`, rồi mã hoá lại bằng key global `silver_vtt` với cột hằng `key_scope=vtt`, giống hệt cách
làm với `imsi` của f022. Vì vậy hai bảng nối được với nhau theo `imsi`.

## 0. Định dạng `isdn`, `imsi` ở hai nguồn (đã kiểm trên mẫu thật, 2026-11-27)

DS chỉ nối VTT với VTNET qua `isdn` và `imsi`. Giải mã mẫu của từng bảng bằng đúng key và thuật toán của nguồn:

| Cột | VTT | VTNET | Xử lý ở silver |
| --- | --- | --- | --- |
| `imsi` | `f022_register`: 15 chữ số, `45204…` (bản rõ ở nguồn) | `ebs_3g_4g`, `isdn_imsi_mapping`: 15 chữ số, `45204…` | Không đổi: cùng định dạng |
| `isdn` | `f022_register`, `f_home_bts_daily_area`, `f016`: 9 chữ số, không mã nước | `isdn_imsi_mapping`: 11 chữ số, `84` + 9 chữ số | `isdn_imsi_mapping` chuẩn hoá `isdn:ISDN_NATIONAL` (bỏ `84`) sau giải mã, trước mã hoá |

Bản mã và định danh giả là của chuỗi giá trị: không chuẩn hoá thì cùng một số thuê bao ra hai bản mã khác nhau và không nối được.

## 1. Vì sao dùng token

HLA và HKH dùng **cùng tên realm** `VAILAKEHOUSE.VIETTEL.COM` nhưng có **hai KDC khác nhau** (KDC của HKH ở `10.208.217.245`). Một JVM chỉ đọc
được một `krb5.conf`, nên job chạy ở HLA không xin được vé Kerberos cho NameNode HKH. Delegation token không cần KDC: NameNode HKH tự kiểm token do
chính nó cấp.

Mã nguồn đã kiểm (Spark 3.5.1, Hadoop 3.3.4):

| Điểm | Ở đâu trong mã nguồn | Hệ quả |
|---|---|---|
| Executor tự nạp file token | `UserGroupInformation.createLoginUser` đọc biến `HADOOP_TOKEN_FILE_LOCATION` | DAG đặt biến này cho executor |
| Driver đăng nhập bằng keytab thì **bỏ qua** file token | `SparkSubmit` gọi `loginUserFromKeytab` | Job tự nạp file qua `INPUT_TOKEN_FILE` (`InputToken.install`) |
| Token do Spark gửi xuống executor được **thêm vào**, không thay thế | `SparkHadoopUtil.addDelegationTokens` | Token HKH vẫn còn khi Spark làm mới token HLA |
| Service của token là `host:port` (HKH đặt `use_ip=false`, Isilon SmartConnect đổi IP theo DNS) | `SecurityUtil.buildTokenService`, đọc `use_ip` trong khởi tạo tĩnh từ `core-site.xml` | `core-site.xml` của job ở HLA (`teamtscauiy-hadoop-conf`) cũng phải có `hadoop.security.token.service.use_ip=false`; đặt qua sparkConf không có tác dụng |
| Không được liệt kê HKH trong `spark.kerberos.access.hadoopFileSystems` | `HadoopFSDelegationTokenProvider` | Nếu liệt kê, Spark sẽ xin token HKH từ KDC của HLA và lỗi |

## 2. Lấy token bằng tay

Làm trong pod **`hdfs-kerberos-client-hkh`** ở namespace **`lakehouse`**. Pod này đã có cấu hình HKH và `krb5.conf` trỏ tới KDC `10.208.217.245`.
Secret thì tạo ở namespace của job Spark (`vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy`).

```bash
POD_NS=lakehouse; POD=hdfs-kerberos-client-hkh          # nơi lấy token
JOB_NS=vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy           # nơi job Spark chạy và đọc Secret
kubectl -n $POD_NS exec -it $POD -- bash
```

Các lệnh dưới đây chạy **trong pod**:

```bash
# 2.1 Đăng nhập KDC HKH bằng principal được ĐỌC /landing/ebs_3g_4g (vd k8s). Renewer = chính principal đó (tên ngắn), để renew/cancel được.
kinit -kt <đường dẫn keytab> k8s@VAILAKEHOUSE.VIETTEL.COM
klist                                   # phải có krbtgt/VAILAKEHOUSE.VIETTEL.COM, cấp bởi KDC HKH
hdfs getconf -confKey fs.defaultFS      # phải là hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020
hdfs dfs -ls /landing/ebs_3g_4g/dt=20261120 | head -3

# 2.2 Lấy token vào một file. Client của pod lấy luôn cả token KMS của HKH (10.221.149.66:9292) nếu cấu hình có key provider.
hdfs fetchdt --renewer k8s /tmp/hkh.token
hdfs fetchdt --print /tmp/hkh.token     # 2 token: vailakehouse.datalakehkh.viettel.com.vn:8020 (HDFS) và 10.221.149.66:9292 (KMS)

# 2.3 Renew (in cảnh báo "No TokenRenewer defined for token kind kms-dt": client này KHÔNG renew được token KMS, xem mục 4)
hdfs fetchdt --renew /tmp/hkh.token

# 2.4 Thử đọc CHỈ bằng token, không dùng vé Kerberos. -cat (không chỉ -ls) để thử cả đường dữ liệu (DataNode, privacy, KMS nếu có)
kdestroy
HADOOP_TOKEN_FILE_LOCATION=/tmp/hkh.token hdfs dfs -ls /landing/ebs_3g_4g/dt=20261120 | head -3
HADOOP_TOKEN_FILE_LOCATION=/tmp/hkh.token hdfs dfs -cat /landing/ebs_3g_4g/dt=20261120/batch=2340/ebs_3g_4g-231050.csv | head -2
```

## 3. Tạo Secret bằng tay

Chạy ở máy có `kubectl`. Dùng `exec` **không có `-t`**, để file nhị phân không bị đổi ký tự xuống dòng.

```bash
kubectl -n $POD_NS exec $POD -- cat /tmp/hkh.token > hkh.token
kubectl -n $JOB_NS create secret generic hkh-hdfs-token --from-file=hadoop.token=hkh.token --dry-run=client -o yaml | kubectl apply -f -
rm hkh.token
kubectl -n $JOB_NS get secret hkh-hdfs-token -o jsonpath='{.data.hadoop\.token}' | base64 -d | wc -c   # > 0
```

- Tên Secret và tên key (`hadoop.token`) **phải đúng như trên**: DAG đọc chúng từ `INPUT_CLUSTERS["hkh"]` trong `airflow/dags/silver_ingest_common.py`.
- Dùng lại lệnh `apply` này mỗi lần lấy token mới. Mỗi pod Spark mới khởi động đều đọc phiên bản Secret mới nhất.

## 4. Vận hành token

| Việc | Làm thế nào |
|---|---|
| Giữ token sống mỗi ngày | Trước giờ chạy của DAG: `kinit ...` rồi `hdfs fetchdt --renew /tmp/hkh.token` ở pod client. Renew **không đổi nội dung file** nên **không phải cập nhật Secret**. Mỗi lần renew lùi hạn thêm một renew-interval (mặc định 24 giờ) |
| Hết max-lifetime (mặc định 7 ngày từ lúc cấp) | Không renew được nữa: làm lại mục 2 và mục 3 (token mới, cập nhật Secret) |
| Pod client bị tạo lại, mất `/tmp/hkh.token` | Lấy lại file từ Secret: `kubectl -n $JOB_NS get secret hkh-hdfs-token -o jsonpath='{.data.hadoop\.token}' \| base64 -d > hkh.token` rồi `kubectl cp hkh.token $POD_NS/$POD:/tmp/hkh.token` |
| Thôi dùng token (hoặc token bị lộ) | `hdfs fetchdt --cancel /tmp/hkh.token`: token chết ngay lập tức |

**Đặt thời hạn token:** client không đặt được. Hạn do server HKH quyết định qua `dfs.namenode.delegation.token.renew-interval` và `max-lifetime`;
muốn đổi phải nhờ quản trị HKH/Isilon. Phía mình chỉ có hai việc: renew để kéo dài (tối đa tới max-lifetime) và cancel để chấm dứt sớm.

**Job tự kiểm token:** khi khởi động, driver log mục `0. INPUT TOKEN` gồm service, owner, lúc cấp và hạn tối đa của từng token.
- Không có token cho đúng cluster của `INPUT_PATH`, hoặc token đã quá max-lifetime: job **dừng với exit 2**.
- Token đã cấp hơn 20 giờ: job **cảnh báo**. Token có thể đã hết hạn nếu chưa được renew.
- Token đã hết hạn theo renew-interval mà vẫn được dùng: lỗi xuất hiện khi đọc, dạng `token (...) is expired`.

## 5. Phần còn lại phải chuẩn bị (một lần)

- **DAK**:
  - key by-path cho đường dẫn đúng chuỗi `ebs_3g_4g`, giá trị là `inputKey` mà VTNET dùng để mã hoá `imsi`;
  - phiếu chọn key global vùng `silver_vtt` (`encryptColumns` = `imsi`, `keyField` = `key_scope`);
  - phiếu f022 khai `encryptColumns` = `isdn, imsi`.
- **Image**: build lại có `viettelsecurity-2.1.jar` (xem [silver-ingest-app/Dockerfile](../../silver-ingest-app/Dockerfile)), rồi đổi `IMAGE`.
- **Mạng** từ pod ở namespace HLA tới NameNode HKH cổng 8020 và tới mọi node Isilon ở cổng dữ liệu.
- Nếu đọc bị `SaslException`: mức `privacy` của HKH khác HLA. Đặt `spark.hadoop.hadoop.rpc.protection` và
  `spark.hadoop.dfs.data.transfer.protection` thành danh sách, ví dụ `privacy,authentication`.
- Chạy thử với conf `{"date": "20261120", "fromHour": "23"}`. Sau đó kiểm tra phép nối:
  `SELECT count(*) FROM silver.vtnet_ebs_3g_4g e JOIN silver.vtt_f022_register r ON e.imsi = r.imsi WHERE e.dt = '20261120'`.
  Lưu ý f022 phải đã được nạp lại để `imsi` được mã hoá.
