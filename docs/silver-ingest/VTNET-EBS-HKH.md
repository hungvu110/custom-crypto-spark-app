# VTNET EBS-4G: đọc landing ở HKH, ghi silver ở HLA

DAG `silver_ingest_vtnet_ebs_3g_4g` nạp landing EBS-4G (cụm HDFS **HKH**) vào `silver.vtnet_ebs_3g_4g` (HMS và HDFS của **HLA**). Job Spark chạy ở
namespace HLA (`vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy`) với keytab `presto` của HLA, như mọi bảng VTT, và **đọc HKH bằng một delegation token
của HKH** lấy bằng tay, đặt trong Secret `hkh-hdfs-token` của namespace đó. DAG chỉ có `validate_conf` rồi `ingest`.

Dữ liệu là CSV có dòng tiêu đề `imsi,lac,sac,eci,event_time,event_id,event_result`, nằm trong thư mục `dt=YYYYMMDD/batch=HHMM/`. Chỉ `imsi` được VTNET
mã hoá, bằng thư viện `PassTranformer`. Job giải mã `imsi`, rồi mã hoá lại bằng key global `silver_vtt` với cột hằng `key_scope=vtt`, giống hệt cách
làm với `imsi` của f022. Vì vậy hai bảng nối được với nhau theo `imsi`.

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
| `hdfs fetchdt` chỉ lấy token HDFS, không lấy token KMS | `DelegationTokenFetcher`: `fs.getDelegationToken` | Nếu landing nằm trong encryption zone thì cần thêm token KMS (xem bước 2.3) |
| Không được liệt kê HKH trong `spark.kerberos.access.hadoopFileSystems` | `HadoopFSDelegationTokenProvider` | Nếu liệt kê, Spark sẽ xin token HKH từ KDC của HLA và lỗi |

## 2. Lấy token bằng tay

Làm trong **pod HDFS client của HKH** ở namespace `uiy`. Pod này đã có cấu hình HKH và `krb5.conf` trỏ tới KDC `10.208.217.245`.

```bash
NS=vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy
POD=<tên pod hdfs client hkh>
kubectl -n $NS exec -it $POD -- bash
```

Các lệnh dưới đây chạy **trong pod**:

```bash
# 2.1 Đăng nhập KDC HKH bằng principal được ĐỌC /landing/ebs_3g_4g (vd k8s). Renewer = chính principal đó (tên ngắn), để renew/cancel được.
kinit -kt <đường dẫn keytab> k8s@VAILAKEHOUSE.VIETTEL.COM
klist                                   # phải có krbtgt/VAILAKEHOUSE.VIETTEL.COM, cấp bởi KDC HKH
hdfs getconf -confKey fs.defaultFS      # phải là hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020
hdfs dfs -ls /landing/ebs_3g_4g/dt=20261120 | head -3

# 2.2 Lấy token HDFS vào một file
hdfs fetchdt --renewer k8s /tmp/hkh.token
hdfs fetchdt --print /tmp/hkh.token     # xem service (vd vailakehouse.datalakehkh.viettel.com.vn:8020), owner, renewer

# 2.3 Landing có nằm trong encryption zone không? "No FileEncryptionInfo found" = KHÔNG, token HDFS là đủ.
hdfs crypto -getFileEncryptionInfo -path /landing/ebs_3g_4g/dt=20261120/batch=2340/ebs_3g_4g-231050.csv
#     Nếu CÓ FileEncryptionInfo: còn cần token KMS, fetchdt không lấy được -> dừng lại, báo để bổ sung bước lấy token KMS.

# 2.4 Thêm bản sao cho dạng ip:port. Client HDFS bên HLA tìm token theo hadoop.security.token.service.use_ip của NÓ (mặc định Hadoop
#     là true = ip:port), còn HKH đặt false (host:port). Có cả hai bản thì bên nào cũng tìm thấy.
IP=$(getent hosts vailakehouse.datalakehkh.viettel.com.vn | awk '{print $1}'); echo $IP
hadoop dtutil edit -service vailakehouse.datalakehkh.viettel.com.vn:8020 -alias $IP:8020 /tmp/hkh.token
hadoop dtutil print /tmp/hkh.token      # 2 dòng HDFS_DELEGATION_TOKEN: host:8020 và ip:8020

# 2.5 Biết hạn thật: renew một lần, lệnh in ra thời điểm hết hạn mới (renew-interval của HKH, Hadoop mặc định 24 giờ)
hdfs fetchdt --renew /tmp/hkh.token

# 2.6 Thử đọc CHỈ bằng token, không dùng vé Kerberos (đúng như job sẽ làm)
kdestroy
HADOOP_TOKEN_FILE_LOCATION=/tmp/hkh.token hdfs dfs -cat /landing/ebs_3g_4g/dt=20261120/batch=2340/ebs_3g_4g-231050.csv | head -2
```

## 3. Tạo Secret bằng tay

Chạy ở máy có `kubectl`. Dùng `exec` **không có `-t`**, để file nhị phân không bị đổi ký tự xuống dòng.

```bash
kubectl -n $NS exec $POD -- cat /tmp/hkh.token > hkh.token
kubectl -n $NS create secret generic hkh-hdfs-token --from-file=hadoop.token=hkh.token --dry-run=client -o yaml | kubectl apply -f -
rm hkh.token
kubectl -n $NS get secret hkh-hdfs-token -o jsonpath='{.data.hadoop\.token}' | base64 -d | wc -c   # > 0
```

- Tên Secret và tên key (`hadoop.token`) **phải đúng như trên**: DAG đọc chúng từ `INPUT_CLUSTERS["hkh"]` trong `airflow/dags/silver_ingest_common.py`.
- Dùng lại lệnh `apply` này mỗi lần lấy token mới. Mỗi pod Spark mới khởi động đều đọc phiên bản Secret mới nhất.

## 4. Vận hành token

| Việc | Làm thế nào |
|---|---|
| Giữ token sống mỗi ngày | Trước giờ chạy của DAG: `kinit ...` rồi `hdfs fetchdt --renew /tmp/hkh.token` ở pod client. Renew **không đổi nội dung file** nên **không phải cập nhật Secret**. Mỗi lần renew lùi hạn thêm một renew-interval (mặc định 24 giờ) |
| Hết max-lifetime (mặc định 7 ngày từ lúc cấp) | Không renew được nữa: làm lại mục 2 và mục 3 (token mới, cập nhật Secret) |
| Pod client bị tạo lại, mất `/tmp/hkh.token` | Lấy lại file từ Secret: `kubectl -n $NS get secret hkh-hdfs-token -o jsonpath='{.data.hadoop\.token}' \| base64 -d > hkh.token` rồi `kubectl cp` vào pod |
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
