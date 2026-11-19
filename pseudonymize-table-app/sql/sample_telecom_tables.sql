-- Bảng Delta mẫu (dữ liệu viễn thông GIẢ) để thử pseudonymize-table-app.
--
-- Chạy trong một Spark SQL session có Delta (io.delta.sql.DeltaSparkSessionExtension + DeltaCatalog), cùng warehouse/Kerberos với job.
-- Số điện thoại, IMSI, IMEI, CCCD, họ tên đều do chương trình đặt ra, không phải của người thật.
--
-- Hai bảng, hai dạng partition giống dữ liệu silver thật:
--   demo_db.cdr_voice           partition (dt, hour): dataset hằng giờ như ebs_3g_4g. Cột dt = yyyyMMdd, hour = HH (chuỗi, có số 0 đầu).
--                               DAG luôn truyền INPUT_DATE_KEY=dt, INPUT_HOUR_KEY=hour nên tên hai cột này phải là dt và hour.
--   demo_db.subscriber_profile  partition (dt): snapshot theo ngày như nims, KHÔNG có tầng giờ (INPUT_HOURS bị job từ chối).
--
-- Cột định danh để dạng STRING (số 84 đứng đầu, số 0 đầu). dt và hour KHÔNG được đưa vào PSEUDO_COLUMNS.
--
-- Thử với job (pseudoColumns, khoảng ngày giờ):
--   cdr_voice            msisdn:HASH,called_msisdn:HASH,imsi:HASH,imei:HASH
--                        không khoảng               -> cả bảng, bảng đích không partition
--                        20261012..20261012, giờ 6-7 -> 5 dòng (OUTPUT_MODE=overwrite-partitions)
--   subscriber_profile   msisdn:HASH,full_name:HASH,id_number:HASH      (chỉ lọc theo ngày)
-- Mỗi lần thử nên đặt OUTPUT_TABLE mới, và bảng nguồn cần có key trong Vault (DAK cấp key theo database + table).

CREATE DATABASE IF NOT EXISTS demo_db;

-- ------------------------------------------------------------------------------------------------------------------------------
-- 1. CDR thoại / SMS / data, partition (dt, hour)
-- ------------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS demo_db.cdr_voice;
CREATE TABLE demo_db.cdr_voice (
  msisdn            STRING,          -- thuê bao chủ gọi: 84 + 9 số (vd 8496xxxxxxx)
  called_msisdn     STRING,          -- thuê bao bị gọi / nhận SMS
  imsi              STRING,          -- 15 số, Viettel = 45204 + 10 số
  imei              STRING,          -- 15 số (TAC 8 số + serial 6 số + check digit)
  call_type         STRING,          -- MOC | MTC | SMS_MO | DATA
  start_time        TIMESTAMP,
  duration_sec      INT,
  cell_id           BIGINT,          -- ECI
  lac               INT,
  charge_vnd        DECIMAL(12,2),
  termination_cause STRING,          -- NORMAL | BUSY | NO_ANSWER | DROPPED
  is_roaming        BOOLEAN,
  dt                STRING,          -- yyyyMMdd theo start_time (giờ Việt Nam)
  hour              STRING           -- HH
)
USING DELTA
PARTITIONED BY (dt, hour);

-- Dữ liệu viết tay: 9 dòng, 2 ngày, 3 giờ mỗi ngày (06, 07, 18) -> 6 partition.
INSERT INTO demo_db.cdr_voice VALUES
 ('84961234567','84978765432','452040123456789','353918110234561','MOC',   TIMESTAMP'2026-10-12 06:05:12',  184, 53734012, 12001, 1840.00,'NORMAL',   false,'20261012','06'),
 ('84978765432','84961234567','452040987654321','860123045678901','MTC',   TIMESTAMP'2026-10-12 06:05:12',  184, 53734012, 12001,    0.00,'NORMAL',   false,'20261012','06'),
 ('84988001122','84339998877','452040555500012','356789102345670','MOC',   TIMESTAMP'2026-10-12 06:41:30',    0, 53734100, 12002,    0.00,'NO_ANSWER',false,'20261012','06'),
 ('84335551234','84961234567','452041122334455','490154203237518','SMS_MO',TIMESTAMP'2026-10-12 07:12:08',    0, 53734012, 12001,  350.00,'NORMAL',   false,'20261012','07'),
 ('84961234567','84868001234','452040123456789','353918110234561','MOC',   TIMESTAMP'2026-10-12 07:48:55',  912, 53739877, 12044, 9120.00,'NORMAL',   false,'20261012','07'),
 ('84868001234','84988001122','452042233445566','866543210987654','MOC',   TIMESTAMP'2026-10-12 18:02:41',   47, 53734100, 12002,  470.00,'DROPPED',  false,'20261012','18'),
 ('84339998877','84335551234','452043344556677','358240051111110','MOC',   TIMESTAMP'2026-10-13 06:15:03',  305, 53740001, 12050, 3050.00,'NORMAL',   false,'20261013','06'),
 ('84961234567','6591234567', '452040123456789','353918110234561','MOC',   TIMESTAMP'2026-10-13 07:20:19',  620, 53734012, 12001,31000.00,'NORMAL',   true, '20261013','07'),
 ('84978765432','84961234567','452040987654321','860123045678901','MOC',   TIMESTAMP'2026-10-13 18:33:27',   12, 53739877, 12044,  120.00,'BUSY',     false,'20261013','18');

-- Dữ liệu sinh ngẫu nhiên để thử khối lượng lớn: 100.000 dòng cho MỘT giờ (dt=20261014, hour=06).
-- Muốn thêm giờ khác thì đổi mốc thời gian ở start_time cùng hai giá trị dt, hour rồi chạy lại.
INSERT INTO demo_db.cdr_voice
SELECT
  concat('84', element_at(array('96','97','98','32','33','34','35','86'), pmod(hash(id,1),8)+1),
         lpad(cast(pmod(hash(id,2),10000000) as string),7,'0'))                         AS msisdn,
  concat('84', element_at(array('96','97','98','32','33','34','35','86'), pmod(hash(id,7),8)+1),
         lpad(cast(pmod(hash(id,8),10000000) as string),7,'0'))                         AS called_msisdn,
  concat('45204', lpad(cast(pmod(hash(id,3),1000000000) as string),10,'0'))             AS imsi,
  concat(lpad(cast(pmod(hash(id,4),100000000) as string),8,'0'),
         lpad(cast(pmod(hash(id,5),1000000) as string),6,'0'),
         cast(pmod(hash(id,6),10) as string))                                           AS imei,
  element_at(array('MOC','MTC','SMS_MO','DATA'), pmod(hash(id,9),4)+1)                  AS call_type,
  cast(from_unixtime(unix_timestamp('2026-10-14 06:00:00') + pmod(hash(id,10),3600)) as timestamp) AS start_time,
  1 + pmod(hash(id,11),1800)                                                            AS duration_sec,
  53734000 + pmod(hash(id,12),5000)                                                     AS cell_id,
  12000 + pmod(hash(id,13),100)                                                         AS lac,
  cast(pmod(hash(id,14),50000) as decimal(12,2))                                        AS charge_vnd,
  element_at(array('NORMAL','NORMAL','NORMAL','BUSY','NO_ANSWER','DROPPED'), pmod(hash(id,15),6)+1) AS termination_cause,
  pmod(hash(id,16),50) = 0                                                              AS is_roaming,
  '20261014' AS dt,
  '06'       AS hour
FROM (SELECT explode(sequence(1, 100000)) AS id);

-- Kiểm tra:
-- Bảng Delta không hỗ trợ SHOW PARTITIONS (lỗi PARTITION_MANAGEMENT_IS_UNSUPPORTED). Xem partition bằng:
-- DESCRIBE DETAIL demo_db.cdr_voice;                                  -- partitionColumns, numFiles, sizeInBytes
-- SELECT DISTINCT dt, hour FROM demo_db.cdr_voice ORDER BY dt, hour;
-- SELECT dt, hour, count(*) FROM demo_db.cdr_voice GROUP BY dt, hour ORDER BY dt, hour;

-- ------------------------------------------------------------------------------------------------------------------------------
-- 2. Hồ sơ thuê bao, snapshot theo ngày, partition (dt)
-- ------------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS demo_db.subscriber_profile;
CREATE TABLE demo_db.subscriber_profile (
  msisdn        STRING,
  full_name     STRING,
  id_number     STRING,        -- CCCD 12 số
  date_of_birth DATE,
  province      STRING,
  address       STRING,
  plan_code     STRING,
  activated_at  TIMESTAMP,
  dt            STRING
)
USING DELTA
PARTITIONED BY (dt);

INSERT INTO demo_db.subscriber_profile VALUES
 ('84961234567','Nguyễn Văn An',  '001085012345',DATE'1985-03-14','Hà Nội',        '12 Láng Hạ, Đống Đa',     'V120',TIMESTAMP'2019-05-02 09:30:00','20261012'),
 ('84978765432','Trần Thị Bích',  '079092067890',DATE'1992-11-02','TP Hồ Chí Minh','45 Nguyễn Huệ, Quận 1',   'V90', TIMESTAMP'2021-08-17 14:10:00','20261012'),
 ('84988001122','Lê Hoàng Cường', '048078034567',DATE'1978-07-25','Đà Nẵng',       '8 Bạch Đằng, Hải Châu',   'ST60',TIMESTAMP'2017-01-09 10:00:00','20261012'),
 ('84335551234','Phạm Thu Dung',  '031099022334',DATE'1999-01-30','Hải Phòng',     '77 Lạch Tray, Ngô Quyền', 'V90', TIMESTAMP'2023-03-21 16:45:00','20261012'),
 ('84961234567','Nguyễn Văn An',  '001085012345',DATE'1985-03-14','Hà Nội',        '12 Láng Hạ, Đống Đa',     'V150',TIMESTAMP'2019-05-02 09:30:00','20261013'),
 ('84868001234','Vũ Minh Hải',    '092088045678',DATE'1988-09-09','Cần Thơ',       '3 Hòa Bình, Ninh Kiều',   'V120',TIMESTAMP'2020-12-05 11:20:00','20261013');

-- Kiểm tra:
-- Bảng Delta không hỗ trợ SHOW PARTITIONS, dùng:
-- DESCRIBE DETAIL demo_db.subscriber_profile;                         -- partitionColumns, numFiles, sizeInBytes
-- SELECT DISTINCT dt FROM demo_db.subscriber_profile ORDER BY dt;
-- SELECT dt, count(*) FROM demo_db.subscriber_profile GROUP BY dt ORDER BY dt;

-- ------------------------------------------------------------------------------------------------------------------------------
-- 3. Sinh nhiều hồ sơ thuê bao KHỚP với 100.000 dòng cdr_voice ngẫu nhiên (dt=20261014), để thử join
-- ------------------------------------------------------------------------------------------------------------------------------
-- Cách sinh msisdn GIỐNG HỆT đoạn sinh cdr_voice ở trên (cùng id 1..100000, cùng hash(id,1)/hash(id,2) cho người gọi và hash(id,7)/hash(id,8)
-- cho người bị gọi), nên không cần đọc bảng cdr_voice mà vẫn trùng số. Chỉ một phần số có hồ sơ, phần còn lại KHÔNG có để thử left join:
--   người gọi    : hash(id,20) -> khoảng 80% id có hồ sơ
--   người bị gọi : hash(id,21) -> khoảng 50% id có hồ sơ
-- Mỗi msisdn chỉ một dòng ở dt=20261014 (min(id) khi hai id sinh trùng số), để join theo (msisdn, dt) không nhân dòng.
-- Chạy sau mục 1 (cần đã sinh 100.000 dòng cdr_voice) và chỉ chạy MỘT lần (chạy lại sẽ nhân đôi hồ sơ: DELETE FROM demo_db.subscriber_profile
-- WHERE dt IN ('20261013','20261014') hoặc DROP rồi tạo lại bảng ở mục 2).
INSERT INTO demo_db.subscriber_profile
WITH ids AS (SELECT explode(sequence(1, 100000)) AS id),
nums AS (
  SELECT id,
         concat('84', element_at(array('96','97','98','32','33','34','35','86'), pmod(hash(id,1),8)+1),
                lpad(cast(pmod(hash(id,2),10000000) as string),7,'0')) AS msisdn
  FROM ids WHERE pmod(hash(id,20),10) < 8
  UNION ALL
  SELECT id,
         concat('84', element_at(array('96','97','98','32','33','34','35','86'), pmod(hash(id,7),8)+1),
                lpad(cast(pmod(hash(id,8),10000000) as string),7,'0')) AS msisdn
  FROM ids WHERE pmod(hash(id,21),10) < 5
),
one AS (SELECT msisdn, min(id) AS id FROM nums GROUP BY msisdn)
SELECT
  msisdn,
  concat(element_at(array('Nguyễn','Trần','Lê','Phạm','Hoàng','Vũ','Đặng','Bùi'), pmod(hash(id,30),8)+1), ' ',
         element_at(array('Văn','Thị','Minh','Thu','Hoàng','Quốc'), pmod(hash(id,31),6)+1), ' ',
         element_at(array('An','Bình','Cường','Dung','Hải','Lan','Nam','Phúc'), pmod(hash(id,32),8)+1))   AS full_name,
  concat(lpad(cast(pmod(hash(id,33),100) as string),3,'0'),
         lpad(cast(pmod(hash(id,34),1000000000) as string),9,'0'))                                          AS id_number,
  date_add(DATE'1960-01-01', pmod(hash(id,35),16000))                                                      AS date_of_birth,
  element_at(array('Hà Nội','TP Hồ Chí Minh','Đà Nẵng','Hải Phòng','Cần Thơ','Huế','Nghệ An','Khánh Hòa'),
             pmod(hash(id,36),8)+1)                                                                         AS province,
  concat(1 + pmod(hash(id,37),200), ' Đường số ', 1 + pmod(hash(id,38),60))                                AS address,
  element_at(array('V90','V120','V150','ST60','D60'), pmod(hash(id,39),5)+1)                               AS plan_code,
  cast(from_unixtime(unix_timestamp('2015-01-01 00:00:00') + pmod(hash(id,40),300000000)) as timestamp)    AS activated_at,
  '20261014'                                                                                               AS dt
FROM one;

-- Snapshot ngày trước (dt=20261013) cho khoảng 30% số đã có hồ sơ, ĐỔI gói cước: cùng msisdn có hồ sơ ở hai ngày, để thử join CÓ và KHÔNG
-- có điều kiện dt (không có điều kiện dt thì mỗi dòng cdr của số đó bị nhân đôi). Chạy MỘT lần, sau lệnh trên.
INSERT INTO demo_db.subscriber_profile
SELECT msisdn, full_name, id_number, date_of_birth, province, address,
       element_at(array('V90','V120','V150','ST60','D60'), pmod(hash(msisdn,41),5)+1) AS plan_code,
       activated_at,
       '20261013' AS dt
FROM demo_db.subscriber_profile
WHERE dt = '20261014' AND pmod(hash(msisdn,42),10) < 3;

-- Kiểm tra dữ liệu vừa sinh:
-- SELECT dt, count(*) AS so_dong, count(DISTINCT msisdn) AS so_msisdn FROM demo_db.subscriber_profile GROUP BY dt ORDER BY dt;
-- SELECT msisdn, dt, count(*) FROM demo_db.subscriber_profile GROUP BY msisdn, dt HAVING count(*) > 1;   -- phải RỖNG (khoá (msisdn, dt) duy nhất)

-- ------------------------------------------------------------------------------------------------------------------------------
-- 4. Join cdr_voice với subscriber_profile (chạy từng câu, đối chiếu với phần "Kiểm tra" của mỗi câu)
-- ------------------------------------------------------------------------------------------------------------------------------
-- 4.1 Inner join theo (msisdn, dt): chỉ dòng CDR có hồ sơ đúng ngày.
-- SELECT c.dt, c.hour, c.msisdn, c.call_type, c.duration_sec, p.full_name, p.province, p.plan_code
-- FROM demo_db.cdr_voice c
-- JOIN demo_db.subscriber_profile p ON c.msisdn = p.msisdn AND c.dt = p.dt
-- WHERE c.dt IN ('20261012','20261013');                       -- dữ liệu viết tay: kỳ vọng 6 dòng (xem mục 5)
--
-- 4.2 Left join: giữ mọi dòng CDR, số không có hồ sơ cho cột p.* = NULL.
-- SELECT c.dt, c.msisdn, c.call_type, p.full_name, p.plan_code
-- FROM demo_db.cdr_voice c
-- LEFT JOIN demo_db.subscriber_profile p ON c.msisdn = p.msisdn AND c.dt = p.dt
-- WHERE c.dt IN ('20261012','20261013')
-- ORDER BY c.dt, c.start_time;                                  -- kỳ vọng 9 dòng, 3 dòng có p.* = NULL
--
-- 4.3 Join hai lần: cả người gọi lẫn người bị gọi.
-- SELECT c.dt, c.call_type, c.msisdn, a.full_name AS ten_nguoi_goi, c.called_msisdn, b.full_name AS ten_nguoi_nhan
-- FROM demo_db.cdr_voice c
-- LEFT JOIN demo_db.subscriber_profile a ON c.msisdn        = a.msisdn AND c.dt = a.dt
-- LEFT JOIN demo_db.subscriber_profile b ON c.called_msisdn = b.msisdn AND c.dt = b.dt
-- WHERE c.dt = '20261012';
--
-- 4.4 Thống kê sau join: doanh thu và số cuộc theo tỉnh, gói cước (khối lượng lớn, dt=20261014).
-- SELECT p.province, p.plan_code, count(*) AS so_cuoc, sum(c.charge_vnd) AS doanh_thu
-- FROM demo_db.cdr_voice c
-- JOIN demo_db.subscriber_profile p ON c.msisdn = p.msisdn AND c.dt = p.dt
-- WHERE c.dt = '20261014'
-- GROUP BY p.province, p.plan_code
-- ORDER BY doanh_thu DESC;
--
-- 4.5 Join SAI chỉ theo msisdn (không có c.dt = p.dt): dòng CDR của số có hồ sơ ở hai ngày bị nhân đôi. Dùng để so với 4.1.
-- SELECT count(*) FROM demo_db.cdr_voice c JOIN demo_db.subscriber_profile p ON c.msisdn = p.msisdn WHERE c.dt = '20261014';

-- ------------------------------------------------------------------------------------------------------------------------------
-- 5. Cách kiểm tra output của join
-- ------------------------------------------------------------------------------------------------------------------------------
-- (a) Dữ liệu viết tay (dt 20261012, 20261013) có kết quả biết trước:
--     inner join (msisdn, dt)  = 6 dòng   (5 dòng ngày 12, 1 dòng ngày 13: 84961234567)
--     left join                = 9 dòng   (3 dòng không có hồ sơ: 84868001234 ngày 12, 84339998877 và 84978765432 ngày 13, vì hồ sơ của
--                                           hai số 84868001234 / 84978765432 nằm ở ngày khác)
--     Số bị gọi 6591234567 (roaming) không có hồ sơ ở bất kỳ ngày nào.
-- (b) Khoá (msisdn, dt) của profile duy nhất thì left join KHÔNG làm tăng số dòng, và inner = semi:
-- SELECT
--   (SELECT count(*) FROM demo_db.cdr_voice WHERE dt = '20261014')                                                     AS cdr,
--   (SELECT count(*) FROM demo_db.cdr_voice c LEFT JOIN demo_db.subscriber_profile p ON c.msisdn=p.msisdn AND c.dt=p.dt
--      WHERE c.dt = '20261014')                                                                                        AS left_join,    -- = cdr
--   (SELECT count(*) FROM demo_db.cdr_voice c JOIN demo_db.subscriber_profile p ON c.msisdn=p.msisdn AND c.dt=p.dt
--      WHERE c.dt = '20261014')                                                                                        AS inner_join,
--   (SELECT count(*) FROM demo_db.cdr_voice c LEFT SEMI JOIN demo_db.subscriber_profile p ON c.msisdn=p.msisdn AND c.dt=p.dt
--      WHERE c.dt = '20261014')                                                                                        AS semi,         -- = inner_join
--   (SELECT count(*) FROM demo_db.cdr_voice c LEFT ANTI JOIN demo_db.subscriber_profile p ON c.msisdn=p.msisdn AND c.dt=p.dt
--      WHERE c.dt = '20261014')                                                                                        AS anti;         -- inner_join + anti = cdr
--     Chạy thử đúng file này (hash tất định nên số cố định): cdr 100000, left_join 100000, inner_join 79834, semi 79834, anti 20166.
--     Hồ sơ sinh ra: dt=20261014 có 129621 dòng, dt=20261013 có 38690 dòng (gồm 2 dòng viết tay), không có khoá (msisdn, dt) trùng.
--     Join theo người bị gọi (4.3 với c.called_msisdn, dt=20261014) khớp 50019 dòng (khoảng 50%).
-- (c) Join đúng ngày không nhân dòng: câu 4.5 lớn hơn 4.1 đúng bằng số dòng CDR của các số có hồ sơ ở cả hai ngày (khoảng 30%).
--     Chạy thử: 4.5 = 103590 so với 4.1 = 79834, chênh 23756 dòng (khoảng 30% của 79834).
-- (d) Kiểm tra chéo từng số: SELECT * FROM demo_db.subscriber_profile WHERE msisdn = '<msisdn lấy từ một dòng kết quả>' ORDER BY dt;
-- (e) Join trên bảng ĐÃ KHỬ ĐỊNH DANH (đặt tên bảng đích theo OUTPUT_TABLE của bạn, vd demo_db.cdr_voice_deid, demo_db.subscriber_profile_deid).
--     HASH là HMAC tất định: join chỉ đúng khi hai bảng khử định danh bằng CÙNG một khoá (phiếu thứ hai chọn "dùng lại key của phiếu trước").
--     Số dòng join phải BẰNG số dòng join trên bảng gốc cùng khoảng ngày:
-- SELECT count(*) FROM demo_db.cdr_voice_deid c JOIN demo_db.subscriber_profile_deid p ON c.msisdn = p.msisdn AND c.dt = p.dt
--   WHERE c.dt = '20261014';                                    -- = inner_join của (b)
-- SELECT count(DISTINCT msisdn) FROM demo_db.cdr_voice_deid WHERE dt = '20261014';       -- = count(DISTINCT msisdn) của bảng gốc cùng ngày
-- SELECT count(*) FROM demo_db.cdr_voice_deid WHERE dt = '20261014' AND msisdn NOT RLIKE '^k[0-9]+_[0-9a-f]{32}$';   -- phải = 0 (dạng k<version>_<32 hex>)
--     Hai bảng khử định danh bằng HAI khoá khác nhau thì join trả 0 dòng: dùng để xác nhận khoá có thực sự khác.
