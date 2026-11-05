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
