# Streaming — GBFS Kafka → Spark

| File | Vai trò |
|---|---|
| `utils/schemas.py` | `StructType` của 3 topic `gbfs.station_status`, `gbfs.station_information`, `gbfs.invalid` (contract: `docs/contracts.md` 1.4, 1.5, 2.3.2) |
| `jobs/gbfs_history.py` | Entry point của service `spark-streaming`: đọc Kafka → `from_json` → phân loại hợp lệ / sai schema → sink; khởi động thêm query Silver status |
| `jobs/silver.py` | Phần dùng chung Bronze → Silver: đọc Bronze incremental, parse + validate lại `raw_value`, dedup `(station_id, poll_id)`, MERGE chỉ insert |
| `jobs/silver_status.py` | Bronze → `silver.gbfs_status` (STR-09) |
| `jobs/silver_information.py` | Bronze → `silver.gbfs_station_information` (STR-10) |
| `jobs/dq_checks.py` | Data-quality checks dạng SQL "đếm dòng xấu"; check `error` thất bại thì exit 1 |
| `fixtures/gbfs/*.jsonl` | Message mẫu (mỗi dòng 1 message). `station_status_bad.jsonl`: 10 message sai, mỗi dòng một kiểu lỗi |

Sink mặc định là **Bronze** (STR-08). Sink `console` dùng để debug. Dedup `(station_id, poll_id)` và lọc message sai làm ở Silver (STR-09/10). Với `--sink bronze` ở chế độ liên tục, job khởi động thêm một query Silver cho mỗi feed (tắt bằng `--no-silver`).

Schema Spark khớp tên field với JSON Schema của producer (`ingestion/gbfs/json_schemas`). Cả hai feed đều bắt buộc có `poll_id` không rỗng; `poll_id` của information là mã lần polling information (`station_information-<uuid4>`), khác `poll_id` của status.

## Phân loại message

`validate()` thêm 3 cột `dq_is_valid`, `dq_error_type`, `dq_error_message`:

| `dq_error_type` | Khi nào |
|---|---|
| `PARSE_ERROR` | Message rỗng hoặc không phải JSON |
| `SCHEMA_ERROR` | JSON hợp lệ nhưng không phải object, hoặc field sai kiểu JSON (`"five"` cho số, `"true"` cho boolean, `"station_id": 123`, `"poll_id": {}`, `vehicle_types_available[0].vehicle_type_id` là số, `rental_uris.android` là số), một key xuất hiện hai lần trong cùng object, hoặc JSON không chuẩn (dấu nháy đơn) |
| `VALIDATION_ERROR` | Đúng kiểu nhưng vi phạm contract: thiếu field bắt buộc, chuỗi rỗng, số âm, `schema_version != 1`, datetime thiếu timezone, `lat/lon` ngoài phạm vi, epoch không phải giây (`last_reported` / `last_updated` > 4102444800, tức sau năm 2100)… (liệt kê tất cả lỗi, nối bằng `; `) |

`from_json` của Spark đổi kiểu thay vì từ chối (`123` → `"123"` cho field `STRING`) và chấp nhận dấu nháy đơn, còn producer (Pydantic, strict) thì không. `NaN`, `Infinity`, `-Infinity` (Python `json.loads` mặc định nhận) cũng bị từ chối; JSON lồng quá sâu (khoảng 1 000 lớp trở lên với Python 3.8 của image) là `SCHEMA_ERROR` "JSON nested too deep", không làm hỏng cả micro-batch. Vì vậy `validate()` kiểm tra thêm kiểu JSON bằng Python (`json_type_errors`, UDF trên `raw_value`): `json.loads` rồi so **từng field theo đúng cấp** với schema Spark, kể cả struct lồng và phần tử mảng; key trùng bị từ chối vì `from_json` âm thầm giữ một trong hai giá trị. Field lạ bị bỏ qua (như `from_json`); `raw_payload` của `gbfs.invalid` nhận mọi kiểu. Giới hạn epoch có ở cả producer (`EpochSeconds`) và Spark: giá trị mili/micro giây làm tràn số khi Silver đổi sang `TIMESTAMP`.

Producer: một trạm có `last_reported` sai đơn vị chỉ làm trạm đó vào `gbfs.invalid`. Nhưng `last_updated` **cấp feed** sai thì cả lần polling hỏng trước khi tách trạm: không có message nào lên Kafka (kể cả `gbfs.invalid`), log `Giữ lại ...: feed không xử lý được`, `raw.json` được giữ trên máy (và lưu lên MinIO raw nếu bật) để kiểm tra; thư mục này không `--replay` được vì không có `valid.jsonl` (xem "Lỗi cấp feed" trong `ingestion/gbfs/README.md`).

Message sai được đưa về đúng dạng `gbfs.invalid` (`source, error_type, error_message, raw_payload, detected_at`) bằng `invalid_rows()`.

## Bronze (STR-08)

| Bảng (catalog `lakehouse`, Trino: `iceberg.bronze.*`) | Topic | Checkpoint (service `spark-streaming`) |
|---|---|---|
| `bronze.gbfs_status` | `gbfs.station_status` | `STATUS_CHECKPOINT_LOCATION` = `s3a://checkpoints/status-history/` |
| `bronze.gbfs_station_information` | `gbfs.station_information` | `INFORMATION_CHECKPOINT_LOCATION` = `s3a://checkpoints/information-history/` |

- **Grain:** một dòng = một message Kafka, khóa `(kafka_topic, kafka_partition, kafka_offset, kafka_timestamp)`. Sink, DQ (`bronze_*_no_duplicate_messages`) và test dùng cùng khóa này. `kafka_timestamp` có mặt để phân biệt các "thế hệ" của một topic bị xóa rồi tạo lại (offset bắt đầu lại từ 0). Đây là giả định chứ không phải định danh thế hệ được bảo đảm: hai message của hai thế hệ cùng offset và cùng millisecond sẽ bị coi là một. Check `bronze_*_offset_reused` (warn) cho biết khi nào offset đã bị dùng lại; sau khi tạo lại topic, ghi việc đó vào runbook.
- **Append-only, không lọc, không dedup:** message sai vẫn được ghi, kèm `dq_is_valid`, `dq_error_type`, `dq_error_message`. Silver đọc từ Bronze và lọc theo `dq_is_valid`.
- `raw_value` là value Kafka nguyên văn, để dựng lại Silver từ Bronze khi đổi logic parse.
- `poll_id`, `station_id`, `collected_at` (timestamp) được tách ra từ message cho việc lọc, đọc incremental và dedup ở Silver; null khi message không parse được.
- `kafka_key` là key producer gửi (`station_id`); `ingested_at` là thời điểm micro-batch được xử lý.
- Partition theo `days(ingested_at)`. Iceberg format v2.
- **Exactly-once:** offset Kafka nằm trong checkpoint của query; mỗi micro-batch là **một lần append Iceberg** (`foreachBatch`) chỉ gồm các message **Bronze chưa có**, so theo `(kafka_topic, kafka_partition, kafka_offset, kafka_timestamp)` (`messages_not_in`). Bằng chứng một message đã ghi là chính dòng dữ liệu, nên batch chạy lại (crash sau commit Iceberg, trước khi checkpoint ghi `commits/<n>`) không thêm dòng nào, dù giữa hai lần chạy snapshot đã bị expire, checkpoint do Iceberg sink bản cũ ghi, hay checkpoint bị mất. Batch không có gì mới thì không tạo snapshot.
- **Chi phí:** chỉ đọc các file Bronze có khoảng offset / timestamp chồng lên batch (thống kê min/max cột của Iceberg), bình thường là các file của chính batch được chạy lại. `kafka_timestamp` nằm trong khóa để topic bị xóa và tạo lại (offset bắt đầu lại từ 0) không bị nhầm là message cũ.
- **Một writer cho mỗi bảng Bronze:** hai query cùng ghi một bảng với cùng offset ở cùng lúc có thể cùng thấy message là mới. Service chạy một query Bronze mỗi feed; khi chạy tay, dừng service trước.
- **Vì sao không dùng Iceberg streaming sink (`writeStream.format("iceberg")`):** khi một batch đã commit được chạy lại trong một ứng dụng Spark mới, sink đó ghi file dữ liệu **trùng tên** với file đã commit (`<partition>-<task>-<query id>-<epoch>-…`), ghi đè lên MinIO rồi mới bỏ qua commit; snapshot đã commit trỏ tới file có kích thước khác và mọi lần đọc Bronze lỗi `EOFException`. Lỗi này lộ ra ở kịch bản 2 của `tests/integration/test_spark_recovery.py` (review STR-12). Append kiểu batch luôn tạo tên file mới.
- Mỗi feed một query, một checkpoint riêng. **Không** dùng chung checkpoint giữa Bronze và console.

## Silver status (STR-09)

`silver.gbfs_status` (Trino: `iceberg.silver.gbfs_status`), partition `days(collected_at)`.

- **Grain:** một dòng = một trạm tại một lần polling, khóa `(station_id, poll_id)`. Lần polling mà trạm không đổi trạng thái vẫn là một dòng mới.
- **Nguồn:** đọc `bronze.gbfs_status` incremental (Iceberg streaming source), checkpoint `SILVER_STATUS_CHECKPOINT_LOCATION` = `s3a://checkpoints/silver-status/`. Checkpoint này **phải nằm trên S3/MinIO**: Iceberg source ghi offset bằng S3FileIO của bảng.
- **Validation:** parse + validate lại `raw_value` bằng cùng schema và luật của STR-07 (không tin cờ `dq_is_valid` đã lưu ở Bronze), nên đổi luật thì dựng lại Silver từ Bronze được. Message sai chỉ nằm ở Bronze.
- **Dedup:** message phát lại cùng `(station_id, poll_id)` (producer retry, `--replay`) chỉ còn một dòng; giữ Kafka offset nhỏ nhất.
- **Một `collected_at` cho mỗi lần polling:** bảng phụ `silver.gbfs_status_polls` (`poll_id`, `collected_at`) ghi `collected_at` của mọi lần polling đã vào Silver (lần đầu tạo thì lấy từ Silver hiện có). Poll mới lấy `collected_at` của message đầu tiên (Kafka offset nhỏ nhất). Message có `poll_id` đã biết nhưng `collected_at` khác (producer hỏng) **không vào Silver**, vẫn nằm ở Bronze, log `[silver] … rows dropped` và check `silver_status_poll_time_conflicts` (warn) đếm chúng. Bảng phụ được ghi trước MERGE: crash giữa hai bước thì batch chạy lại thấy poll với cùng thời gian. Thứ tự trong một batch: **xác định thời gian poll và loại message sai thời gian trước, rồi mới dedup**; dedup trước có thể giữ message sai và bỏ mất message đúng của cùng khóa. Lần đầu tạo bảng phụ trên một Silver đã có dữ liệu, job **từ chối chạy** nếu Silver có `poll_id` mang nhiều `collected_at` (lỗi in ra câu SQL liệt kê chúng); dọn các dòng đó rồi chạy lại.
- **Idempotent:** `MERGE ... ON (station_id, poll_id) WHEN NOT MATCHED THEN INSERT *`. Micro-batch chạy lại, hoặc mất checkpoint Silver rồi đọc lại toàn bộ Bronze, không thêm dòng trùng, bất kể `collected_at` của message chạy lại cách bao xa.
- **Prune partition:** vì mọi dòng của một poll có cùng `collected_at`, điều kiện MERGE có thêm `t.collected_at = s.collected_at` và khoảng `collected_at` của batch, tính bằng micro giây epoch (`timestamp_micros`, không phụ thuộc timezone của driver hay session Spark): MERGE chỉ đọc các partition ngày của batch.
- `last_updated`, `last_reported` (epoch giây) và `collected_at` được đổi sang `TIMESTAMP` UTC, giữ riêng từng cột. Cột lineage: `kafka_partition`, `kafka_offset`, `bronze_ingested_at`, `silver_processed_at`.

```bash
# Chạy bù Silver từ Bronze rồi dừng (thủ công / Airflow).
# Mỗi checkpoint chỉ được MỘT writer: dừng service trước, rồi truyền đúng checkpoint của nó
# (spark-master không có sẵn biến SILVER_*_CHECKPOINT_LOCATION).
docker compose --profile pipeline stop spark-streaming
docker compose exec -e SILVER_STATUS_CHECKPOINT_LOCATION=s3a://checkpoints/silver-status/ \
  spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/silver_status.py --available-now

# Data-quality gate (exit 1 khi có check error thất bại); chạy khi Bronze đứng yên
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/dq_checks.py --layer silver_status
docker compose --profile pipeline start spark-streaming
```

| Check `silver_status` | Mức | Ý nghĩa |
|---|---|---|
| `silver_status_grain_unique` | error | Không có hai dòng cùng `(station_id, poll_id)` |
| `silver_status_one_collected_at_per_poll` | error | Mỗi `poll_id` chỉ có một `collected_at` (MERGE dựa vào điều này) |
| `silver_status_required_not_null` | error | Các cột bắt buộc không null |
| `silver_status_counts_non_negative` | error | Số xe, số dock không âm |
| `silver_status_has_every_valid_bronze_key` | error | Mọi khóa hợp lệ ở Bronze đều có ở Silver (không mất) |
| `silver_status_only_valid_bronze_keys` | error | Silver không có khóa mà Bronze không coi là hợp lệ |
| `silver_status_poll_completeness` | warn | Lần polling có ít hơn 90% số trạm của lần lớn nhất |
| `silver_status_poll_time_conflicts` | warn | Message hợp lệ ở Bronze có `poll_id` đã ghi nhận với `collected_at` khác (bị giữ ngoài Silver, không tính là "thiếu") |
| `silver_status_last_reported_not_after_collected` | warn | `last_reported` sau `collected_at` |

Hai check đối soát với Bronze so Silver với các khóa Bronze **hợp lệ theo luật hiện tại**: `dq_checks.py` validate lại `raw_value` bằng cùng code Silver dùng, không tin cờ `dq_is_valid` đã lưu lúc ingest. Chúng chỉ đúng khi Bronze đứng yên và Silver đã bắt kịp (như lệnh trên: dừng service, `--available-now`, rồi DQ); nếu Bronze commit thêm sau lần chạy cuối của Silver, các khóa đó bị báo là thiếu. Layer `bronze_status` kiểm tra không trùng message (`no_duplicate_messages`, error) và offset bị dùng lại (`offset_reused`, warn).

### Giữ snapshot và dựng lại Silver

Query Silver đọc Bronze bằng Iceberg streaming source: nó cần **mọi snapshot Bronze sau snapshot ghi trong checkpoint**. Compaction (`rewrite_data_files`, snapshot `replace`) không ảnh hưởng, source bỏ qua snapshot đó. Nhưng `expire_snapshots` xóa snapshot mà checkpoint còn trỏ tới thì query Silver dừng với lỗi, và tạo checkpoint mới cũng **không** lấy lại được dữ liệu đã bỏ lỡ.

- **Quy tắc maintenance:** chỉ expire snapshot Bronze cũ hơn độ trễ tối đa có thể của Silver, ví dụ `older_than` = 7 ngày trước (bằng retention Kafka) và `retain_last` đủ lớn. Bình thường Silver trễ Bronze dưới một trigger.
- **Khi đã lỡ expire (hoặc mất / hỏng checkpoint Silver):** dừng service, chạy `--rebuild` với **checkpoint mới**:

```bash
docker compose --profile pipeline stop spark-streaming
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/silver_status.py --rebuild --available-now \
  --checkpoint-dir s3a://checkpoints/silver-status-rebuild-$(date +%Y%m%d)
# rồi đổi SILVER_STATUS_CHECKPOINT_LOCATION của spark-streaming sang
# s3a://checkpoints/silver-status-rebuild-<ngày>/silver_status và khởi động lại service
```

`--rebuild` MERGE toàn bộ Bronze ở snapshot hiện tại S vào Silver (chỉ insert khóa chưa có, nên không trùng), rồi khởi động query streaming với checkpoint mới bằng `stream-from-timestamp` = thời điểm commit của S **trừ 60 giây**. Iceberg chọn snapshot bắt đầu theo thời gian commit, mà thời gian này không duy nhất (hai snapshot có thể cùng millisecond); bắt đầu sớm hơn S thì đọc lại vài snapshot (vô hại vì MERGE chỉ insert khóa mới) chứ không bỏ sót. Nếu các snapshot trước S đã bị expire, Iceberg bắt đầu từ snapshot cũ nhất còn lại, tức S. `--rebuild` từ chối checkpoint đã tồn tại. `silver_information.py` có cùng tùy chọn.

Điều kiện và giới hạn:
- **Bắt buộc dừng service** trước khi rebuild (một writer cho mỗi bảng Silver, và không có snapshot Bronze mới xen vào giữa lần đọc batch và lúc stream bắt đầu).
- `--rebuild` chỉ thêm khóa còn thiếu, **không xóa** dòng Silver đã có. Nếu luật validation được siết lại, các dòng Silver cũ nay không còn hợp lệ vẫn ở đó (check `silver_*_only_valid_bronze_keys` báo); muốn dọn thì xóa bảng Silver (và bảng `*_polls`) rồi rebuild.

### Kafka mất dữ liệu (`FAIL_ON_DATA_LOSS`)

Mặc định `FAIL_ON_DATA_LOSS=true`: nếu offset mà checkpoint cần đã bị Kafka xóa (Spark dừng lâu hơn retention 7 ngày, topic bị tạo lại), query Bronze dừng với lỗi thay vì chạy tiếp với lịch sử bị hổng. Khi gặp lỗi này:

1. Xác định khoảng bị mất (log lỗi có offset; `kafka-get-offsets.sh --time -2` cho offset sớm nhất còn lại).
2. Nếu cần lịch sử đầy đủ: bù từ raw trên MinIO (`s3://raw/gbfs/...`, mỗi lần polling một file).
3. Nếu chấp nhận khoảng trống: dừng service, chạy lại một lần với `FAIL_ON_DATA_LOSS=false` để Spark nhảy qua, ghi lại khoảng trống trong runbook, rồi chạy lại service với giá trị mặc định:

```bash
docker compose --profile pipeline stop spark-streaming
FAIL_ON_DATA_LOSS=false docker compose --profile pipeline up -d spark-streaming
# đợi log [progress] gbfs_*_bronze cho thấy query đã qua khoảng mất, rồi
docker compose --profile pipeline up -d spark-streaming   # tạo lại container, FAIL_ON_DATA_LOSS=true
```

Trong lúc lỗi này chưa được xử lý, `restart: unless-stopped` khởi động lại container liên tục và lần nào cũng dừng với cùng lỗi: đó là tín hiệu cần can thiệp, không phải tự phục hồi.

## Silver station information (STR-10)

`silver.gbfs_station_information` (Trino: `iceberg.silver.gbfs_station_information`), partition `months(collected_at)`.

- **Grain:** một dòng = một trạm trong một lần polling information, khóa `(station_id, poll_id)`. Mỗi lần polling (24 giờ một lần, và một lần mỗi khi poller khởi động) là một **snapshot đầy đủ** danh sách trạm, giữ nguyên như quan sát được, kể cả khi không đổi. Nhờ vậy đọc được tên, tọa độ, sức chứa, `short_name` của trạm "tại" bất kỳ lần polling nào (cần cho mapping với Trip History, scope Slide 11).
- **Đầy đủ trường của feed nguồn:** `station_id`, `name`, `short_name`, `lat`, `lon`, `capacity`, `region_id`, `rental_uris` (`android`, `ios`, `web`), `is_charging`, cùng `is_virtual_station` (contract có, nguồn hiện không gửi, luôn null). Producer gửi đủ các trường này từ STR-10.
- Cách validate, dedup, MERGE, bảng phụ poll (`silver.gbfs_station_information_polls`) và checkpoint giống Silver status (`jobs/silver.py`); checkpoint `SILVER_INFORMATION_CHECKPOINT_LOCATION` = `s3a://checkpoints/silver-information/`.
- Message information gửi trước khi có `poll_id` hoặc trước STR-10 thiếu trường: thiếu `poll_id` thì không hợp lệ và chỉ nằm ở Bronze; thiếu ba trường mới thì vẫn hợp lệ, ba trường là null.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/silver_information.py --available-now
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/dq_checks.py --layer silver_information
```

| Check `silver_information` | Mức | Ý nghĩa |
|---|---|---|
| `silver_information_grain_unique` | error | Không có hai dòng cùng `(station_id, poll_id)` |
| `silver_information_one_collected_at_per_poll` | error | Mỗi `poll_id` chỉ có một `collected_at` |
| `silver_information_has_every_valid_bronze_key` / `_only_valid_bronze_keys` | error | Silver khớp tập khóa hợp lệ của Bronze (không thiếu, không thừa) |
| `silver_information_required_not_null` | error | `station_id`, `poll_id`, `collected_at`, `last_updated`, `name`, `lat`, `lon` không null |
| `silver_information_coordinates_in_range` | error | `lat` trong [-90, 90], `lon` trong [-180, 180] |
| `silver_information_capacity_non_negative` | error | `capacity` không âm |
| `silver_information_poll_completeness` | warn | Snapshot có ít hơn 90% số trạm của snapshot lớn nhất |
| `silver_information_poll_time_conflicts` | warn | Message hợp lệ có `poll_id` đã ghi nhận với `collected_at` khác (bị giữ ngoài Silver) |
| `silver_information_short_name_present` | warn | Thiếu `short_name` (không map được với Trip History) |

Đối soát Bronze với Kafka (số dòng theo partition, offset trùng, số trạm mỗi `poll_id`, số snapshot):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/tests/integration/str08_bronze_check.py lakehouse.bronze.gbfs_status
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 --topic gbfs.station_status --time -1
```

## Chạy

Cần các service `kafka`, `spark-master`, `iceberg-rest` đang chạy. `spark-defaults.conf` đặt `lakehouse` làm catalog mặc định, nên mọi query Spark đều cần kết nối được `iceberg-rest`.

```bash
# Kiểm tra parse + validate với fixture (không cần Kafka)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/gbfs_history.py --fixture

# Đọc Kafka, in message hợp lệ và sai schema của từng micro-batch
docker compose exec -e STREAM_TRIGGER_INTERVAL="30 seconds" spark-master \
  /opt/spark/bin/spark-submit /opt/project/streaming/jobs/gbfs_history.py \
  --sink console --feed status --checkpoint-dir /tmp/str07-checkpoints

# Ghi Bronze liên tục (service; trigger STREAM_TRIGGER_INTERVAL, mặc định 5 phút)
docker compose --profile pipeline up -d spark-streaming
```

| Tham số | Ý nghĩa |
|---|---|
| `--sink bronze\|console` | `bronze` (mặc định): ghi bảng Bronze; `console`: in ra màn hình |
| `--feed status\|information\|all` | Feed cần đọc (mặc định `all`) |
| `--checkpoint-dir DIR` | Mỗi feed một thư mục `DIR/<feed>`. Không truyền thì Bronze dùng `STATUS_/INFORMATION_CHECKPOINT_LOCATION`, còn console dùng thư mục tạm và luôn đọc lại từ `STARTING_OFFSETS` |
| `--available-now` | Xử lý hết dữ liệu đang có trong Kafka rồi dừng (dùng cho test restart) |

## Queries, checkpoint và trigger (STR-11)

Service `spark-streaming` chạy **một ứng dụng Spark với 4 query**, mỗi query một checkpoint riêng trên MinIO:

| Query | Đọc | Ghi | Checkpoint |
|---|---|---|---|
| `gbfs_status_bronze` | Kafka `gbfs.station_status` | `bronze.gbfs_status` | `s3a://checkpoints/status-history/` |
| `gbfs_information_bronze` | Kafka `gbfs.station_information` | `bronze.gbfs_station_information` | `s3a://checkpoints/information-history/` |
| `gbfs_status_silver` | `bronze.gbfs_status` | `silver.gbfs_status` | `s3a://checkpoints/silver-status/` |
| `gbfs_information_silver` | `bronze.gbfs_station_information` | `silver.gbfs_station_information` | `s3a://checkpoints/silver-information/` |

Hai query "history" của contract mục 2.3.4 là hai query Bronze; hai query Silver đọc tiếp từ Bronze.

- **Trigger:** `STREAM_TRIGGER_INTERVAL`, mặc định **5 phút** (contract 2.3.3). Spark căn trigger theo mốc thời gian (…:00, …:05, …:10). Silver đọc phần Bronze đã commit ở trigger trước, nên Silver trễ Bronze 5–10 phút.
- **Kiểm tra trước khi chạy:** job in kế hoạch (`[plan] <query>: checkpoint=… trigger=… maxOffsetsPerTrigger=…`) và **dừng trước khi start bất kỳ query nào** nếu một query thiếu checkpoint hoặc hai query dùng chung / lồng checkpoint nhau.
- **`MAX_OFFSETS_PER_TRIGGER` = 100000** (mỗi query Kafka, mỗi micro-batch). Với trigger theo thời gian, batch xong sớm thì batch sau chờ tới mốc trigger kế tiếp (chỉ khi một batch chạy lâu hơn chu kỳ, batch sau mới bắt đầu ngay), nên một batch phải đọc hết số message đến trong một trigger: status khoảng 2519 × 5 = 12 595 message mỗi 5 phút. Với giới hạn cũ 5000, query status tụt thêm khoảng 7 600 message mỗi trigger (khoảng 2,2 triệu mỗi ngày). Về lý thuyết 100000 cho phép đọc nhanh hơn tốc độ đến khoảng 8 lần khi đuổi backlog; con số này **chưa được benchmark** với tài nguyên đã cấu hình.
- **Kafka group:** mỗi query dùng group-id prefix `<KAFKA_GROUP_ID>-<feed>`. Spark tự gán partition và giữ offset trong checkpoint, không commit lên Kafka, nên các group này không xuất hiện trong `kafka-consumer-groups --list`; độ trễ đọc theo dõi bằng log tiến độ.
- **Log tiến độ:** mỗi micro-batch một dòng `[progress] <query> batch=<n> at=<thời điểm> input_rows=<n> duration_ms=<n>`.
- **Restart:** `restart: unless-stopped`. Offset và tiến độ nằm trong checkpoint nên khởi động lại thì đọc tiếp, không trùng (xem STR-08, STR-09).

Biến môi trường dùng đúng tên trong `docker-compose.yml`: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID`, `GBFS_STATUS_TOPIC`, `GBFS_INFORMATION_TOPIC`, `STREAM_TRIGGER_INTERVAL` (mặc định 5 phút), `STATUS_CHECKPOINT_LOCATION`, `INFORMATION_CHECKPOINT_LOCATION`, cùng `STARTING_OFFSETS`, `MAX_OFFSETS_PER_TRIGGER`, `BRONZE_STATUS_TABLE`, `BRONZE_INFORMATION_TABLE`.

## Test

```bash
# Unit + contract tests (chạy trong image Spark, không cần stack)
docker run --rm -v "$PWD":/opt/project:ro -w /opt/project --entrypoint bash citibike/spark:3.5.6-local -c \
  'export PYTHONPATH=/opt/project:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip); python3 -W ignore::ResourceWarning -m unittest discover -s tests/streaming -v'

# Integration: đọc Kafka + restart từ checkpoint (dùng topic test riêng, tự xoá sau khi chạy)
bash tests/integration/str07_checkpoint_restart.sh

# Integration STR-08: Bronze khớp end offset Kafka, restart không trùng/không sót
# (topic, bảng, checkpoint riêng, tự xoá sau khi chạy)
bash tests/integration/str08_bronze.sh

# Integration STR-09: Silver dedup, idempotent khi chạy lại / mất checkpoint, DQ gate
bash tests/integration/str09_silver.sh

# Integration STR-10: Silver information đủ trường, snapshot, dedup, idempotent, DQ gate
bash tests/integration/str10_silver_information.sh

# STR-12: crash/restart Spark, replay, Iceberg không ghi trùng (Python trên host cần
# confluent-kafka; COMPOSE_PROJECT_NAME phải kết thúc bằng -itest; khoảng 20 phút)
python -m unittest tests.integration.test_spark_recovery -v
```

`test_matches_producer_json_schemas` so tên field với `ingestion/gbfs/json_schemas/*.schema.json` của producer; test này tự skip khi các file đó chưa có trên nhánh.

### Kết quả đã ghi nhận (23/11/2026)

| Kiểm tra | Kết quả |
|---|---|
| Unit + contract tests (`tests/streaming`) | 14/14 pass, gồm so field với JSON Schema của producer |
| `--fixture` (qua `spark-defaults.conf` thật) | PASSED |
| `tests/integration/str07_checkpoint_restart.sh` | 3/3: lần đầu đọc đủ 2 valid + 10 invalid; restart chỉ đọc 2 message mới; restart không có message mới thì không đọc lại gì |
| Dữ liệu thật: `gbfs-poller` (GBFS API) → Kafka → job `--available-now` | `status` 2519 valid / 0 invalid; `information` 2519 valid / 0 invalid |
| Service `spark-streaming` (client mode lên `spark-master` + `spark-worker`) | Batch 0 của mỗi feed: 2519 valid / 0 invalid |

Các lần chạy dùng một Compose project riêng (`COMPOSE_PROJECT_NAME=citibike-itest`, đổi cổng MinIO/Iceberg REST/Spark UI) để không đụng stack đang dùng.

### Kết quả STR-08 (23/11/2026)

| Kiểm tra | Kết quả |
|---|---|
| Unit tests (`tests/streaming`) | 19/19 pass (thêm 5 test Bronze: giữ mọi message kể cả sai, `raw_value` nguyên văn, Kafka key, kiểu cột khớp bảng) |
| `tests/integration/str08_bronze.sh` | 3/3: 12 message (2 valid + 10 invalid) → 12 dòng, khớp end offset từng partition; restart với 3 message mới → 15 dòng; restart không có gì mới → 15 dòng, không thêm snapshot; 0 offset trùng |
| `str07_checkpoint_restart.sh` (hồi quy, `--sink console`) | 3/3 |
| Dữ liệu thật: `gbfs-poller` → Kafka → service `spark-streaming` (checkpoint `s3a://checkpoints/...`), có restart service giữa chừng | `bronze.gbfs_status`: 4 lần polling, 10076 dòng = tổng end offset Kafka (3368 + 3428 + 3280), 0 offset trùng, mỗi `poll_id` đủ 2519 trạm. `bronze.gbfs_station_information`: 2519 dòng = end offset (842 + 857 + 820) |

Chưa kiểm tra: đọc Bronze bằng Trino; crash giữa lúc commit Iceberg (STR-12).

### Kết quả STR-09 (23/11/2026)

| Kiểm tra | Kết quả |
|---|---|
| Unit tests (`tests/streaming`) | 31/31 pass (thêm 12 test Silver + DQ: chỉ message hợp lệ, không tin cờ Bronze, dedup, poll không đổi vẫn giữ, kiểu cột; DQ pass khi sạch, fail khi trùng / thiếu / thừa khóa) |
| `tests/integration/str09_silver.sh` | 7/7: replay dedup + poll không đổi giữ + invalid loại → 3 dòng; chạy lại không đổi; mất checkpoint Silver, đọc lại toàn bộ Bronze → vẫn 3 dòng; thêm poll mới + replay → 4 dòng; DQ pass, và exit 1 khi chèn dòng trùng |
| Dữ liệu thật: `gbfs-poller` → Kafka → service `spark-streaming` (Bronze + Silver), restart giữa chừng | 3 lần polling → `silver.gbfs_status` 7557 dòng = 3 × 2519, 7557 khóa khác nhau; `dq_checks --layer silver_status` 7/7 PASS |

Ghi chú: khi mất checkpoint Silver và đọc lại Bronze, MERGE không thêm dòng nhưng Iceberg vẫn ghi một snapshot rỗng.
Sau review (24–25/11/2026): `str09_silver.sh` 12/12, thêm cùng khóa với `collected_at` lệch 1 phút và 2 ngày (vẫn 4 dòng; DQ pass, `silver_status_poll_time_conflicts` cảnh báo 2 dòng) và replay toàn bộ với session time zone `Asia/Ho_Chi_Minh`.
Chưa kiểm tra: đọc Silver bằng Trino; xóa snapshot cũ của Bronze (maintenance) khi query Silver đang chạy.

### Kết quả STR-10 (23/11/2026)

| Kiểm tra | Kết quả |
|---|---|
| Producer unit tests (`tests/unit`) | 58/58 pass (message information giữ `region_id`, `rental_uris`, `is_charging`; kiểu dữ liệu chặt) |
| Spark unit tests (`tests/streaming`) | 42/42 pass (thêm 11 test Silver information + DQ) |
| `tests/integration/str10_silver_information.sh` | 7/7: đủ trường; replay dedup; trạm đổi tên ở snapshot sau vẫn giữ cả hai phiên bản; message sai (lat 95, thiếu `poll_id`, không phải JSON) bị loại; chạy lại / mất checkpoint không thêm dòng; DQ exit 1 khi chèn dòng trùng |
| `tests/integration/str09_silver.sh` (hồi quy sau khi tách `jobs/silver.py`) | 7/7 |
| Dữ liệu thật: poller → Kafka → `spark-streaming` (Bronze + 2 query Silver), poller khởi động lại để có snapshot thứ hai | `silver.gbfs_station_information` 5038 dòng = 2 snapshot × 2519; `region_id` khác null 5014 = 2 × 2507 (12 trạm nguồn không có `region_id`), `rental_uris` và `is_charging` 5038; DQ `silver_information` 8/8 và `silver_status` 7/7 PASS |

### Kết quả STR-11 (24/11/2026)

| Kiểm tra | Kết quả |
|---|---|
| Spark unit tests (`tests/streaming`) | 50/50 pass (thêm 8 test kế hoạch query: 4 query với checkpoint riêng, `--checkpoint-dir`, `--available-now`/`--no-silver` chỉ chạy Bronze, console không cần checkpoint, từ chối khi thiếu / dùng chung / lồng checkpoint, mặc định trigger 5 phút và giới hạn offset đủ lớn) |
| Chạy thật **với trigger 5 phút mặc định**, khoảng 17 phút (poller dừng ở phút 11) | Trigger ở 00:35:00, 00:40:00, 00:45:00, 00:50:00. Batch Bronze status 12 595 dòng mỗi trigger (lớn hơn giới hạn 5000 cũ). Cuối lượt: `bronze.gbfs_status` 30 228 dòng = end offset Kafka từng partition (10104 / 10284 / 9840), 0 offset trùng; `bronze.gbfs_station_information` 2519 = end offset; `silver.gbfs_status` 30 228 = 12 lần polling × 2519; DQ `silver_status` và `silver_information` PASS |
| Hai query dùng chung checkpoint (`STATUS_` và `SILVER_STATUS_CHECKPOINT_LOCATION` cùng một đường dẫn) | Job dừng với `invalid streaming setup: gbfs_status_bronze and gbfs_status_silver share a checkpoint`, không query nào được start |
| Hồi quy `str08_bronze.sh`, `str09_silver.sh` | 3/3, 7/7 |

## Chịu lỗi Spark → Iceberg (STR-12)

`tests/integration/test_spark_recovery.py` chạy các kịch bản của contract mục 2.4 bằng lỗi thật trên stack thật, cho cả Bronze (Kafka → Iceberg append) và Silver (Bronze → Iceberg MERGE). Sau mỗi kịch bản kiểm tra: Bronze theo từng partition bằng end offset Kafka, không có `(topic, partition, offset)` trùng, Silver có đúng số khóa đã gửi. Test chỉ chạy khi `COMPOSE_PROJECT_NAME` kết thúc bằng `-itest`; topic, bảng, checkpoint có hậu tố theo lượt chạy; chỉ kill driver mang checkpoint của lượt đó; log đầy đủ từng bước được giữ trong thư mục in ra lúc bắt đầu.

| Kịch bản (contract 2.4) | Cách gây lỗi | Kết quả (25/11/2026, 7/7 pass, khoảng 23 phút) |
|---|---|---|
| Chạy bình thường 100 message | — | Bronze 100 dòng (30 / 40 / 30 = end offset), Silver 100 |
| Crash **sau Iceberg commit, trước checkpoint commit** | Xóa `commits/<n>` của batch cuối (test xác nhận file đã mất và batch `n` thành "chưa commit"), chạy lại | Batch `n` được chạy lại, thấy mọi message đã có: Bronze vẫn 200 dòng, **không thêm snapshot**, đọc được bình thường; Silver vẫn 200 |
| Crash **trong lúc ghi Iceberg** | SIGKILL driver Bronze khi batch in `appending N messages` (batch đầu, rồi batch thứ 4; 20 000 message, `MAX_OFFSETS_PER_TRIGGER=2000`); SIGKILL driver Silver khi batch in `[silver] merge` (ngay trước câu MERGE) | Exit 137. Test **assert** checkpoint có batch đã lên kế hoạch mà chưa commit (Bronze `[2]`, `[5]`; Silver `[2]`); Bronze chỉ giữ các batch đã commit (200 rồi 8 200 dòng), 0 trùng. Chạy lại: 20 200 = end offset từng partition, Silver 20 200. Kill có thể rơi ngay trước hoặc ngay sau commit Iceberg; cả hai trường hợp đều phải phục hồi như trên |
| Crash **sau khi checkpoint hoàn tất** | Chạy lại khi không có dữ liệu mới | Cả Bronze và Silver: không batch mới, không snapshot mới, `offsets/` và `commits/` không đổi |
| **MinIO mất giữa lượt chạy** | `docker pause minio` ngay sau batch đầu tiên của lượt 10 000 message | Lần này job **tự dừng sau 183 giây** (exit 1); lần chạy trước (bản `8be6bad`) nó treo trong retry hơn 300 giây và test phải kill. Bronze dừng ở 22 200 / 30 200 (các batch trước khi mất MinIO đã commit). Bật lại MinIO và chạy lại: 30 200 = end offset, Silver 30 200, 0 trùng |
| **Snapshot Bronze bị expire** (ngoài contract, theo review) | `expire_snapshots` giữ lại 1 snapshot khi Silver còn chưa đọc phần mới | Silver với checkpoint cũ dừng (exit 1). `silver_status.py --rebuild` với checkpoint mới: Silver 30 600 = toàn bộ khóa; checkpoint mới đọc tiếp lần append sau (30 700) |
| **Snapshot của batch vừa commit bị expire trước khi chạy lại** (theo review) | Xóa `commits/<n>` của batch cuối, compact Bronze (`rewrite_data_files`, snapshot mới cùng dữ liệu), `expire_snapshots` giữ 1 snapshot: snapshot do batch `n` tạo không còn. Test assert compaction viết lại file (19 file), snapshot hiện tại đã đổi và ID snapshot của batch `n` không còn trong lịch sử | Batch `n` chạy lại: `100 of 100 messages already in Bronze, skipped`, Bronze vẫn 30 800 dòng, vẫn 1 snapshot, 0 trùng |

| Tiêu chí | Kết luận |
|---|---|
| Fault tolerance | Phục hồi sau SIGKILL và sau khi MinIO trở lại bằng cách chạy lại (service: `restart: unless-stopped`); mất snapshot Bronze thì phục hồi bằng `--rebuild` |
| Offset recovery | Số dòng Bronze khớp end offset từng partition sau mọi lần restart |
| Replay | Batch đã lên kế hoạch mà chưa commit được xử lý lại với đúng offset trong `offsets/<n>` |
| Idempotency | Bronze: chỉ append message chưa có theo `(topic, partition, offset, kafka_timestamp)`; batch chạy lại không thêm dòng hay snapshot nào. Silver: MERGE chỉ insert theo `(station_id, poll_id)`, mỗi poll một `collected_at` |
| Data integrity | Không mất, không nhân đôi observation trong mọi kịch bản |

Checkpoint do Iceberg streaming sink bản cũ để lại được xử lý bằng cùng cơ chế (so theo vị trí Kafka trong dữ liệu) nhưng **không có kịch bản riêng**. Handoff `--rebuild` khi hai snapshot trùng millisecond cũng không tái hiện được trên stack thật; kịch bản 6 chỉ chạy nhánh "snapshot trước S đã bị expire".

**Lỗi kịch bản 2 phát hiện:** với Iceberg streaming sink (bản trước), batch đã commit được chạy lại trong một ứng dụng Spark mới ghi file trùng tên và ghi đè file đã commit (kích thước file trên MinIO đổi, ví dụ 6449 → 6458 byte), làm Bronze không đọc được nữa (`EOFException`), kéo theo Silver lỗi. Lần chạy 5/5 trước đó pass chỉ vì file ghi lại tình cờ cùng kích thước. Bản này dùng `foreachBatch` + append các message chưa có (mục Bronze ở trên).

**Khi MinIO mất giữa chừng:** service `spark-streaming` có thể treo trong retry thay vì thoát (lần thử pause MinIO *trước* khi khởi động thì job lỗi sau khoảng 190 giây). Test chỉ chứng minh **phục hồi sau khi kill và chạy lại**, không chứng minh service tự phục hồi: khi MinIO trở lại mà log không còn `[progress]`, chạy `docker compose restart spark-streaming`. Dữ liệu không mất.
