# Hệ thống xử lý hồ sơ và hành vi người dùng theo thời gian thực

Đồ án xây dựng pipeline thu thập, xử lý và lưu dữ liệu hồ sơ người dùng cùng sự kiện hành vi.

## Kiến trúc

```text
Random User API -> Airflow random_people_names -> Kafka random_names ----+
                                                                        +-> Spark Structured Streaming -> Cassandra
Activity Generator -> Airflow user_activity_stream -> Kafka user_activities
```

- **Airflow** chạy hai DAG định kỳ và quản lý retry.
- **Kafka** lưu luồng JSON cho hai loại dữ liệu.
- **Spark Structured Streaming** đọc hai topic đồng thời, kiểm tra schema, loại bản ghi không hợp lệ và xử lý theo micro-batch.
- **Cassandra** lưu hồ sơ và hoạt động đã xử lý.
- **Docker Compose** chạy các dịch vụ trên cùng mạng Docker.

## Cấu trúc dự án

| Tệp | Vai trò |
|---|---|
| [stream_to_kafka.py](stream_to_kafka.py) | Gọi Random User API và gửi hồ sơ JSON vào `random_names`. |
| [stream_to_kafka_dag.py](stream_to_kafka_dag.py) | DAG `random_people_names`. |
| [activity_generator.py](activity_generator.py) | Tạo và gửi một sự kiện hành vi vào `user_activities`. |
| [activity_generator_dag.py](activity_generator_dag.py) | DAG `user_activity_stream`. |
| [spark_streaming.py](spark_streaming.py) | Đọc Kafka, kiểm tra dữ liệu, ghi micro-batch vào Cassandra. |
| [tests/test_spark_streaming.py](tests/test_spark_streaming.py) | Kiểm thử schema/validation Spark bằng dữ liệu mẫu, không ghi vào Kafka/Cassandra. |
| [docker-compose.yml](docker-compose.yml) | Kafka, Spark, Cassandra và dịch vụ liên quan. |
| [docker-compose-LocalExecutor.yml](docker-compose-LocalExecutor.yml) | Airflow LocalExecutor và PostgreSQL. |
| [Dockerfile.airflow](Dockerfile.airflow) | Tạo image Airflow có thư viện Python trong `requirements.txt`. |

## Phiên bản và kết nối

- Spark `3.4.1`, Scala `2.12.17`.
- Kafka connector `spark-sql-kafka-0-10_2.12:3.4.1`.
- Cassandra connector `spark-cassandra-connector_2.12:3.4.0`.
- DataStax Java driver `java-driver-core-shaded:4.13.0`.
- Kafka brokers nội bộ: `kafka1:19092,kafka2:19093,kafka3:19094`.
- Spark Master: `spark://spark:7077`.
- Cassandra: `cassandra:9042`.

Các connector được khai báo cùng một chỗ trong lệnh `spark-submit` bằng `--packages`; không ghép thêm JAR rời phiên bản khác.

## Khởi động

Mở PowerShell tại thư mục dự án. Mạng dùng chung phải tồn tại trước khi khởi động:

```powershell
docker network inspect airflow-kafka *> $null
if ($LASTEXITCODE -ne 0) { docker network create airflow-kafka }
```

Khởi động các dịch vụ bằng cả hai file Compose:

```powershell
docker compose -f docker-compose.yml -f docker-compose-LocalExecutor.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose-LocalExecutor.yml ps
```

Lệnh trên không xóa volumes. Không thêm `-v` vào lệnh `down`.

ZooKeeper và Kafka được cấu hình healthcheck/restart policy; các broker phụ thuộc vào trạng thái healthy của ZooKeeper. Thay đổi này đã được xác nhận bằng `docker compose config` và thử healthcheck ZooKeeper, nhưng chưa áp dụng bằng cách recreate các container đang chạy để tránh gián đoạn Kafka/Spark. Khi cần áp dụng cấu hình mới, chọn thời điểm có thể gián đoạn ngắn, kiểm tra trạng thái Spark trước và sau lệnh `up`; không xóa volumes.

Giao diện:

- Airflow: <http://localhost:8080>
- Kafka UI: <http://localhost:8888>
- Spark Master UI: <http://localhost:8085>
- Cassandra native transport: `localhost:9042`

Đợi Kafka và Cassandra khởi động hoàn tất trước khi chạy pipeline.

### Kiểm tra schema Cassandra

Schema hiện có gồm bảng legacy `spark_streaming.random_names` với `full_name` làm khóa chính, bảng mới `spark_streaming.random_names_by_id` với `profile_id` làm khóa chính và `spark_streaming.user_activities` với `event_id` làm khóa chính. Kiểm tra mà không thay đổi dữ liệu:

```powershell
docker exec cassandra cqlsh -e "DESCRIBE KEYSPACE spark_streaming;"
```

Nếu triển khai mới hoàn toàn, chỉ tạo schema khi chưa tồn tại:

```sql
CREATE KEYSPACE IF NOT EXISTS spark_streaming
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS spark_streaming.random_names (
    full_name text PRIMARY KEY,
    gender text,
    location text,
    city text,
    country text,
    postcode text,
    latitude float,
    longitude float,
    email text
);

CREATE TABLE IF NOT EXISTS spark_streaming.random_names_by_id (
    profile_id text PRIMARY KEY,
    full_name text,
    gender text,
    location text,
    city text,
    country text,
    postcode text,
    latitude float,
    longitude float,
    email text
);

CREATE TABLE IF NOT EXISTS spark_streaming.user_activities (
    event_id text PRIMARY KEY,
    user_id text,
    event_type text,
    event_time timestamp,
    device text,
    product_id text
);
```

Không chạy `DROP`, `TRUNCATE` hoặc lệnh xóa dữ liệu trên các bảng đang dùng.

## Chạy Airflow

Mở Airflow UI, kiểm tra và bật hai DAG:

- `random_people_names`
- `user_activity_stream`

Mỗi DAG được lập lịch mỗi phút để giảm tải cho Airflow và máy chạy Docker. Có thể bấm **Trigger DAG** để chạy thủ công. Task cần đạt trạng thái `success`; nếu thất bại, mở log của task để kiểm tra lỗi API hoặc Kafka.

Kiểm tra DAG import trong container:

```powershell
docker exec kafka_spark_structured_streaming-webserver-1 airflow list_dags
```

## Chạy Spark trên Master/Worker

Standalone cluster không hỗ trợ submit Python ở `cluster` deploy mode; dùng `client` deploy mode để driver ở `spark_master` kết nối Master và executor chạy trên Worker. Dùng hostname service `spark` cho RPC; tránh `spark_master` vì dấu gạch dưới làm Spark RPC URL không hợp lệ.

Chạy ở foreground để theo dõi log:

```powershell
docker exec -it spark_master /opt/spark/bin/spark-submit `
  --master spark://spark:7077 `
  --deploy-mode client `
  --conf spark.driver.host=spark `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.driver.port=39000 `
  --conf spark.blockManager.port=39001 `
  --conf spark.cores.max=2 `
  --conf spark.executor.cores=2 `
  --conf spark.cassandra.connection.host=cassandra `
  --conf spark.cassandra.connection.port=9042 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,com.datastax.oss:java-driver-core-shaded:4.13.0 `
  /home/spark_streaming.py
```

Các lỗi ghi Cassandra do kết nối tạm thời được thử tối đa ba lần (tính cả lần đầu), với thời gian chờ tăng dần giữa các lần. Nếu query vẫn kết thúc lỗi (ví dụ Kafka hoặc Cassandra mất kết nối quá lâu), ứng dụng ghi exception và khởi động lại cả hai query sau 10 giây từ checkpoint hiện có. Vì vậy, khi theo dõi cần kiểm tra log micro-batch/retry và trạng thái query, không chỉ dựa vào việc driver còn chạy.

Hai checkpoint được lưu ổn định trong volume `spark-data`:

- `/opt/spark/work-dir/checkpoints/cluster-v1/random_names`
- `/opt/spark/work-dir/checkpoints/cluster-v1/user_activities`

Giữ nguyên checkpoint khi khởi động lại ứng dụng để Spark tiếp tục từ offset đã commit. Không xóa checkpoint hoặc volume trong khi chưa quyết định rõ có cần phát lại dữ liệu hay không.

## Kiểm tra pipeline

### Kiểm thử validation (không ảnh hưởng dữ liệu thật)

Các bài test tạo Spark DataFrame cục bộ với JSON mẫu, gồm dữ liệu hợp lệ, thiếu/null, sai định dạng, timestamp không hợp lệ và tọa độ ngoài miền. Chúng không kết nối Kafka/Cassandra và không cần xóa checkpoint.

Chạy từ PowerShell:

```powershell
docker exec spark_master /opt/spark/bin/spark-submit --master "local[1]" --conf spark.ui.enabled=false /home/tests/test_spark_streaming.py
```

Kết quả mong đợi là hai test `... ok` và `OK`. Mỗi lần test tạo một Spark session local ngắn hạn; không submit thêm streaming application lên cluster.

### Kiểm thử end-to-end (ghi thêm dữ liệu bình thường)

1. **Kiểm tra dịch vụ.** `docker ps` cần hiển thị ba Kafka broker, ZooKeeper, Cassandra, Airflow và Spark Master/Worker là `Up`. Không cần Kafka Connect/Schema Registry cho luồng chính.
2. **Kiểm tra Kafka.** Chạy hai lệnh `kafka-topics --describe` ở mục Kafka bên dưới. Xác nhận topic tồn tại và ISR/leader có mặt.
3. **Kiểm tra Airflow.** Mở <http://localhost:8080>; xác nhận DAG `random_people_names` và `user_activity_stream` có run mới `success`. Lịch sử run Failed cũ không tự mất và không đồng nghĩa các run mới vẫn lỗi.
4. **Kiểm tra Spark trước khi chạy.** Mở <http://localhost:8085/json/>. Nếu `activeapps` đã có `UserProfileAndActivityStreaming` ở `RUNNING`, không submit thêm driver. Nếu chưa có app, chạy duy nhất một lần lệnh Spark ở trên; giữ nguyên hai checkpoint.
5. **Theo dõi log.** Chạy `docker exec spark_master sh -c "tail -f /tmp/spark_streaming-recovery.log"` khi ứng dụng được khởi động theo cấu hình hiện tại, hoặc theo dõi terminal nếu chạy foreground. Cần thấy `Profile batch` và `Activity batch` với số `valid_unique_*` lớn hơn 0.
6. **Xác nhận Cassandra.** Chạy các lệnh `SELECT count(*)` bên dưới hai lần cách nhau khoảng 30–60 giây. Khi DAG tiếp tục phát message và Spark xử lý, số lượng tăng. Đọc vài dòng bằng `SELECT ... LIMIT 5` để kiểm tra nội dung.
7. **Lưu minh chứng.** Chụp Airflow run success, Kafka UI hai topic, Spark UI application/executor running, log micro-batch và CQL row counts. Ghi thời điểm chụp vì DAG chạy liên tục nên số đếm thay đổi.

Không cần clear/trigger hàng loạt DAG để kiểm thử. Nếu muốn bấm **Trigger DAG**, chỉ trigger một lần mỗi DAG và chờ task hoàn thành trước khi trigger lại.

### Kiểm tra trạng thái DAG bằng metadata database (chỉ đọc)

```powershell
docker exec kafka_spark_structured_streaming-postgres-1 psql -U airflow -d airflow -c "SELECT dag_id, state, execution_date FROM dag_run WHERE dag_id IN ('random_people_names','user_activity_stream') ORDER BY execution_date DESC LIMIT 10;"
```

### Kiểm tra trạng thái Spark bằng API (chỉ đọc)

```powershell
docker exec spark_master python3 -c "import json,urllib.request; d=json.load(urllib.request.urlopen('http://localhost:8080/json/')); print('apps:',[(a['id'],a['name'],a['state']) for a in d['activeapps']]); print('workers:',[(w['state'],w['coresused'],w['cores']) for w in d['workers']])"
```

Điều kiện xác nhận: có đúng một app `UserProfileAndActivityStreaming` trạng thái `RUNNING`, Worker trạng thái `ALIVE` và `coresused` lớn hơn 0.

### Docker và Spark

```powershell
docker ps
curl.exe http://localhost:8085/json/
docker logs --since 10m spark_worker
```

Trong JSON của Spark Master, xác nhận `aliveworkers` lớn hơn 0, `activeapps` có ứng dụng `UserProfileAndActivityStreaming` ở trạng thái `RUNNING`, và Worker có `coresused` lớn hơn 0. `FINISHED` không chứng minh streaming query đang hoạt động.

### Kafka

```powershell
docker exec kafka_spark_structured_streaming-kafka1-1 kafka-topics --bootstrap-server kafka1:19092 --describe --topic random_names
docker exec kafka_spark_structured_streaming-kafka1-1 kafka-topics --bootstrap-server kafka1:19092 --describe --topic user_activities
```

Trong Kafka UI, mở từng topic để xem JSON message. Profile mới có `profile_id` lấy từ `login.uuid` của Random User API. Activity có `event_id` UUID.

### Cassandra

```powershell
docker exec cassandra cqlsh -e "SELECT count(*) FROM spark_streaming.random_names;"
docker exec cassandra cqlsh -e "SELECT count(*) FROM spark_streaming.random_names_by_id;"
docker exec cassandra cqlsh -e "SELECT count(*) FROM spark_streaming.user_activities;"
docker exec cassandra cqlsh -e "SELECT * FROM spark_streaming.random_names_by_id LIMIT 10;"
docker exec cassandra cqlsh -e "SELECT * FROM spark_streaming.user_activities LIMIT 10;"
```

Log micro-batch trong tiến trình Spark có số bản ghi nhận, số bản ghi hợp lệ duy nhất và số bản ghi bị loại. Log của lần phục hồi gần nhất:

```powershell
docker exec spark_master sh -c "tail -n 100 /tmp/spark_streaming-recovery.log"
```

## Chống trùng và giới hạn schema hồ sơ

- Activity được kiểm tra theo `event_id` và chống trùng trong mỗi batch. Cassandra cũng dùng `event_id` làm primary key nên retry ghi là idempotent.
- Profile được kiểm tra và chống trùng trong mỗi batch theo `profile_id`, không theo `full_name`.
- Spark dual-write các hồ sơ mới vào `random_names_by_id` (khóa `profile_id`) và bảng legacy `random_names` để tương thích với báo cáo cũ.
- Migration đã áp dụng theo cách additive: bảng legacy và dữ liệu cũ không bị sửa/xóa. Các message lịch sử trước khi producer phát `profile_id` không thể backfill đáng tin cậy vào bảng mới; dữ liệu profile từ sau thay đổi sẽ có định danh riêng ngay cả khi trùng tên.

## Lỗi thường gặp

- **`Failed to find data source: kafka`**: dùng `--packages` với Kafka connector đúng Spark/Scala version như lệnh ở trên.
- **Thiếu class DataStax**: đảm bảo có `java-driver-core-shaded:4.13.0` cùng Cassandra connector `3.4.0`.
- **`Invalid Spark URL ... spark_master` / executor liên tục thoát**: đặt `spark.driver.host=spark`, không dùng hostname có dấu gạch dưới.
- **Cassandra `Connection refused`**: xác nhận `cassandra` phân giải đúng trong container Spark, kiểm tra `docker exec cassandra nodetool status`, rồi xem log `Transient Cassandra write failure`. Ứng dụng thử lại lỗi kết nối tạm thời; lỗi kéo dài sẽ được ghi rõ và làm query khởi động lại từ checkpoint.
- **Kafka offset hết hạn**: giữ checkpoint ổn định và kiểm tra log. Cấu hình hiện không tắt `failOnDataLoss`, nên lỗi mất offset không bị bỏ qua âm thầm.
- **Kafka báo `NodeExists` khi đăng ký `/brokers/ids/<id>`**: kiểm tra ZooKeeper và bảo đảm nó đã sẵn sàng; nếu Kafka vừa khởi động trong lúc ZooKeeper đang khôi phục session cũ, broker có thể thoát vì znode tạm thời còn tồn tại. Không xóa ZooKeeper/Kafka data. Sau khi xác nhận ZooKeeper đang chạy và các Kafka container đã dừng, có thể khởi động lại đúng các container broker hiện có bằng `docker start <tên-container-broker>` rồi xác nhận bằng `kafka-broker-api-versions` và trạng thái ISR của topic.
- **Airflow task `NoBrokersAvailable` / `DNS lookup failed for kafkaN`**: kiểm tra Kafka brokers trước. DAG có thể tiếp tục chạy theo lịch; không cần clear hàng loạt các lần chạy cũ.
- **Không có dữ liệu Cassandra**: xác nhận DAG gửi message sau khi Spark start (offset khởi đầu mới dùng `latest`), xem log `Profile batch` / `Activity batch`, kiểm tra schema và kết nối.
- **Compose báo cấu hình volume lỗi**: dùng bản Compose đã căn chỉnh khai báo volume cho cả webserver và scheduler; xác nhận trước bằng `docker compose -f docker-compose.yml -f docker-compose-LocalExecutor.yml config --quiet`.

## Kết quả kiểm thử thực tế gần nhất

Tại lần kiểm tra ngày 03/11/2026:

- Bộ test mới `tests/test_spark_streaming.py` đã chạy trong Spark `3.4.1` bằng Spark local mode: **2/2 test PASS** (profile schema/UUID và activity schema/timestamp; kiểm tra JSON hợp lệ, thiếu/null, malformed và field sai). Test chỉ tạo DataFrame cục bộ, không ghi vào Kafka/Cassandra. Spark in một số Python `ResourceWarning` từ socket Py4J nhưng cả hai test đều `ok`, kết quả cuối `OK`.
- Sau Docker restart, cả ba Kafka broker ban đầu ở trạng thái `Exited (1)`. Airflow task logs có `NoBrokersAvailable`; broker logs xác nhận `KeeperException$NodeExistsException` khi đăng ký broker ID vào ZooKeeper. Sau khi ZooKeeper ổn định và session cũ hết hạn, đã khởi động lại ba Kafka container hiện hữu (không xóa volumes/dữ liệu). Cả ba broker trả lời Kafka protocol; topic `random_names` có 3/3 replica ISR, `user_activities` hoạt động.
- Các DAG mới chạy thành công sau khi Kafka phục hồi. Các lần chạy `failed` cũ vẫn còn trong lịch sử Airflow; không clear/xóa chúng.
- Đã khởi động một Spark driver duy nhất trên cluster: `app-20261103103606-0000`, trạng thái `RUNNING`, một Worker dùng 2 cores. Cả hai query dùng lại checkpoint hiện hữu dưới `/opt/spark/work-dir/checkpoints/cluster-v1`.
- Trước lần chạy Spark này, Cassandra có 1.020 hồ sơ legacy, 70 hồ sơ theo UUID và 343 activity. Lần truy vấn mới nhất lúc 10:45 UTC: lần lượt **1.189**, **239** và **511**; tăng tương ứng 169 legacy profiles, 169 UUID profiles và 168 activities. Do Airflow phát dữ liệu liên tục, đây là số tích lũy giữa hai thời điểm đo, không phải số batch đơn lẻ. Log cũng ghi nhận profile batch 115 bản ghi hợp lệ và activity batch 113 bản ghi hợp lệ.
- Kafka Connect và Schema Registry vẫn ở trạng thái `Exited` tại lần kiểm tra cuối. Chúng không nằm trên đường đi dữ liệu Airflow → Kafka → Spark → Cassandra; Kafka UI và các dịch vụ chính vẫn hoạt động. Chưa khởi động lại hai dịch vụ phụ trợ để tránh tăng tải bộ nhớ trong lúc Cassandra và Spark đang chạy.
- **Trạng thái cuối được xác nhận:** Airflow có các DAG run mới `success`; Kafka brokers và topics hoạt động; Spark app `app-20261103103606-0000` đang `RUNNING`; Cassandra `nodetool status` trả `UN`; số liệu Cassandra tăng sau các micro-batch. Không xóa hoặc tạo lại checkpoint, volumes hay dữ liệu. Các healthcheck/restart policy Compose mới cần được áp dụng trong lần recreate có kiểm soát tiếp theo.
- Cấu hình Compose ghép được xác nhận hợp lệ.
- Airflow liệt kê được hai DAG; cả hai đã được trigger thủ công, và metadata database ghi nhận các DAG run thành công. Các run theo lịch mới nhất vẫn có thể ở trạng thái `running` tại thời điểm chụp.
- Hai Kafka topic có message JSON; đã xác nhận profile mới chứa `profile_id`.
- Spark `3.4.1` với Scala `2.12.17` đã resolve connector và chạy trên Standalone Master/Worker. App `app-20261103094352-0008` được Master báo `RUNNING`; Worker dùng 2 cores. Không có driver Spark khác đang hoạt động trong lần xác minh này.
- Log Spark ghi nhận các micro-batch: activity batch 1 nhận 24 bản ghi hợp lệ, profile batch 1 nhận 22 hồ sơ hợp lệ; các batch kế tiếp tiếp tục được xử lý.
- Cassandra được kiểm tra tăng từ 911 lên 933 hồ sơ và từ 227 lên 252 hoạt động trong lần đo sau khi ứng dụng cluster được khởi động. Đây là số liệu tích lũy, không phải số bản ghi của một DAG run riêng.
- Ở lần truy vấn Cassandra cuối ngày 03/11/2026, `random_names_by_id` có 55 hồ sơ có UUID (bảng mới có 0 trước lần chạy này), bảng legacy có 1005 hồ sơ (950 lúc bắt đầu kiểm tra) và `user_activities` có 323 sự kiện (262 lúc bắt đầu kiểm tra). Đây là số liệu tích lũy trong khi hai DAG tiếp tục chạy định kỳ, không phải số ghi của một run đơn lẻ; CQL đã đọc lại được `profile_id`, `full_name` và email từ bảng mới.
- Bài kiểm thử riêng gửi profile hợp lệ và ba loại sai (thiếu UUID, JSON lỗi, JSON null): batch hợp lệ ghi một hồ sơ; batch kế tiếp ghi `0`, rejected `3`. Bản ghi hợp lệ xuất hiện trong bảng legacy; lần xác minh sau migration đọc lại được các hồ sơ dual-write theo UUID.
- Bài kiểm thử riêng gửi activity hợp lệ và bốn loại sai (thiếu trường, event type/device/time sai, JSON lỗi/null): log ghi một event hợp lệ và tổng cộng bốn bản ghi bị loại; chỉ event hợp lệ xuất hiện trong Cassandra.
- Trong lần chạy sau cùng, Spark ghi log lỗi kết nối Cassandra tạm thời và phát cảnh báo retry lần 1/3; sau đó cả hai query tiếp tục xử lý nhiều micro-batch, Cassandra count activity tăng từ 286 lên 323, và app `app-20261103094352-0008` vẫn `RUNNING` với một executor 2 cores ở lần xác minh cuối. Không cố ý ngắt dịch vụ để thử phục hồi vì có thể làm gián đoạn pipeline đang dùng.

## Dừng ứng dụng

Nếu chạy foreground, nhấn `Ctrl+C` trong terminal đang chạy Spark. Nếu chạy nền, xác định chính xác PID của `spark-submit`/Python streaming app rồi gửi tín hiệu graceful tới đúng tiến trình; không dừng tiến trình Master/Worker.

Để dừng container mà vẫn giữ dữ liệu, có thể chạy `docker compose ... down` **không có** `-v`. Không dùng Docker prune hoặc xóa volumes.
