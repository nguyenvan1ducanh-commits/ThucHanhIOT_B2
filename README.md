# Thực hành IoT – Buổi 2: Lập trình Python với giao thức AMQP

- **Sinh viên:** Nguyễn Văn Đức Anh
- **Mã sinh viên:** B23DCCN040
- **Hình thức:** làm cá nhân

## 1. Cấu trúc thư mục

| File | Nội dung |
|---|---|
| `config.py` | Cấu hình chung: thông tin sinh viên, địa chỉ RabbitMQ, tên queue/exchange, hàm kết nối |
| `producer_bai1.py` | Bài 1 – Producer gửi lời chào vào queue `iot_lab_queue` |
| `consumer_bai1.py` | Bài 1 – Consumer nhận message, in nội dung và thời điểm nhận |
| `sensor_producer_bai2.py` | Bài 2 – Mô phỏng cảm biến, gửi JSON mỗi 3 giây vào `sensor_data_queue` |
| `monitor_consumer_bai2.py` | Bài 2 – Nhận dữ liệu, cảnh báo nhiệt độ > 35, độ ẩm < 40 |
| `alert_producer_bai3.py` | Bài 3 – Tạo direct exchange `iot_alert_exchange`, gửi cảnh báo info / warning / critical |
| `warning_consumer_bai3.py` | Bài 3 – Queue `warning_queue`, bind routing key `warning` |
| `critical_consumer_bai3.py` | Bài 3 – Queue `critical_queue`, bind routing key `critical` |
| `all_alerts_consumer_bai3.py` | Bài 3 (mở rộng) – Topic exchange, nhận tất cả cảnh báo và ghi log ra file |
| `requirements.txt` | Thư viện cần cài |
| `image/` | Ảnh chụp màn hình kết quả chạy thực tế |

## 2. Cài đặt môi trường

Yêu cầu Python 3.8 trở lên.

```bash
pip install -r requirements.txt      # cài pika
```

Họ tên và mã sinh viên được cấu hình ở `STUDENT_NAME`, `STUDENT_ID` trong `config.py`.

## 3. Broker sử dụng

**Broker sử dụng khi làm bài:** RabbitMQ chạy trên máy cá nhân (`localhost`), cổng AMQP `5672`, tài khoản mặc định `guest / guest`, virtual host `/`. Giao diện quản lý: http://localhost:15672.

Code đọc địa chỉ broker từ biến `AMQP_URL` trong `config.py`, mặc định:

```
amqp://guest:guest@localhost:5672/%2F
```

(`%2F` là virtual host `/` đã mã hoá.)

### Cách 1 – Cài RabbitMQ trên Windows

1. Cài **Erlang/OTP** (bản Windows 64-bit) tại https://www.erlang.org/downloads.
2. Cài **RabbitMQ** tại https://www.rabbitmq.com/docs/install-windows. Cài xong RabbitMQ tự chạy như một Windows service ở cổng 5672.
3. Bật giao diện quản lý web: mở **RabbitMQ Command Prompt (sbin dir)** trong Start menu và chạy

   ```bat
   rabbitmq-plugins enable rabbitmq_management
   ```

4. Mở http://localhost:15672, đăng nhập `guest` / `guest` để kiểm tra.

### Cách 2 – Docker

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### Cách 3 – Broker trên Internet (CloudAMQP) hoặc broker của lớp

Tạo instance miễn phí tại https://www.cloudamqp.com, copy **AMQP URL** rồi đặt biến môi trường trước khi chạy:

```powershell
# Windows PowerShell
$env:AMQP_URL="amqps://user:password@host/vhost"
```

```bash
# Linux / macOS
export AMQP_URL="amqps://user:password@host/vhost"
```

Tài khoản `guest` của RabbitMQ chỉ đăng nhập được từ chính máy chạy broker. Nếu dùng broker trên máy khác, cần tạo user riêng.

## 4. Cách chạy từng chương trình

Mỗi chương trình chạy trong **một terminal riêng**. Nên chạy consumer trước để queue và binding được tạo sẵn. Nhấn `Ctrl+C` để dừng consumer.

### Bài 1 – Producer → Queue → Consumer

```bash
# Terminal 1
python consumer_bai1.py            # chạy liên tục đến khi Ctrl+C
# Terminal 2
python producer_bai1.py            # gửi 1 message
python producer_bai1.py -n 5       # (mở rộng) gửi 5 message liên tiếp
```

Producer gửi qua **default exchange** với `routing_key = "iot_lab_queue"`. Queue khai báo `durable=True`, message gửi kiểu persistent, consumer gửi `ack` sau khi xử lý xong. Nếu chạy producer trước khi bật consumer, message vẫn nằm chờ trong queue, bật consumer lên sẽ nhận được.

### Bài 2 – Cảm biến gửi telemetry

```bash
# Terminal 1
python monitor_consumer_bai2.py
# Terminal 2
python sensor_producer_bai2.py                                       # sensor01, gửi mỗi 3 giây
python sensor_producer_bai2.py --devices sensor01 sensor02 sensor03  # (mở rộng) 3 cảm biến
```

Payload JSON (có thêm trường thời gian gửi – mở rộng):

```json
{"device_id": "sensor01", "temperature": 29.5, "humidity": 62.1, "timestamp": "2026-10-17T21:27:46"}
```

Nhiệt độ ngẫu nhiên 24–38, độ ẩm 35–85 để thỉnh thoảng có cảnh báo.

### Bài 3 – Điều phối cảnh báo với exchange

```bash
# Terminal 1
python warning_consumer_bai3.py
# Terminal 2
python critical_consumer_bai3.py
# Terminal 3 (mở rộng, không bắt buộc)
python all_alerts_consumer_bai3.py
# Terminal 4
python alert_producer_bai3.py                 # gửi 3 cảnh báo mẫu: info, warning, critical
python alert_producer_bai3.py --interactive   # tự nhập, vd: warning Nhiet do kho 38 do
python alert_producer_bai3.py --random 10     # gửi 10 cảnh báo ngẫu nhiên
```

Sơ đồ định tuyến:

```
                                   routing key "warning"   ┌───────────────┐
                              ┌──────────────────────────► │ warning_queue │ ──► warning_consumer
alert_producer ──► iot_alert_exchange (direct)             └───────────────┘
                              │    routing key "critical"  ┌────────────────┐
                              └──────────────────────────► │ critical_queue │ ──► critical_consumer
                                   "info": không có queue nào bind → bị bỏ   └────────────────┘

alert_producer ──► iot_alert_topic (topic) ── "alert.#" ──► all_alerts_queue ──► all_alerts_consumer ──► alerts_log.txt
```

## 5. Kết quả đạt được

### Ảnh chụp khi chạy thực tế

**Bài 1 – Producer / Consumer**

![Bài 1](image/bai1.png)

**Bài 2 – Cảm biến và giám sát**

![Bài 2](image/bai2.png)

**Bài 3 – Exchange và routing key**

![Bài 3](image/bai3.png)

### Output dạng văn bản

**Bài 1**

```
Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN040 - Nguyen Van Duc Anh
Thoi gian nhan: 21:27:43
```

**Bài 2**

```
Device: sensor02
Temperature: 37.2
Humidity: 39.7
Gui luc: 2026-10-17T21:27:46  |  Nhan luc: 21:27:46
CANH BAO: Nhiet do cao
CANH BAO: Do am thap
----------------------------------------
Device: sensor01
Temperature: 30.2
Humidity: 45.0
Gui luc: 2026-10-17T21:27:46  |  Nhan luc: 21:27:46
```

**Bài 3** – producer gửi 3 cảnh báo info, warning, critical:

```
Da gui [info] -> iot_alert_exchange: Cam bien phong hoc A2 hoat dong binh thuong info
Da gui [warning] -> iot_alert_exchange: Nhiet do phong may vuot nguong warning
Da gui [critical] -> iot_alert_exchange: Cam bien kho lanh mat ket noi critical
```

```
[warning_queue] Da nhan: Nhiet do phong may vuot nguong warning   (21:27:52, routing key = warning)
```

```
[critical_queue] Da nhan: Cam bien kho lanh mat ket noi critical   (21:27:53, routing key = critical)
```

- `info`: không consumer nào trong hai consumer trên nhận được.
- `warning`: chỉ `warning_queue` nhận.
- `critical`: chỉ `critical_queue` nhận.

Mở rộng – `all_alerts_queue` nhận cả 3 mức và ghi vào `alerts_log.txt`:

```
2026-10-17 21:27:52 [INFO] Cam bien phong hoc A2 hoat dong binh thuong info
2026-10-17 21:27:52 [WARNING] Nhiet do phong may vuot nguong warning
2026-10-17 21:27:53 [CRITICAL] Cam bien kho lanh mat ket noi critical
```

Tóm tắt:

- [x] Kết nối được RabbitMQ, gửi message vào queue, consumer nhận và hiển thị đúng (Bài 1)
- [x] Producer gửi nhiều message, consumer chạy đến khi Ctrl+C (mở rộng Bài 1)
- [x] Payload JSON, gửi tuần hoàn 3 giây, consumer phân tích và cảnh báo đúng ngưỡng (Bài 2)
- [x] Thêm trường thời gian gửi, mô phỏng 3 cảm biến (mở rộng Bài 2)
- [x] Tạo direct exchange, bind queue đúng routing key, message phân phối đúng nơi nhận (Bài 3)
- [x] Consumer nhận tất cả cảnh báo bằng topic exchange và ghi log ra file (mở rộng Bài 3)

## 6. Lưu ý / xử lý lỗi

- `Khong ket noi duoc RabbitMQ`: kiểm tra service RabbitMQ đã chạy chưa (Windows: mở **Services**, tìm *RabbitMQ*), hoặc `AMQP_URL` có đúng không.
- `ModuleNotFoundError: No module named 'pika'`: chạy lại `pip install -r requirements.txt`.
- Xem queue, exchange, binding và số message đang chờ trong giao diện http://localhost:15672 (tab **Queues and Streams** và **Exchanges**).
- Nếu chạy `alert_producer_bai3.py` khi chưa từng chạy consumer, các queue chưa được tạo nên cảnh báo bị bỏ. Hãy chạy consumer ít nhất một lần trước.
