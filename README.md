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

**Broker sử dụng khi làm bài:** RabbitMQ trên dịch vụ **CloudAMQP** (gói *Little Lemur* miễn phí), host `armadillo.rmq.cloudamqp.com`, cổng `5671` (AMQPS – AMQP có mã hoá TLS). Không cần cài RabbitMQ trên máy.

### Cấu hình

Địa chỉ broker được đọc theo thứ tự:

1. Biến môi trường `AMQP_URL` (nếu có).
2. File `amqp_url.txt` đặt cạnh `config.py`, chứa đúng 1 dòng AMQP URL.
3. Mặc định `amqp://guest:guest@localhost:5672/%2F` (RabbitMQ cài trên máy).

Cách làm với CloudAMQP:

1. Đăng ký tại https://www.cloudamqp.com, tạo instance gói **Little Lemur (Free)**.
2. Mở instance, copy **AMQP URL** (dạng `amqps://user:password@host/vhost`).
3. Tạo file `amqp_url.txt` trong thư mục project, dán URL vào và lưu lại.

File `amqp_url.txt` chứa mật khẩu broker nên **không được đưa lên GitHub** (đã có trong `.gitignore`).

Xem queue, exchange, binding trực tiếp trên web: trong trang CloudAMQP, bấm **RabbitMQ Manager**.

### Cách khác – RabbitMQ chạy trên máy

- **Windows:** cài Erlang/OTP (https://www.erlang.org/downloads) rồi RabbitMQ (https://www.rabbitmq.com/docs/install-windows). Bật giao diện web bằng `rabbitmq-plugins enable rabbitmq_management`, mở http://localhost:15672 (guest/guest).
- **Docker:** `docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management`

Khi đó không cần file `amqp_url.txt`, code tự dùng `localhost`.

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
{"device_id": "sensor01", "temperature": 29.5, "humidity": 62.1, "timestamp": "2026-10-18T01:13:30"}
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

Chạy trên Windows, broker CloudAMQP `armadillo.rmq.cloudamqp.com:5671`.

**Bài 1** – producer gửi 3 message (`python producer_bai1.py -n 3`):

```
Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN040 - Nguyen Van Duc Anh (#1)
Thoi gian nhan: 01:11:50
--------------------------------------------------
Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN040 - Nguyen Van Duc Anh (#2)
Thoi gian nhan: 01:11:51
--------------------------------------------------
Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN040 - Nguyen Van Duc Anh (#3)
Thoi gian nhan: 01:11:52
```

**Bài 2** – 3 cảm biến sensor01, sensor02, sensor03:

```
Device: sensor03
Temperature: 30.6
Humidity: 37.8
Gui luc: 2026-10-18T01:13:30  |  Nhan luc: 01:13:30
CANH BAO: Do am thap
----------------------------------------
Device: sensor03
Temperature: 37.8
Humidity: 67.0
Gui luc: 2026-10-18T01:13:33  |  Nhan luc: 01:13:33
CANH BAO: Nhiet do cao
```

**Bài 3** – producer gửi 3 cảnh báo info, warning, critical:

```
Da gui [info] -> iot_alert_exchange: Cam bien phong hoc A2 hoat dong binh thuong info
Da gui [warning] -> iot_alert_exchange: Nhiet do phong may vuot nguong warning
Da gui [critical] -> iot_alert_exchange: Cam bien kho lanh mat ket noi critical
```

```
[warning_queue] Da nhan: Nhiet do phong may vuot nguong warning   (01:14:54, routing key = warning)
```

```
[critical_queue] Da nhan: Cam bien kho lanh mat ket noi critical   (01:14:54, routing key = critical)
```

- `info`: không consumer nào trong hai consumer trên nhận được.
- `warning`: chỉ `warning_queue` nhận.
- `critical`: chỉ `critical_queue` nhận.

Mở rộng: chạy thêm `all_alerts_consumer_bai3.py` trước khi chạy producer thì queue `all_alerts_queue` (bind `alert.#` trên topic exchange `iot_alert_topic`) nhận cả 3 mức và ghi từng dòng vào `alerts_log.txt` theo dạng `thời gian [MỨC] nội dung`.

Tóm tắt:

- [x] Kết nối được RabbitMQ, gửi message vào queue, consumer nhận và hiển thị đúng (Bài 1)
- [x] Producer gửi nhiều message, consumer chạy đến khi Ctrl+C (mở rộng Bài 1)
- [x] Payload JSON, gửi tuần hoàn 3 giây, consumer phân tích và cảnh báo đúng ngưỡng (Bài 2)
- [x] Thêm trường thời gian gửi, mô phỏng 3 cảm biến (mở rộng Bài 2)
- [x] Tạo direct exchange, bind queue đúng routing key, message phân phối đúng nơi nhận (Bài 3)
- [x] Consumer nhận tất cả cảnh báo bằng topic exchange và ghi log ra file (mở rộng Bài 3)

## 6. Lưu ý / xử lý lỗi

- `Khong ket noi duoc RabbitMQ`: kiểm tra file `amqp_url.txt` có đúng URL không, máy có Internet không. Một số mạng (wifi trường, công ty) chặn cổng 5671; khi đó đổi `amqps://` thành `amqp://` trong URL để dùng cổng 5672.
- `ModuleNotFoundError: No module named 'pika'`: chạy lại `pip install -r requirements.txt`.
- Xem queue, exchange, binding và số message đang chờ trong **RabbitMQ Manager** của CloudAMQP (tab **Queues and Streams** và **Exchanges**).
- Nếu chạy `alert_producer_bai3.py` khi chưa từng chạy consumer, các queue chưa được tạo nên cảnh báo bị bỏ. Hãy chạy consumer ít nhất một lần trước.
