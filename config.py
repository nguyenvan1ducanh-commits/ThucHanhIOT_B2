"""
Cau hinh chung cho cac bai thuc hanh AMQP (RabbitMQ).
Sua cac gia tri ben duoi (hoac dat bien moi truong) truoc khi chay.
"""
import os
import sys

import pika

# ---------------- Thong tin sinh vien ----------------
STUDENT_NAME = os.getenv("STUDENT_NAME", "Nguyen Van Duc Anh")   # Nguyễn Văn Đức Anh
STUDENT_ID = os.getenv("STUDENT_ID", "B23DCCN040")

# ---------------- Cau hinh RabbitMQ broker ----------------
# Mac dinh: RabbitMQ cai tren may, tai khoan mac dinh guest/guest, cong 5672.
# Neu dung broker cua lop hoac CloudAMQP thi dat bien moi truong AMQP_URL, vd:
#   amqps://user:pass@host/vhost
#
# Cach don gian nhat khi dung CloudAMQP: tao file amqp_url.txt canh file nay,
# dan AMQP URL vao (1 dong). File nay chua mat khau -> KHONG tai len GitHub.
_URL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "amqp_url.txt")
if os.getenv("AMQP_URL"):
    AMQP_URL = os.environ["AMQP_URL"]
elif os.path.exists(_URL_FILE):
    with open(_URL_FILE, encoding="utf-8") as _f:
        AMQP_URL = _f.read().strip()
else:
    AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"

# ---------------- Ten queue / exchange dung trong bai ----------------
QUEUE_BAI1 = "iot_lab_queue"
QUEUE_BAI2 = "sensor_data_queue"
ALERT_EXCHANGE = "iot_alert_exchange"          # direct exchange (Bai 3)
ALERT_TOPIC_EXCHANGE = "iot_alert_topic"       # topic exchange (mo rong Bai 3)
ALERT_LEVELS = ("info", "warning", "critical")


def broker_name() -> str:
    """Dia chi broker de in ra man hinh (an mat khau)."""
    params = pika.URLParameters(AMQP_URL)
    return f"{params.host}:{params.port}"


def get_connection() -> pika.BlockingConnection:
    """Ket noi toi RabbitMQ, bao loi de hieu neu khong ket noi duoc."""
    params = pika.URLParameters(AMQP_URL)
    params.heartbeat = 60
    params.blocked_connection_timeout = 30
    try:
        conn = pika.BlockingConnection(params)
    except pika.exceptions.AMQPConnectionError as err:
        print(f"[LOI] Khong ket noi duoc RabbitMQ tai {broker_name()}: {err!r}")
        print("      Kiem tra RabbitMQ da chay chua, hoac bien AMQP_URL co dung khong.")
        sys.exit(1)
    print(f"[OK] Da ket noi RabbitMQ {broker_name()}")
    return conn
