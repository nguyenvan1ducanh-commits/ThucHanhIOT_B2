"""
Bai 3 - Chuong trinh 1: Alert Producer
Tao direct exchange iot_alert_exchange va gui canh bao voi routing key
info / warning / critical.

(Mo rong) Moi canh bao cung duoc gui sang topic exchange iot_alert_topic
voi routing key alert.<muc> de all_alerts_consumer_bai3.py nhan tat ca.

Cach chay:
    python alert_producer_bai3.py                  # gui 3 canh bao mau: info, warning, critical
    python alert_producer_bai3.py --interactive    # tu nhap: <muc> <noi dung>, vd: warning Kho lanh 8 do
    python alert_producer_bai3.py --random 10      # gui 10 canh bao ngau nhien, moi 2 giay
"""
import argparse
import random
import time

import pika

from config import ALERT_EXCHANGE, ALERT_LEVELS, ALERT_TOPIC_EXCHANGE, get_connection

SAMPLE_ALERTS = [
    ("info", "Cam bien phong hoc A2 hoat dong binh thuong info"),
    ("warning", "Nhiet do phong may vuot nguong warning"),
    ("critical", "Cam bien kho lanh mat ket noi critical"),
]

RANDOM_TEXT = {
    "info": ["Gateway tang 3 khoi dong lai info", "Cam bien do am phong lab cap nhat firmware info"],
    "warning": ["Do am kho thuoc thap hon 40% warning", "Pin cam bien sensor02 con 15% warning"],
    "critical": ["Phat hien khoi tai phong server critical", "Mat dien tu lanh bao quan vaccine critical"],
}


def publish_alert(channel, level: str, message: str):
    props = pika.BasicProperties(content_type="text/plain",
                                 delivery_mode=pika.DeliveryMode.Persistent)
    # Direct exchange: chi queue bind dung routing key == level moi nhan
    channel.basic_publish(exchange=ALERT_EXCHANGE, routing_key=level,
                          body=message.encode("utf-8"), properties=props)
    # (Mo rong) Topic exchange: routing key dang alert.<level>
    channel.basic_publish(exchange=ALERT_TOPIC_EXCHANGE, routing_key=f"alert.{level}",
                          body=message.encode("utf-8"), properties=props)
    print(f"Da gui [{level}] -> {ALERT_EXCHANGE}: {message}")


def interactive_mode(channel):
    print("Nhap: <muc> <noi dung>  (muc = info | warning | critical), EXIT de thoat")
    while True:
        line = input("> ").strip()
        if not line:
            continue
        if line.upper() == "EXIT":
            break
        level, _, message = line.partition(" ")
        level = level.lower()
        if level not in ALERT_LEVELS or not message:
            print("[LOI] Sai cu phap. Vi du: warning Nhiet do phong may 36 do")
            continue
        publish_alert(channel, level, f"{message} {level}")


def main():
    parser = argparse.ArgumentParser(description="Alert Producer - Bai 3")
    parser.add_argument("--interactive", action="store_true", help="tu nhap canh bao tu ban phim")
    parser.add_argument("--random", type=int, default=0, metavar="N", help="gui N canh bao ngau nhien")
    parser.add_argument("--interval", type=float, default=2.0, help="khoang cach khi gui ngau nhien (giay)")
    args = parser.parse_args()

    connection = get_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=ALERT_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=ALERT_TOPIC_EXCHANGE, exchange_type="topic", durable=True)
    print(f"Da tao direct exchange: {ALERT_EXCHANGE}\n")

    try:
        if args.interactive:
            interactive_mode(channel)
        elif args.random > 0:
            for _ in range(args.random):
                level = random.choice(ALERT_LEVELS)
                publish_alert(channel, level, random.choice(RANDOM_TEXT[level]))
                connection.sleep(args.interval)
        else:
            for level, message in SAMPLE_ALERTS:
                publish_alert(channel, level, message)
                time.sleep(0.5)
    except (KeyboardInterrupt, EOFError):
        print()
    finally:
        if connection.is_open:
            connection.close()
        print("Da dong ket noi.")


if __name__ == "__main__":
    main()
