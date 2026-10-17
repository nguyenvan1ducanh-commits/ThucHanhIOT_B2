"""
Bai 3 - Chuong trinh 3: Critical Consumer
Tao queue critical_queue, bind voi exchange iot_alert_exchange bang routing key "critical".
Chi nhan cac canh bao muc critical.

Cach chay:
    python critical_consumer_bai3.py
"""
from datetime import datetime

from config import ALERT_EXCHANGE, get_connection

QUEUE_NAME = "critical_queue"
ROUTING_KEY = "critical"


def on_message(channel, method, properties, body):
    message = body.decode("utf-8", errors="replace")
    print(f"[{QUEUE_NAME}] Da nhan: {message}   ({datetime.now():%H:%M:%S}, routing key = {method.routing_key})")
    print("   !!! Canh bao nghiem trong - can xu ly ngay !!!")
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = get_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=ALERT_EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=ALERT_EXCHANGE, routing_key=ROUTING_KEY)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    print(f"Queue [{QUEUE_NAME}] da bind voi [{ALERT_EXCHANGE}] bang routing key '{ROUTING_KEY}'")
    print("Dang cho canh bao muc critical...  (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung critical consumer.")
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
