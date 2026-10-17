"""
Bai 3 - Chuong trinh 2: Warning Consumer
Tao queue warning_queue, bind voi exchange iot_alert_exchange bang routing key "warning".
Chi nhan cac canh bao muc warning.

Cach chay:
    python warning_consumer_bai3.py
"""
from datetime import datetime

from config import ALERT_EXCHANGE, get_connection

QUEUE_NAME = "warning_queue"
ROUTING_KEY = "warning"


def on_message(channel, method, properties, body):
    message = body.decode("utf-8", errors="replace")
    print(f"[{QUEUE_NAME}] Da nhan: {message}   ({datetime.now():%H:%M:%S}, routing key = {method.routing_key})")
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = get_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=ALERT_EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=ALERT_EXCHANGE, routing_key=ROUTING_KEY)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    print(f"Queue [{QUEUE_NAME}] da bind voi [{ALERT_EXCHANGE}] bang routing key '{ROUTING_KEY}'")
    print("Dang cho canh bao muc warning...  (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung warning consumer.")
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
