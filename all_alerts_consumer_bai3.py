"""
Bai 3 - (Mo rong) All Alerts Consumer
Dung topic exchange iot_alert_topic, bind queue all_alerts_queue bang pattern "alert.#"
de nhan TAT CA canh bao (info, warning, critical) va ghi log ra file alerts_log.txt.

Cach chay:
    python all_alerts_consumer_bai3.py
"""
from datetime import datetime

from config import ALERT_TOPIC_EXCHANGE, get_connection

QUEUE_NAME = "all_alerts_queue"
BINDING_PATTERN = "alert.#"   # '#' khop moi muc: alert.info, alert.warning, alert.critical
LOG_FILE = "alerts_log.txt"


def on_message(channel, method, properties, body):
    message = body.decode("utf-8", errors="replace")
    level = method.routing_key.split(".")[-1].upper()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{now} [{level}] {message}"
    print(f"[{QUEUE_NAME}] {line}")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = get_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=ALERT_TOPIC_EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=ALERT_TOPIC_EXCHANGE, routing_key=BINDING_PATTERN)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    print(f"Queue [{QUEUE_NAME}] da bind voi topic exchange [{ALERT_TOPIC_EXCHANGE}] bang '{BINDING_PATTERN}'")
    print(f"Nhan tat ca canh bao, ghi log vao {LOG_FILE}  (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung all-alerts consumer.")
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
