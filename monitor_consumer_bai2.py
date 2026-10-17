"""
Bai 2 - Chuong trinh 2: Monitoring Consumer
Nhan du lieu tu queue sensor_data_queue, in thong tin va canh bao:
    - temperature > 35  -> "CANH BAO: Nhiet do cao"
    - humidity    < 40  -> "CANH BAO: Do am thap"

Cach chay:
    python monitor_consumer_bai2.py
"""
import json
from datetime import datetime

from config import QUEUE_BAI2, get_connection

TEMP_MAX = 35.0
HUMIDITY_MIN = 40.0


def check_alerts(temperature: float, humidity: float) -> list:
    alerts = []
    if temperature > TEMP_MAX:
        alerts.append("CANH BAO: Nhiet do cao")
    if humidity < HUMIDITY_MIN:
        alerts.append("CANH BAO: Do am thap")
    return alerts


def on_message(channel, method, properties, body):
    raw = body.decode("utf-8", errors="replace")
    try:
        data = json.loads(raw)
        device = data["device_id"]
        temperature = float(data["temperature"])
        humidity = float(data["humidity"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        print(f"[LOI] Payload khong hop le, bo qua: {raw}\n")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"Device: {device}")
    print(f"Temperature: {temperature}")
    print(f"Humidity: {humidity}")
    if "timestamp" in data:
        print(f"Gui luc: {data['timestamp']}  |  Nhan luc: {datetime.now():%H:%M:%S}")
    for alert in check_alerts(temperature, humidity):
        print(alert)
    print("-" * 40)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_BAI2, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_BAI2, on_message_callback=on_message)

    print(f"Dang giam sat queue: {QUEUE_BAI2}  (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung giam sat.")
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
