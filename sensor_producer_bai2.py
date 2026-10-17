"""
Bai 2 - Chuong trinh 1: Sensor Producer
Mo phong cam bien gui du lieu JSON moi 3 giay vao queue sensor_data_queue.

Cach chay:
    python sensor_producer_bai2.py                                      # chi sensor01
    python sensor_producer_bai2.py --devices sensor01 sensor02 sensor03 # (mo rong) 3 cam bien
    python sensor_producer_bai2.py --count 10                           # gui 10 vong roi dung
"""
import argparse
import json
import random
import time
from datetime import datetime

import pika

from config import QUEUE_BAI2, get_connection


def read_sensor(device_id: str) -> dict:
    """Sinh gia tri ngau nhien; khoang gia tri cho phep thinh thoang vuot nguong canh bao."""
    return {
        "device_id": device_id,
        "temperature": round(random.uniform(24.0, 38.0), 1),
        "humidity": round(random.uniform(35.0, 85.0), 1),
        "timestamp": datetime.now().isoformat(timespec="seconds"),  # (mo rong) thoi gian gui
    }


def main():
    parser = argparse.ArgumentParser(description="Sensor Producer - Bai 2")
    parser.add_argument("--devices", nargs="+", default=["sensor01"], help="danh sach device_id")
    parser.add_argument("--interval", type=float, default=3.0, help="chu ky gui (giay), mac dinh 3")
    parser.add_argument("--count", type=int, default=0, help="so vong gui, 0 = gui mai mai")
    args = parser.parse_args()

    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_BAI2, durable=True)

    print(f"Gui du lieu vao queue [{QUEUE_BAI2}] moi {args.interval} giay (Ctrl+C de dung)\n")
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            for device_id in args.devices:
                payload = json.dumps(read_sensor(device_id))
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE_BAI2,
                    body=payload.encode("utf-8"),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=pika.DeliveryMode.Persistent,
                    ),
                )
                print(f"[{datetime.now():%H:%M:%S}] Da gui: {payload}")
            sent += 1
            if args.count == 0 or sent < args.count:
                # sleep qua connection de van giu heartbeat voi broker
                connection.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nDung cam bien.")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
