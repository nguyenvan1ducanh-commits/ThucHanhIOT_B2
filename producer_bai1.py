"""
Bai 1 - Chuong trinh 1: Producer
Gui message chao mung vao queue iot_lab_queue.

Cach chay:
    python producer_bai1.py              # gui 1 message
    python producer_bai1.py -n 5         # (mo rong) gui 5 message lien tiep
    python producer_bai1.py -n 5 -i 2    # 5 message, cach nhau 2 giay
"""
import argparse
import time

import pika

from config import QUEUE_BAI1, STUDENT_ID, STUDENT_NAME, get_connection

GREETING = "Xin chao tu ung dung Python AMQP"


def main():
    parser = argparse.ArgumentParser(description="AMQP Producer - Bai 1")
    parser.add_argument("-n", "--count", type=int, default=1, help="so message can gui")
    parser.add_argument("-i", "--interval", type=float, default=1.0, help="khoang cach giua 2 lan gui (giay)")
    args = parser.parse_args()

    connection = get_connection()
    channel = connection.channel()
    # durable=True: queue van con khi RabbitMQ khoi dong lai
    channel.queue_declare(queue=QUEUE_BAI1, durable=True)

    try:
        for i in range(1, args.count + 1):
            message = f"{GREETING} - {STUDENT_ID} - {STUDENT_NAME}"
            if args.count > 1:
                message += f" (#{i})"
            channel.basic_publish(
                exchange="",                 # default exchange: routing_key = ten queue
                routing_key=QUEUE_BAI1,
                body=message.encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="text/plain",
                    delivery_mode=pika.DeliveryMode.Persistent,  # message duoc luu xuong dia
                ),
            )
            print(f"Da gui vao queue [{QUEUE_BAI1}]: {message}")
            if i < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nDung gui.")
    finally:
        connection.close()
        print("Da dong ket noi.")


if __name__ == "__main__":
    main()
