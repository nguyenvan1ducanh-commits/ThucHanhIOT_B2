"""
Bai 1 - Chuong trinh 2: Consumer
Lang nghe queue iot_lab_queue, in noi dung message va thoi diem nhan.
Chay lien tuc cho toi khi nhan Ctrl+C.

Cach chay:
    python consumer_bai1.py
"""
from datetime import datetime

from config import QUEUE_BAI1, get_connection


def on_message(channel, method, properties, body):
    message = body.decode("utf-8", errors="replace")
    print(f"Da nhan message: {message}")
    print(f"Thoi gian nhan: {datetime.now():%H:%M:%S}")
    print("-" * 50)
    # Xac nhan da xu ly xong -> RabbitMQ xoa message khoi queue
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_BAI1, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_BAI1, on_message_callback=on_message)

    print(f"Dang lang nghe queue: {QUEUE_BAI1}  (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung consumer.")
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
