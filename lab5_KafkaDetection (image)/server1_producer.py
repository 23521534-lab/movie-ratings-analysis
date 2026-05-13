import os, time, base64, json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=10485760
)

IMAGE_DIR = "./images"

def send_images():
    image_files = [f for f in os.listdir(IMAGE_DIR)
                   if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
    if not image_files:
        print("Không tìm thấy ảnh trong thư mục images/")
        return
    for filename in image_files:
        filepath = os.path.join(IMAGE_DIR, filename)
        with open(filepath, "rb") as f:
            image_bytes = f.read()
        image_b64 = base64.b64encode(image_bytes).decode('utf-8')
        message = {
            "frame_id": filename,
            "timestamp": time.time(),
            "image_data": image_b64
        }
        producer.send('camera-frames', value=message)
        print(f"[Server 1] Đã gửi ảnh: {filename}")
        time.sleep(1)
    producer.flush()
    print("[Server 1] Hoàn thành gửi tất cả ảnh.")

if __name__ == "__main__":
    print("[Server 1] Camera Producer đang chạy...")
    send_images()
