import base64, json, time
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from ultralytics import YOLO
from PIL import Image
import io

print("[Server 2] Đang load model YOLOv8n...")
model = YOLO("yolov8n.pt")
print("[Server 2] Load model xong!")

consumer = KafkaConsumer(
    'camera-frames',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='processor-group'
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def detect_people(image_b64):
    image_bytes = base64.b64decode(image_b64)
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img_array = np.array(image)
    results = model(img_array, classes=[0], verbose=False)
    bounding_boxes = []
    for box in results[0].boxes:
        x1, y1, x2, y2 = box.xyxy[0].tolist()
        confidence = float(box.conf[0])
        bounding_boxes.append({
            "x1": round(x1, 2), "y1": round(y1, 2),
            "x2": round(x2, 2), "y2": round(y2, 2),
            "confidence": round(confidence, 4)
        })
    return bounding_boxes

print("[Server 2] Đang lắng nghe topic 'camera-frames'...")

for message in consumer:
    data = message.value
    frame_id  = data["frame_id"]
    timestamp = data["timestamp"]
    image_b64 = data["image_data"]
    print(f"[Server 2] Đang xử lý: {frame_id}")
    boxes = detect_people(image_b64)
    person_count = len(boxes)
    result = {
        "frame_id":       frame_id,
        "timestamp":      timestamp,
        "processed_at":   time.time(),
        "person_count":   person_count,
        "bounding_boxes": boxes
    }
    producer.send('detection-results', value=result)
    producer.flush()
    print(f"[Server 2] {frame_id} → Phát hiện {person_count} người, đã gửi kết quả.")
