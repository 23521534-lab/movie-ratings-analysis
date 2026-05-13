import json, os
from datetime import datetime
from kafka import KafkaConsumer

os.makedirs("results", exist_ok=True)
RESULT_FILE = "results/results.json"

consumer = KafkaConsumer(
    'detection-results',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='storage-group'
)

def save_result(data):
    all_results = []
    if os.path.exists(RESULT_FILE):
        with open(RESULT_FILE, "r") as f:
            try:
                all_results = json.load(f)
            except:
                all_results = []
    data["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_results.append(data)
    with open(RESULT_FILE, "w") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

print("[Server 3] Storage Server đang chạy...")
print(f"[Server 3] Kết quả sẽ lưu vào: {RESULT_FILE}")

for message in consumer:
    data = message.value
    save_result(data)
    print(f"[Server 3] Đã lưu: {data['frame_id']} | Số người: {data['person_count']}")
