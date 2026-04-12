from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

user_windows = defaultdict(deque)

WINDOW_SECONDS = 60
LIMIT = 3

print("Consumer started...")

for msg in consumer:
    tx = msg.value

    user_id = tx["user_id"]
    timestamp = datetime.fromisoformat(tx["timestamp"])

    user_windows[user_id].append(timestamp)

    window = user_windows[user_id]
    while window and (timestamp - window[0]).total_seconds() > WINDOW_SECONDS:
        window.popleft()

    if len(window) > LIMIT:
        print("ALERT: ANOMALY DETECTED")
        print(f"User: {user_id}")
        print(f"Transactions in last 60s: {len(window)}")
        print(f"Timestamps: {list(window)}")
        print("-" * 50)
    else:
        print(f"OK {user_id} -> {len(window)} tx in last 60s")
