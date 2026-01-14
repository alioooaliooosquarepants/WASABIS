"""
Combined MQTT ingestion + trigger-based retrainer.

Behavior:
- Subscribe to sensor data on `DATA_TOPIC`, append to CSV.
- On each new row, increment a counter and (re)start a debounce timer.
- When debounce expires or MIN_ROWS reached, spawn a worker process to train a DecisionTree,
  save the model and publish it to `MODEL_TOPIC` as base64 (optional HMAC signature).

Run:
    python receivetopkl.py

Notes:
- Keeps ingestion lightweight; training runs in a separate process to avoid blocking MQTT loop.
- You can still run a separate Streamlit dashboard that subscribes to the model topic.
"""

import os
import time
import json
import base64
import io
import hmac
import hashlib
import threading
import multiprocessing
import traceback
import subprocess
from datetime import datetime

import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt

from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# === Config ===
CSV_FILE = "river_data_log.csv"
MODEL_FILE = "decision_tree.pkl"

DATA_TOPIC = "river/monitoring/data"
NEW_DATA_TOPIC = "river/monitoring/new_data"
MODEL_TOPIC = "river/monitoring/model"

MQTT_BROKER = os.environ.get("MQTT_BROKER", "broker.hivemq.com")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))

# Trigger settings
DEBOUNCE_SEC = int(os.environ.get("DEBOUNCE_SEC", 60))
MIN_ROWS = int(os.environ.get("MIN_ROWS", 10))

# Optional shared secret for model HMAC signing (set to string to enable)
SHARED_SECRET = os.environ.get("MODEL_SHARED_SECRET")  # None or string


def ensure_csv_header():
    if not os.path.exists(CSV_FILE):
        df_init = pd.DataFrame(columns=[
            "timestamp",
            "datetime",
            "water_level_cm",
            "temperature_c",
            "humidity_pct",
            "danger_level",
            "rain_level"
        ])
        df_init.to_csv(CSV_FILE, index=False)


def append_row_to_csv(row):
    try:
        pd.DataFrame([row]).to_csv(CSV_FILE, mode="a", header=False, index=False)
    except Exception:
        # fallback: try atomic append via file write
        try:
            s = (f"{row['timestamp']},{row['datetime']},{row['water_level_cm']},"
                 f"{row['temperature_c']},{row['humidity_pct']},{row['danger_level']},{row['rain_level']}\n")
            with open(CSV_FILE, 'a', encoding='utf-8') as f:
                f.write(s)
        except Exception:
            print("Failed to append to CSV:", traceback.format_exc())


def load_and_preprocess_data():
    df = pd.read_csv(CSV_FILE)
    df = df.sort_values("timestamp")
    df = df[(df["water_level_cm"].between(0, 1000)) &
            (df["temperature_c"].between(-10, 80)) &
            (df["humidity_pct"].between(0, 100))]
    df = df.dropna()
    # Map danger_level to labels if numeric 0/1/2 else keep as is
    LABELS = ["Aman", "Waspada", "Bahaya"]
    df["danger_label"] = df["danger_level"].map({i: label for i, label in enumerate(LABELS)})
    df = df.dropna(subset=["danger_label"])
    df["water_level_norm"] = df["water_level_cm"] / 50.0
    df["water_rise_rate"] = df["water_level_cm"].diff().fillna(0)
    df["rain"] = (df["rain_level"] > 0).astype(int)
    return df


def train_model_and_publish(broker, port, model_topic, shared_secret=None):
    try:
        df = load_and_preprocess_data()
        if len(df) < 10:
            print("Not enough data for training, rows:", len(df))
            return

        features = ["water_level_norm", "water_rise_rate", "rain", "humidity_pct"]
        X = df[features]
        y = df["danger_label"]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        model = DecisionTreeClassifier(random_state=42)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        print(f"Model trained: accuracy={accuracy:.3f}, rows={len(df)}")

        # save canonical model and versioned file
        ts = int(time.time())
        versioned = f"decision_tree_{ts}.pkl"
        with open(versioned, 'wb') as f:
            pickle.dump(model, f)
        with open(MODEL_FILE, 'wb') as f:
            pickle.dump(model, f)

        # publish model over MQTT (base64)
        bio = io.BytesIO()
        pickle.dump(model, bio)
        model_bytes = bio.getvalue()
        model_b64 = base64.b64encode(model_bytes).decode('ascii')

        payload = {
            "timestamp": int(time.time()),
            "model_filename": versioned,
            "accuracy": float(accuracy),
            "model_b64": model_b64
        }
        if shared_secret:
            sig = hmac.new(shared_secret.encode('utf-8'), model_b64.encode('utf-8'), hashlib.sha256).hexdigest()
            payload['signature'] = sig

        client = mqtt.Client()
        client.connect(broker, port)
        client.loop_start()
        client.publish(model_topic, json.dumps(payload))
        time.sleep(0.3)
        client.loop_stop()
        client.disconnect()
        print("Published model to MQTT topic:", model_topic)

    except Exception:
        print("Training failed:", traceback.format_exc())


# === Trigger logic ===
counter_lock = threading.Lock()
counter = 0
debounce_timer = None


def start_training_worker():
    # spawn a separate process to run training to avoid blocking
    p = multiprocessing.Process(target=train_model_and_publish, args=(MQTT_BROKER, MQTT_PORT, MODEL_TOPIC, SHARED_SECRET))
    p.start()
    print("Spawned training worker pid=", p.pid)


def schedule_training():
    global counter, debounce_timer
    with counter_lock:
        if counter <= 0:
            return
        # reset counter before handing off
        counter = 0
    start_training_worker()


def reset_debounce_timer():
    global debounce_timer
    if debounce_timer and debounce_timer.is_alive():
        debounce_timer.cancel()
    debounce_timer = threading.Timer(DEBOUNCE_SEC, schedule_training)
    debounce_timer.daemon = True
    debounce_timer.start()


def on_message(client, userdata, message):
    global counter
    try:
        payload = message.payload.decode()
        data = json.loads(payload)
        row = {
            "timestamp": int(data.get("timestamp", time.time() * 1000)),
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "water_level_cm": float(data.get("water_level_cm", -1)),
            "temperature_c": float(data.get("temperature_c", -1)),
            "humidity_pct": float(data.get("humidity_pct", -1)),
            "danger_level": int(data.get("danger_level", 0)),
            "rain_level": int(data.get("rain_level", 0))
        }
        append_row_to_csv(row)
        print("Logged", row)

        # publish lightweight notice for other systems
        try:
            client.publish(NEW_DATA_TOPIC, json.dumps({"rows": 1, "timestamp": int(time.time())}))
        except Exception:
            pass

        # increment counter and reset debounce
        with counter_lock:
            counter += 1
            cur = counter
        if cur >= MIN_ROWS:
            # immediate spawn
            with counter_lock:
                counter = 0
            start_training_worker()
        else:
            reset_debounce_timer()

    except Exception:
        print("Error handling MQTT message:", traceback.format_exc())


def main():
    ensure_csv_header()
    client = mqtt.Client()
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
    except Exception:
        print("Failed to connect to MQTT broker", MQTT_BROKER, traceback.format_exc())
        return
    client.subscribe(DATA_TOPIC)
    client.loop_start()
    print("Ingest+Trainer running. Subscribed to", DATA_TOPIC)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        try:
            if debounce_timer and debounce_timer.is_alive():
                debounce_timer.cancel()
        except Exception:
            pass
        client.loop_stop()
        client.disconnect()


if __name__ == '__main__':
    main()
