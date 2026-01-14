import streamlit as st
import pandas as pd
import numpy as np
import pickle
import time
import logging
import os
import json
import paho.mqtt.client as mqtt
from datetime import datetime
import base64
import hmac
import hashlib
from streamlit_autorefresh import st_autorefresh

# ===========================
# CONFIG
# ===========================
MODEL_FILE = "decision_tree.pkl"
CSV_FILE = "river_data_log.csv"
MQTT_MODEL_TOPIC = "river/monitoring/model"
MQTT_MODEL_SECRET = os.getenv("MQTT_MODEL_SECRET", "")
REFRESH_INTERVAL = 4  # seconds

# ===========================
# SESSION STATE
# ===========================
if "connected" not in st.session_state:
    st.session_state.connected = False
if "logs" not in st.session_state:
    st.session_state.logs = []
if "last_data" not in st.session_state:
    st.session_state.last_data = None
if "mqtt_client" not in st.session_state:
    st.session_state.mqtt_client = None

# ===========================
# HELPERS
# ===========================
def normalize_emoji(label):
    l = label.upper()
    return {"AMAN": "🟢", "WASPADA": "🟡", "BAHAYA": "🔴"}.get(l, "❓")

def status_box(title, level, mode="danger"):
    if mode == "danger":
        if level == 0: color="#4fc3f7"; emoji="🟢"; text="SAFE"
        elif level == 1: color="#29b6f6"; emoji="🟡"; text="WARNING"
        else: color="#0277bd"; emoji="🔴"; text="DANGEROUS"
    else:
        if level == 0: color="#4fc3f7"; emoji="🌤️"; text="NO RAIN"
        elif level == 1: color="#29b6f6"; emoji="🌦️"; text="LIGHT RAIN"
        else: color="#0277bd"; emoji="🌧️"; text="HEAVY RAIN"

    st.markdown(f"""
        <div style="padding:20px; border-radius:15px; background:{color}; text-align:center;">
            <h2 style="color:white;">{title}</h2>
            <h1 style="color:white; font-size:50px;">{emoji}</h1>
            <h3 style="color:white;">{text}</h3>
        </div>
    """, unsafe_allow_html=True)

def ensure_csv_header():
    if not os.path.exists(CSV_FILE):
        pd.DataFrame(columns=[
            "timestamp", "datetime", "water_level_cm",
            "temperature_c", "humidity_pct",
            "danger_level", "rain_level"
        ]).to_csv(CSV_FILE, index=False)

def append_to_csv(row):
    ensure_csv_header()
    pd.DataFrame([row]).to_csv(CSV_FILE, mode="a", header=False, index=False)

@st.cache_data(ttl=1)
def get_latest_csv():
    if not os.path.exists(CSV_FILE):
        return None
    df = pd.read_csv(CSV_FILE)
    return None if df.empty else df.iloc[-1].to_dict()

# ===========================
# MQTT CALLBACKS
# ===========================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        st.session_state.connected = True
        client.subscribe("river/monitoring/data")
        client.subscribe(MQTT_MODEL_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8").strip()

        # 🚨 Ignore empty payloads
        if not payload_str:
            return

        # 🚨 Must start with JSON object
        if not payload_str.startswith("{"):
            return

        data = json.loads(payload_str)

        if msg.topic == MQTT_MODEL_TOPIC:
            model_b64 = data.get("model_b64")
            if model_b64:
                with open(MODEL_FILE, "wb") as f:
                    f.write(base64.b64decode(model_b64))
                st.cache_resource.clear()
            return

        row = {
            "timestamp": int(time.time() * 1000),
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "water_level_cm": float(data.get("water_level_cm", -1)),
            "temperature_c": float(data.get("temperature_c", -1)),
            "humidity_pct": float(data.get("humidity_pct", -1)),
            "danger_level": int(data.get("danger_level", 0)),
            "rain_level": int(data.get("rain_level", 0)),
        }

        st.session_state.last_data = row
        st.session_state.logs.append(row)
        st.session_state.logs = st.session_state.logs[-500:]
        append_to_csv(row)

    except json.JSONDecodeError:
        # ✅ Ignore bad JSON silently
        return
    except Exception as e:
        st.error(f"MQTT error: {e}")

# ===========================
# 🤖 LOAD ML MODEL
# ===========================
@st.cache_resource
def load_model():
    try:
        with open(MODEL_FILE, "rb") as f:
            return pickle.load(f)
    except:
        return None


# ===========================
# MAIN
# ===========================
def main():
    st.set_page_config(page_title="River Monitor", layout="wide")

    # ✅ AUTOMATIC REFRESH EVERY 4 SECONDS
    st_autorefresh(interval=REFRESH_INTERVAL * 1000, key="river_refresh")

    st.title("🌊 River Monitor Dashboard")

    # Sidebar
    st.sidebar.title("⚙️ Configuration")
    mqtt_broker = st.sidebar.text_input("Broker", "broker.hivemq.com")
    mqtt_port = st.sidebar.number_input("Port", 1, 65535, 1883)
    standard_height = st.sidebar.number_input("Std Height (cm)", 0.0, 1000.0, 50.0)

    st.sidebar.metric("MQTT", "🟢 ON" if st.session_state.connected else "🔴 OFF")

    # MQTT setup
    if st.session_state.mqtt_client is None:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(mqtt_broker, mqtt_port, 60)
        st.session_state.mqtt_client = client

    if st.session_state.mqtt_client:
        st.session_state.mqtt_client.loop(timeout=0.1)

    data = get_latest_csv() or st.session_state.last_data
    if not data:
        st.info("Waiting for data...")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Water Level", f"{data['water_level_cm']:.1f} cm")
    with col2: status_box("Danger", data["danger_level"])
    with col3: status_box("Rain", int(data["rain_level"] > 0), "rain")

    if len(st.session_state.logs) > 1:
        df = pd.DataFrame(st.session_state.logs)
        st.line_chart(df.set_index("datetime")["water_level_cm"])
        # ===========================
    # 🤖 AI PREDICTION
    # ===========================
    model = load_model()

    if model and data:
        st.subheader("🤖 AI Flood Prediction")

        try:
            latest_df = pd.DataFrame([data])

            # Feature engineering (HARUS sama dengan training)
            latest_df["water_level_norm"] = (
                latest_df["water_level_cm"] / standard_height
                if standard_height > 0 else 0
            )
            latest_df["rain"] = (latest_df["rain_level"] > 0).astype(int)

            X = latest_df[[
                "water_level_norm",
                "rain",
                "temperature_c",
                "humidity_pct"
            ]]

            pred = model.predict(X)[0]
            emoji = normalize_emoji(pred)

            st.markdown(f"""
            <div style="padding:30px; border-radius:20px;
                        background:#01579b; color:white;
                        text-align:center;">
                <h1 style="font-size:70px;">{emoji}</h1>
                <h2>{pred}</h2>
            </div>
            """, unsafe_allow_html=True)

        except Exception as e:
            st.warning(f"Prediction error: {e}")

if __name__ == "__main__":
    main()


