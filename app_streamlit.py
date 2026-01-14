import streamlit as st
import pandas as pd
import numpy as np
import pickle
import time
import logging
import os
import json
import paho.mqtt.client as mqtt
import threading
from collections import deque
import base64
import hmac
import hashlib

# ===========================
# CONFIG
# ===========================
MODEL_FILE = "decision_tree.pkl"
CSV_FILE = "river_data_log.csv"
REFRESH_INTERVAL = 3  # seconds
RECENT_MAX = 500
MQTT_MODEL_TOPIC = "river/monitoring/model"
MQTT_MODEL_SECRET = os.getenv("MQTT_MODEL_SECRET", "")

# ===========================
# GLOBAL VARIABLES
# ===========================
recent_buf = deque(maxlen=RECENT_MAX)
buf_lock = threading.Lock()

# ===========================
# HELPERS
# ===========================
def normalize_emoji(label):
    l = label.upper()
    return {
        "AMAN": "🟢",
        "WASPADA": "🟡",
        "BAHAYA": "🔴"
    }.get(l, "❓")

def status_box(title, level, mode="danger"):
    if mode == "danger":
        if level == 0: color="#4fc3f7"; emoji="🟢"; text="SAFE"
        elif level == 1: color="#29b6f6"; emoji="🟡"; text="WARNING"
        else: color="#0277bd"; emoji="🔴"; text="DANGEROUS"
    elif mode == "rain":
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
        df_init = pd.DataFrame(columns=[
            "timestamp", "datetime", "water_level_cm", "temperature_c", 
            "humidity_pct", "danger_level", "rain_level"
        ])
        df_init.to_csv(CSV_FILE, index=False)

def append_to_csv(row):
    try:
        ensure_csv_header()
        df_new = pd.DataFrame([row])
        df_new.to_csv(CSV_FILE, mode='a', header=False, index=False)
        return True
    except:
        return False

# ===========================
# 🔥 FIXED: READ LATEST CSV ROW
# ===========================
@st.cache_data(ttl=1)  # 1s cache for real-time
def get_latest_csv_row():
    """🔥 ALWAYS GETS LATEST CSV ROW"""
    if not os.path.exists(CSV_FILE):
        return None
    
    try:
        df_csv = pd.read_csv(CSV_FILE)
        if df_csv.empty:
            return None
        return df_csv.iloc[-1].to_dict()  # 🔥 LATEST ROW
    except:
        return None

def get_all_data(standard_height):
    """🔥 COMBINE MQTT + LATEST CSV"""
    mqtt_data = []
    csv_latest = get_latest_csv_row()
    
    # MQTT buffer
    with buf_lock:
        mqtt_data = list(recent_buf)
    
    # Feature engineering for MQTT data
    if mqtt_data:
        df_mqtt = pd.DataFrame(mqtt_data)
        df_mqtt["water_level_norm"] = df_mqtt["water_level_cm"] / standard_height
        df_mqtt["water_rise_rate"] = df_mqtt["water_level_cm"].diff().fillna(0)
        df_mqtt["rain"] = (df_mqtt["rain_level"] > 0).astype(int)
    else:
        df_mqtt = pd.DataFrame()
    
    # Always prioritize latest CSV row for current status
    return df_mqtt, csv_latest

# ===========================
# LOGGING & MQTT (unchanged)
# ===========================
logging.basicConfig(filename="audit_log.txt", level=logging.INFO, format='%(asctime)s %(message)s')
def log_event(event): logging.info(event)

def mqtt_model_callback(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        model_b64 = payload.get("model_b64")
        if model_b64:
            model_bytes = base64.b64decode(model_b64.encode('ascii'))
            with open(MODEL_FILE, 'wb') as mf:
                mf.write(model_bytes)
            st.cache_resource.clear()
    except: pass

def mqtt_data_callback(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        row = {
            "timestamp": int(payload.get("timestamp", time.time()*1000)),
            "datetime": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            "water_level_cm": float(payload.get("water_level_cm", -1)),
            "temperature_c": float(payload.get("temperature_c", -1)),
            "humidity_pct": float(payload.get("humidity_pct", -1)),
            "danger_level": int(payload.get("danger_level", 0)),
            "rain_level": int(payload.get("rain_level", 0))
        }
        with buf_lock:
            recent_buf.append(row)
        append_to_csv(row)
    except: pass

# ===========================
# MAIN
# ===========================
def main():
    st.set_page_config(page_title="River Monitor", layout="wide")
    st.title("🌊 River Monitor — 🔄 3s Auto-Refresh")

    # ===========================
    # SIDEBAR
    # ===========================
    st.sidebar.title("⚙️ Config")
    mqtt_broker = st.sidebar.text_input("Broker", "broker.hivemq.com", key="broker")
    mqtt_port = st.sidebar.number_input("Port", 1883, 1, 65535, key="port")
    mqtt_topic = st.sidebar.text_input("Topic", "river/monitoring/data", key="topic")
    
    standard_height = st.sidebar.number_input("Std Height (cm)", 50.0, 0.0, 1000.0, key="std_h")
    
    # Auto-refresh
    st.sidebar.subheader("🔄 Refresh")
    enable_refresh = st.sidebar.toggle("Auto-Refresh 3s", True, key="auto_ref")
    
    refresh_btn = st.sidebar.button("🔄 Refresh Now", key="refresh")

    # CSV info
    if os.path.exists(CSV_FILE):
        df_csv = pd.read_csv(CSV_FILE)
        st.sidebar.metric("CSV Rows", len(df_csv))
        st.sidebar.success("✅ Latest CSV shown")

    # ===========================
    # MQTT
    # ===========================
    @st.cache_resource
    def init_mqtt(broker, port, topic):
        ensure_csv_header()
        client = mqtt.Client()
        client.on_message = lambda c,u,m: (
            mqtt_model_callback(c,u,m) if m.topic == MQTT_MODEL_TOPIC else mqtt_data_callback(c,u,m)
        )
        client.connect(broker, port)
        client.subscribe(topic)
        client.subscribe(MQTT_MODEL_TOPIC)
        client.loop_start()
        return client

    mqtt_client = init_mqtt(mqtt_broker, mqtt_port, mqtt_topic)

    # ===========================
    # 🔥 AUTO-REFRESH + DATA LOAD
    # ===========================
    df_mqtt, csv_latest = get_all_data(standard_height)
    
    # Auto-refresh logic
    if enable_refresh:
        if 'refresh_count' not in st.session_state:
            st.session_state.refresh_count = 0
        st.session_state.refresh_count += 1
        st.sidebar.metric("Refresh #", st.session_state.refresh_count)
        time.sleep(REFRESH_INTERVAL)
        st.rerun()

    if refresh_btn:
        st.cache_data.clear()
        st.rerun()

    # ===========================
    # 🔥 CURRENT STATUS - ALWAYS SHOWS LATEST CSV ROW
    # ===========================
    if csv_latest is not None:
        # 🔥 PRIORITY: LATEST CSV ROW
        water = csv_latest["water_level_cm"]
        rain_level = csv_latest["rain_level"]
        danger = csv_latest.get("danger_level", 0)
        timestamp = pd.to_datetime(csv_latest["datetime"])
        
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Water", f"{water:.1f} cm")
        with col2: status_box("Danger", int(danger), "danger")
        with col3: status_box("Rain", int(rain_level > 0), "rain")
        with col4: st.metric("Time", timestamp.strftime("%H:%M:%S"))
        
        st.success(f"✅ Showing **LATEST CSV ROW**: {water:.1f}cm at {timestamp}")
    else:
        st.warning("⏳ No CSV data yet - waiting for MQTT...")
        return

    # ===========================
    # CHARTS - MQTT DATA
    # ===========================
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📈 Water Level History")
        if not df_mqtt.empty:
            st.line_chart(df_mqtt["water_level_cm"], use_container_width=True)
        else:
            st.info("No MQTT history yet")
    
    with col2:
        st.subheader("🌡️ Environment")
        if not df_mqtt.empty and len(df_mqtt) > 1:
            env_df = pd.DataFrame({
                "Temp": df_mqtt["temperature_c"],
                "Humidity": df_mqtt["humidity_pct"]
            })
            st.line_chart(env_df, use_container_width=True)

    # ===========================
    # ML PREDICTION
    # ===========================
    @st.cache_resource
    def load_model():
        try:
            return pickle.load(open(MODEL_FILE, "rb"))
        except:
            return None

    model = load_model()
    if model and csv_latest:
        st.subheader("🤖 Prediction")
        features = ["water_level_cm", "rain_level", "temperature_c", "humidity_pct"]
        try:
            # Use CSV latest row for prediction
            pred_df = pd.DataFrame([csv_latest])[features]
            pred_df["water_level_norm"] = pred_df["water_level_cm"] / standard_height
            pred_df["rain"] = (pred_df["rain_level"] > 0).astype(int)
            
            prediction = model.predict(pred_df[["water_level_norm", "rain", "temperature_c", "humidity_pct"]])[0]
            emoji = normalize_emoji(prediction)
            
            st.markdown(f"""
            <div style="padding:30px; border-radius:20px; background:#0277bd; color:white; text-align:center;">
                <h1 style="font-size:70px;">{emoji}</h1>
                <h2>{prediction}</h2>
            </div>
            """, unsafe_allow_html=True)
        except:
            st.info("Prediction ready when model matches features")

if __name__ == "__main__":
    main()
