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
REFRESH_INTERVAL = 3
RECENT_MAX = 500
MQTT_MODEL_TOPIC = "river/monitoring/model"
MQTT_MODEL_SECRET = os.getenv("MQTT_MODEL_SECRET", "")
CSV_READ_POSITION_FILE = "csv_read_position.txt"

# ===========================
# GLOBAL VARIABLES
# ===========================
recent_buf = deque(maxlen=RECENT_MAX)
buf_lock = threading.Lock()
last_csv_position = 0

# ===========================
# HELPERS
# ===========================
@st.cache_data
def normalize_emoji(label):
    l = label.upper()
    return {"AMAN": "🟢", "WASPADA": "🟡", "BAHAYA": "🔴"}.get(l, "❓")

def status_box(title, level, mode="danger"):
    if mode == "danger":
        if level == 0: color, emoji, text = "#4fc3f7", "🟢", "SAFE"
        elif level == 1: color, emoji, text = "#29b6f6", "🟡", "WARNING"
        else: color, emoji, text = "#0277bd", "🔴", "DANGEROUS"
    else:  # rain
        if level == 0: color, emoji, text = "#4fc3f7", "🌤️", "NO RAIN"
        elif level == 1: color, emoji, text = "#29b6f6", "🌦️", "LIGHT RAIN"
        else: color, emoji, text = "#0277bd", "🌧️", "HEAVY RAIN"

    st.markdown(f"""
    <div style="padding:20px; border-radius:15px; background:{color}; text-align:center;">
        <h2 style="color:white;">{title}</h2>
        <h1 style="color:white; font-size:50px;">{emoji}</h1>
        <h3 style="color:white;">{text}</h3>
    </div>
    """, unsafe_allow_html=True)

def save_csv_position(position):
    global last_csv_position
    last_csv_position = position
    try:
        with open(CSV_READ_POSITION_FILE, 'w') as f:
            f.write(str(position))
    except: pass

def load_csv_position():
    global last_csv_position
    try:
        if os.path.exists(CSV_READ_POSITION_FILE):
            with open(CSV_READ_POSITION_FILE, 'r') as f:
                return int(f.read().strip())
    except: pass
    return 0

# ===========================
# LOGGING
# ===========================
logging.basicConfig(filename="audit_log.txt", level=logging.INFO, format='%(asctime)s %(message)s')
def log_event(event): logging.info(event)

# ===========================
# MQTT CALLBACKS
# ===========================
def mqtt_model_callback(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        model_b64 = payload.get("model_b64")
        if model_b64:
            model_bytes = base64.b64decode(model_b64.encode('ascii'))
            with open(MODEL_FILE, 'wb') as f:
                f.write(model_bytes)
            st.cache_resource.clear()
            logging.info("Model updated via MQTT")
    except: pass

def mqtt_data_callback(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        row = {
            "timestamp": int(payload.get("timestamp", time.time()*1000)),
            "datetime": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            "water_level_cm": float(payload.get("water_level_cm", 0)),
            "temperature_c": float(payload.get("temperature_c", 0)),
            "humidity_pct": float(payload.get("humidity_pct", 0)),
            "danger_level": int(payload.get("danger_level", 0)),
            "rain_level": int(payload.get("rain_level", 0))
        }
        with buf_lock:
            recent_buf.append(row)
        save_csv_position(-1)  # Reset CSV when live data arrives
    except: pass

# ===========================
# MAIN APP
# ===========================
def main():
    st.set_page_config(page_title="River Monitor", layout="wide")
    st.title("🌊 River Monitoring Dashboard — Live + Historical")
    
    st.markdown("<style>body {background-color: #e0f7fa;}</style>", unsafe_allow_html=True)

    # ===========================
    # SIDEBAR - FULLY FIXED
    # ===========================
    st.sidebar.title("⚙️ Configuration")
    
    mqtt_broker = st.sidebar.text_input("MQTT Broker", value="broker.hivemq.com", key="broker")
    mqtt_port = st.sidebar.number_input("Port", min_value=1, max_value=65535, value=1883, step=1, key="port")
    mqtt_topic = st.sidebar.text_input("Data Topic", value="river/monitoring/data", key="topic")
    
    standard_height = st.sidebar.number_input("Standard Height (cm)", min_value=0.0, max_value=1000.0, value=50.0, step=1.0, key="std_height")
    
    st.sidebar.subheader("📊 CSV Playback")
    csv_speed = st.sidebar.slider("Speed", 1, 10, 3, key="csv_speed")
    reset_csv = st.sidebar.button("🔄 Reset CSV", key="reset_csv")
    
    st.sidebar.subheader("🔧 Manual Override")
    manual_water = st.sidebar.number_input("Water (cm)", min_value=0.0, max_value=1000.0, value=None, key="m_water")
    manual_danger = st.sidebar.selectbox("Danger", [None, 0, 1, 2], key="m_danger")
    apply_manual = st.sidebar.button("Apply", key="manual_btn")
    
    refresh_btn = st.sidebar.button("🔄 Refresh", key="refresh")

    # ===========================
    # MQTT SETUP
    # ===========================
    @st.cache_resource
    def init_mqtt(_broker, _port, _topic):
        if _broker and _port:
            client = mqtt.Client()
            client.on_message = lambda c,u,m: (
                mqtt_model_callback(c,u,m) if m.topic == MQTT_MODEL_TOPIC else mqtt_data_callback(c,u,m)
            )
            try:
                client.connect(_broker, _port)
                client.subscribe(_topic)
                client.subscribe(MQTT_MODEL_TOPIC)
                client.loop_start()
                return client
            except Exception as e:
                st.sidebar.error(f"MQTT Error: {e}")
        return None

    mqtt_client = init_mqtt(mqtt_broker, mqtt_port, mqtt_topic)

    # ===========================
    # DATA LOADING - GRADUAL CSV + LIVE
    # ===========================
    @st.cache_data(ttl=REFRESH_INTERVAL)
    def load_data(_std_height, _csv_pos=-1):
        dataframes = []
        sources = []
        
        # 1. PRIORITY: MQTT Live Data
        with buf_lock:
            mqtt_df = pd.DataFrame(list(recent_buf))
            if not mqtt_df.empty:
                dataframes.append(mqtt_df)
                sources.append("MQTT Live")
        
        # 2. GRADUAL CSV READING
        if os.path.exists(CSV_FILE):
            try:
                csv_pos = load_csv_position() if _csv_pos == -1 else _csv_pos
                if reset_csv: csv_pos = 0
                
                csv_full = pd.read_csv(CSV_FILE)
                if len(csv_full) > csv_pos:
                    # Read gradually - 20 rows at a time based on speed
                    rows_to_read = min(20 * csv_speed, len(csv_full) - csv_pos)
                    new_csv_data = csv_full.iloc[csv_pos:csv_pos + rows_to_read].copy()
                    dataframes.append(new_csv_data)
                    sources.append(f"CSV {csv_pos}-{csv_pos+rows_to_read}")
                    
                    # Save new position
                    save_csv_position(csv_pos + rows_to_read)
                else:
                    dataframes.append(csv_full.tail(1))
                    sources.append("CSV Complete")
            except: pass
        
        if dataframes:
            df = pd.concat(dataframes, ignore_index=True)
            df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
            
            # Feature Engineering
            df["water_level_norm"] = df["water_level_cm"] / _std_height
            df["water_rise_rate"] = df["water_level_cm"].diff().fillna(0)
            df["rain"] = (df["rain_level"] > 0).astype(int)
            return df, sources
        
        return pd.DataFrame(), []

    # Load data
    if refresh_btn:
        st.cache_data.clear()
        st.cache_resource.clear()
    
    df, data_sources = load_data(standard_height)
    
    if df.empty:
        st.info("⏳ Waiting for MQTT data or CSV file...")
        st.stop()

    # ===========================
    # STATUS DISPLAY
    # ===========================
    last_row = df.iloc[-1]
    water = last_row["water_level_cm"]
    rain_val = last_row["rain"]
    danger = last_row.get("danger_level", 0)

    # Status Cards
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Water Level", f"{water:.1f} cm")
    with col2:
        status_box("Danger Level", int(danger), "danger")
    with col3:
        status_box("Rain Status", int(rain_val), "rain")

    # Data Source Info
    st.sidebar.markdown("### 📡 Status")
    st.sidebar.metric("Data Points", len(df))
    st.sidebar.metric("Live MQTT", "✅" if len(recent_buf) > 0 else "❌")
    csv_pos = load_csv_position()
    total_csv_rows = len(pd.read_csv(CSV_FILE)) if os.path.exists(CSV_FILE) else 0
    st.sidebar.metric("CSV Progress", f"{csv_pos}/{total_csv_rows}")

    # ===========================
    # GRAPHS - PAUSE AT LAST POINT
    # ===========================
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📈 Water Level History")
        st.line_chart(df["water_level_cm"], use_container_width=True)
        st.caption(f"Latest: {water:.1f}cm • Paused at {len(df)} points")
    
    with col2:
        st.subheader("🌡️ Environment")
        env_data = pd.DataFrame({
            "Temperature": df["temperature_c"],
            "Humidity": df["humidity_pct"]
        })
        st.line_chart(env_data, use_container_width=True)

    # ===========================
    # PLAYBACK CONTROLS
    # ===========================
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("⏸️ Pause CSV"): save_csv_position(999999)
    with col2:
        data_age = time.time() - (df['timestamp'].iloc[-1] / 1000)
        st.metric("Data Freshness", f"{data_age:.0f}s ago")
    with col3:
        if st.button("📊 End CSV"): save_csv_position(-1)

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
    if model is not None and len(df) > 0:
        st.subheader("🤖 ML Prediction")
        features = ["water_level_norm", "water_rise_rate", "rain", "humidity_pct"]
        latest = df[features].iloc[-1:].copy()
        
        # Manual override
        if apply_manual and manual_water is not None:
            latest["water_level_norm"] = manual_water / standard_height
        
        try:
            prediction = model.predict(latest)[0]
            emoji = normalize_emoji(prediction)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""
                <div style="padding:30px; border-radius:20px; background:#0277bd; color:white; text-align:center;">
                    <h1 style="font-size:70px;">{emoji}</h1>
                    <h2>{prediction}</h2>
                    <p>Water: {water:.1f}cm</p>
                </div>
                """, unsafe_allow_html=True)
            
            # Risk prediction
            rise_rate = df["water_rise_rate"].iloc[-1]
            if rise_rate > 0:
                time_to_danger = max(0, (35 - water) / rise_rate * 60)
                st.info(f"⏰ **{time_to_danger:.0f} minutes** to danger threshold (35cm)")
                
        except Exception as e:
            st.warning(f"Prediction error: {e}")
    else:
        st.info("ℹ️ Place `decision_tree.pkl` file for ML predictions")

if __name__ == "__main__":
    main()
