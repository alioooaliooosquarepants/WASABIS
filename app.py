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
CSV_READ_POSITION_FILE = "csv_read_position.txt"  # Track CSV reading position

# ===========================
# GLOBAL VARIABLES
# ===========================
recent_buf = deque(maxlen=RECENT_MAX)
buf_lock = threading.Lock()
last_csv_position = 0  # Track last CSV row read

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

# ===========================
# CSV POSITION MANAGEMENT
# ===========================
def save_csv_position(position):
    """Save current CSV reading position"""
    global last_csv_position
    last_csv_position = position
    try:
        with open(CSV_READ_POSITION_FILE, 'w') as f:
            f.write(str(position))
    except:
        pass

def load_csv_position():
    """Load last CSV reading position"""
    global last_csv_position
    try:
        if os.path.exists(CSV_READ_POSITION_FILE):
            with open(CSV_READ_POSITION_FILE, 'r') as f:
                last_csv_position = int(f.read().strip())
    except:
        last_csv_position = 0
    return last_csv_position

# ===========================
# LOGGING
# ===========================
logging.basicConfig(filename="audit_log.txt", level=logging.INFO, format='%(asctime)s %(message)s')

def log_event(event):
    logging.info(event)

# ===========================
# MQTT CALLBACKS (unchanged)
# ===========================
def mqtt_model_callback(client, userdata, message):
    try:
        payload = message.payload.decode()
        meta = json.loads(payload)
        model_b64 = meta.get("model_b64")
        if model_b64:
            if MQTT_MODEL_SECRET:
                sig = meta.get("signature")
                calc = hmac.new(MQTT_MODEL_SECRET.encode(), model_b64.encode(), hashlib.sha256).hexdigest()
                if sig and not hmac.compare_digest(calc, sig):
                    return
            model_bytes = base64.b64decode(model_b64.encode('ascii'))
            with open(MODEL_FILE, 'wb') as f:
                f.write(model_bytes)
            st.cache_resource.clear()
            logging.info("Model updated via MQTT")
    except:
        pass

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
        save_csv_position(-1)  # Reset CSV reading when new MQTT data arrives
    except:
        pass

# ===========================
# MAIN FUNCTION
# ===========================
def main():
    st.set_page_config(page_title="River Monitor", layout="wide")
    st.title("🌊 River Monitoring Dashboard — Real-Time + Historical")

    # Styling
    st.markdown("<style>body {background-color: #e0f7fa;}</style>", unsafe_allow_html=True)

    # ===========================
    # SIDEBAR
    # ===========================
    st.sidebar.title("⚙️ Configuration")
    
    mqtt_broker = st.sidebar.text_input("MQTT Broker", value="broker.hivemq.com", key="broker")
    mqtt_port = st.sidebar.number_input("Port", 1883, 1, 65535, key="port")
    mqtt_topic = st.sidebar.text_input("Data Topic", "river/monitoring/data", key="topic")
    
    standard_height = st.sidebar.number_input("Standard Height (cm)", 0.0, 1000.0, 50.0, key="std_height")
    
    # CSV Control
    st.sidebar.subheader("📊 CSV Playback")
    csv_speed = st.sidebar.slider("Playback Speed", 1, 10, 3, key="csv_speed")
    reset_csv = st.sidebar.button("🔄 Reset CSV Reading", key="reset_csv")
    
    # Manual override
    st.sidebar.subheader("Manual Override")
    manual_water = st.sidebar.number_input("Water (cm)", 0.0, 1000.0, None, key="m_water")
    manual_danger = st.sidebar.selectbox("Danger", [None, 0, 1, 2], key="m_danger")
    apply_manual = st.sidebar.button("Apply", key="manual_btn")

    # ===========================
    # MQTT SETUP
    # ===========================
    @st.cache_resource
    def init_mqtt(broker, port, topic):
        client = mqtt.Client()
        client.on_message = lambda c,u,m: mqtt_model_callback(c,u,m) if m.topic == MQTT_MODEL_TOPIC else mqtt_data_callback(c,u,m)
        try:
            client.connect(broker, port)
            client.subscribe(topic)
            client.subscribe(MQTT_MODEL_TOPIC)
            client.loop_start()
            return client
        except:
            return None

    mqtt_client = init_mqtt(mqtt_broker, mqtt_port, mqtt_topic)

    # ===========================
    # ENHANCED DATA LOADING
    # ===========================
    @st.cache_data(ttl=REFRESH_INTERVAL)
    def load_data_enhanced(std_height, csv_pos=-1):
        data_sources = []
        
        # 1. MQTT Buffer (priority 1 - newest)
        with buf_lock:
            mqtt_df = pd.DataFrame(list(recent_buf))
            if not mqtt_df.empty:
                data_sources.append(("MQTT (Live)", mqtt_df))
        
        # 2. CSV - gradual reading from saved position
        if os.path.exists(CSV_FILE):
            try:
                csv_pos = load_csv_position() if csv_pos == -1 else csv_pos
                if reset_csv:
                    csv_pos = 0
                
                csv_df = pd.read_csv(CSV_FILE)
                if len(csv_df) > csv_pos:
                    # Read new rows since last position
                    new_rows = csv_df.iloc[csv_pos:csv_pos + 50]  # Read 50 rows at a time
                    data_sources.append((f"CSV (rows {csv_pos}-{csv_pos+len(new_rows)})", new_rows))
                    
                    # Update position for next read
                    new_pos = csv_pos + len(new_rows)
                    save_csv_position(new_pos)
                else:
                    data_sources.append(("CSV (Complete)", csv_df.tail(1)))
            except:
                pass
        
        # Combine all data sources
        if data_sources:
            all_data = pd.concat([df for _, df in data_sources], ignore_index=True)
            all_data = all_data.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
            
            # Feature engineering
            all_data["water_level_norm"] = all_data["water_level_cm"] / std_height
            all_data["water_rise_rate"] = all_data["water_level_cm"].diff().fillna(0)
            all_data["rain"] = (all_data["rain_level"] > 0).astype(int)
            
            return all_data, data_sources
        return pd.DataFrame(), []

    # Load data
    df, sources = load_data_enhanced(standard_height)
    
    if df.empty:
        st.info("⏳ No data available. Waiting for MQTT or check CSV file.")
        return

    # ===========================
    # DATA SOURCE STATUS
    # ===========================
    st.sidebar.markdown("### 📡 Data Sources")
    mqtt_active = len(recent_buf) > 0
    st.sidebar.metric("MQTT Active", "✅ LIVE" if mqtt_active else "❌ Idle")
    st.sidebar.metric("Total Points", len(df))
    st.sidebar.metric("CSV Position", f"{load_csv_position()}/{len(pd.read_csv(CSV_FILE)) if os.path.exists(CSV_FILE) else 0}")

    # ===========================
    # CURRENT STATUS
    # ===========================
    last_row = df.iloc[-1]
    water = last_row["water_level_cm"]
    rain_level = last_row["rain"]
    danger = last_row.get("danger_level", 0)

    col1, col2, col3 = st.columns(3)
    with col1: st.metric("Water Level", f"{water:.1f} cm")
    with col2: status_box("Danger", int(danger), "danger")
    with col3: status_box("Rain", int(rain_level), "rain")

    # ===========================
    # GRAPHS - PAUSE AT LAST DATA
    # ===========================
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Water Level (Paused at Latest)")
        chart_data = df["water_level_cm"].values
        st.line_chart(chart_data, use_container_width=True)
        st.caption(f"Latest: {water:.1f}cm • {len(df)} points")
    
    with col2:
        st.subheader("🌡️ Environment")
        env_df = pd.DataFrame({
            'Temperature': df["temperature_c"],
            'Humidity': df["humidity_pct"]
        })
        st.line_chart(env_df, use_container_width=True)

    # ===========================
    # PLAYBACK CONTROLS
    # ===========================
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("⏸️ Pause CSV", key="pause"):
            save_csv_position(len(pd.read_csv(CSV_FILE)))
    with col2:
        st.metric("Data Age", f"{time.time() - df['timestamp'].iloc[-1]/1000:.0f}s")
    with col3:
        if st.button("📊 Jump to End", key="jump_end"):
            save_csv_position(-1)

    # ===========================
    # ML PREDICTION
    # ===========================
    @st.cache_resource
    def get_model():
        try:
            return pickle.load(open(MODEL_FILE, "rb"))
        except:
            return None

    model = get_model()
    if model and len(df) > 0:
        st.subheader("🤖 Prediction")
        features = ["water_level_norm", "water_rise_rate", "rain", "humidity_pct"]
        latest = df[features].iloc[-1:].copy()
        
        if apply_manual and manual_water is not None:
            latest["water_level_norm"] = manual_water / standard_height
        
        try:
            pred = model.predict(latest)[0]
            emoji = normalize_emoji(pred)
            st.markdown(f"""
            <div style="padding:30px; border-radius:20px; background:#0277bd; color:white; text-align:center;">
                <h1 style="font-size:70px;">{emoji}</h1>
                <h2>{pred}</h2>
                <p>Based on latest data: {water:.1f}cm</p>
            </div>
            """, unsafe_allow_html=True)
        except:
            st.warning("Prediction failed - check model compatibility")

if __name__ == "__main__":
    main()
