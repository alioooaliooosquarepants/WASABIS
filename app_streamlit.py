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

# ===========================
# CONFIG
# ===========================
MODEL_FILE = "decision_tree.pkl"
CSV_FILE = "river_data_log.csv"
MQTT_MODEL_TOPIC = "river/monitoring/model"
MQTT_MODEL_SECRET = os.getenv("MQTT_MODEL_SECRET", "")

# ===========================
# SESSION STATE (STABLE REALTIME)
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
    else:  # rain
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
# MQTT CALLBACKS
# ===========================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        st.session_state.connected = True
        client.subscribe("river/monitoring/data")
        client.subscribe(MQTT_MODEL_TOPIC)
        st.sidebar.success("✅ MQTT Connected")
    else:
        st.session_state.connected = False
        st.sidebar.error("❌ MQTT Failed")

def on_message(client, userdata, msg):
    try:
        if msg.topic == MQTT_MODEL_TOPIC:
            payload = json.loads(msg.payload.decode())
            model_b64 = payload.get("model_b64")
            if model_b64:
                model_bytes = base64.b64decode(model_b64.encode('ascii'))
                with open(MODEL_FILE, 'wb') as f:
                    f.write(model_bytes)
                st.cache_resource.clear()
        else:
            # REALTIME DATA
            data = json.loads(msg.payload.decode())
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            row = {
                "timestamp": int(time.time()*1000),
                "datetime": ts,
                "water_level_cm": float(data.get("water_level_cm", -1)),
                "temperature_c": float(data.get("temperature_c", -1)),
                "humidity_pct": float(data.get("humidity_pct", -1)),
                "danger_level": int(data.get("danger_level", 0)),
                "rain_level": int(data.get("rain_level", 0))
            }
            
            st.session_state.last_data = row
            st.session_state.logs.append(row)
            
            # Keep only 500 latest
            if len(st.session_state.logs) > 500:
                st.session_state.logs = st.session_state.logs[-500:]
            
            append_to_csv(row)
            
    except Exception as e:
        st.error(f"Parse error: {e}")

# ===========================
# MAIN
# ===========================
def main():
    st.set_page_config(page_title="River Monitor", layout="wide")
    st.title("🌊 River Monitor Dashboard — Auto Refresh")

    # ===========================
    # SIDEBAR
    # ===========================
    st.sidebar.title("⚙️ Configuration")
    mqtt_broker = st.sidebar.text_input("Broker", "broker.hivemq.com", key="broker")
    mqtt_port = st.sidebar.number_input("Port", min_value=1, max_value=65535, value=1883, key="port")
    mqtt_topic = st.sidebar.text_input("Topic", "river/monitoring/data", key="topic")
    
    standard_height = st.sidebar.number_input("Std Height (cm)", 0.0, 1000.0, 50.0, key="std_h")
    
    # Status
    st.sidebar.metric("MQTT", "🟢 ON" if st.session_state.connected else "🔴 OFF")
    st.sidebar.metric("Data Points", len(st.session_state.logs))
    
    if os.path.exists(CSV_FILE):
        csv_rows = len(pd.read_csv(CSV_FILE))
        st.sidebar.metric("CSV Rows", csv_rows)

    # 🔥 AUTO REFRESH BUTTON (same exact function as manual)
    if st.sidebar.button("🔄 Refresh Data", key="refresh"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    # ===========================
    # MQTT CLIENT SETUP
    # ===========================
    if st.session_state.mqtt_client is None:
        try:
            client = mqtt.Client()
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(mqtt_broker, mqtt_port, 60)
            st.session_state.mqtt_client = client
        except Exception as e:
            st.error(f"MQTT Error: {e}")

    # ===========================
    # MQTT POLLING (KEEPS RUNNING)
    # ===========================
    if st.session_state.mqtt_client:
        st.session_state.mqtt_client.loop(timeout=0.1)  # Non-blocking poll

    # ===========================
    # MAIN DISPLAY
    # ===========================
    if not st.session_state.logs:
        st.info("⏳ **Waiting for data...**")
        st.info("**Test:** `mosquitto_pub -h broker.hivemq.com -t \"river/monitoring/data\" -m '{\"water_level_cm\":25.5}'`")
        return

    # Current status
    latest = st.session_state.last_data
    if latest:
        water = latest["water_level_cm"]
        rain = latest["rain_level"]
        danger = latest.get("danger_level", 0)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Water Level", f"{water:.1f} cm")
        with col2:
            status_box("Danger", int(danger), "danger")
        with col3:
            status_box("Rain", int(rain > 0), "rain")

    # Charts
    df = pd.DataFrame(st.session_state.logs)
    if len(df) >= 2:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📈 Water Level")
            st.line_chart(df.set_index("datetime")["water_level_cm"], use_container_width=True)
        with col2:
            st.subheader("🌡️ Environment") 
            env_df = df.set_index("datetime")[["temperature_c", "humidity_pct"]]
            st.line_chart(env_df, use_container_width=True)

    # ML Prediction
    @st.cache_resource
    def load_model():
        try:
            return pickle.load(open(MODEL_FILE, "rb"))
        except:
            return None

    model = load_model()
    if model and st.session_state.last_data:
        st.subheader("🤖 Prediction")
        try:
            latest_df = pd.DataFrame([st.session_state.last_data])
            latest_df["water_level_norm"] = latest_df["water_level_cm"] / standard_height
            latest_df["rain"] = (latest_df["rain_level"] > 0).astype(int)
            
            pred = model.predict(latest_df[["water_level_norm", "rain", "temperature_c", "humidity_pct"]])[0]
            emoji = normalize_emoji(pred)
            st.markdown(f"""
            <div style="padding:30px; border-radius:20px; background:#0277bd; color:white; text-align:center;">
                <h1 style="font-size:70px;">{emoji}</h1>
                <h2>{pred}</h2>
            </div>
            """, unsafe_allow_html=True)
        except:
            st.info("Model loaded")

    # Download
    if st.session_state.logs:
        st.download_button("💾 Download CSV", 
                          pd.DataFrame(st.session_state.logs).to_csv().encode(), 
                          "river_data.csv")

if __name__ == "__main__":
    main()
