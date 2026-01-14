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
MODEL_FILE = "decision_tree.pkl"  # Dedicated to Decision Tree model
CSV_FILE = "river_data_log.csv"
REFRESH_INTERVAL = 3  # detik
LABELS = ["Aman", "Waspada", "Bahaya"]
STANDARD_WATER_HEIGHT = st.sidebar.number_input("Standard Water Height (cm)", min_value=0.0, max_value=1000.0, value=50.0, step=1.0)
RECENT_MAX = 500

# ===========================
# GLOBAL VARIABLES
# ===========================
recent_buf = deque(maxlen=RECENT_MAX)
buf_lock = threading.Lock()
MQTT_MODEL_TOPIC = "river/monitoring/model"
MQTT_MODEL_SECRET = os.getenv("MQTT_MODEL_SECRET", "")  # Optional model verification secret

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

# ===========================
# LOGGING
# ===========================
logging.basicConfig(filename="audit_log.txt", level=logging.INFO, format='%(asctime)s %(message)s')

def log_event(event):
    logging.info(event)

# ===========================
# MQTT CALLBACKS
# ===========================
def mqtt_model_callback(client, userdata, message):
    """Handle model updates from MQTT"""
    try:
        topic = message.topic
        payload = message.payload.decode()
        if topic == MQTT_MODEL_TOPIC:
            meta = json.loads(payload)
            model_b64 = meta.get("model_b64")
            if model_b64:
                # optional verification
                sig = meta.get("signature")
                if sig and MQTT_MODEL_SECRET:
                    calc = hmac.new(MQTT_MODEL_SECRET.encode('utf-8'), model_b64.encode('utf-8'), hashlib.sha256).hexdigest()
                    if not hmac.compare_digest(calc, sig):
                        logging.error("Model signature mismatch; ignoring model message")
                        return
                model_bytes = base64.b64decode(model_b64.encode('ascii'))
                with open(MODEL_FILE, 'wb') as mf:
                    mf.write(model_bytes)
                # clear cached model so Streamlit reloads
                try:
                    st.cache_resource.clear()
                except Exception:
                    pass
                logging.info("Received and saved model from MQTT")
    except Exception as e:
        logging.exception("Failed to handle model message: %s", e)

def mqtt_data_callback(client, userdata, message):
    """Handle data updates from MQTT"""
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
        log_event(f"MQTT data received: water_level={row['water_level_cm']}")
    except Exception as e:
        logging.exception("MQTT data handling error: %s", e)

# ===========================
# MAIN FUNCTION
# ===========================
def main():
    # ===========================
    # SIDEBAR CONFIG (must be before st.sidebar calls)
    # ===========================
    st.sidebar.title("MQTT Configuration")
    MQTT_BROKER = st.sidebar.text_input("MQTT Broker", value="broker.hivemq.com")
    MQTT_PORT = st.sidebar.number_input("MQTT Port", value=1883, min_value=1, max_value=65535)
    MQTT_TOPIC = st.sidebar.text_input("Data Topic", value="river/monitoring/data")
    
    st.sidebar.title("Dashboard Settings")
    STANDARD_WATER_HEIGHT = st.sidebar.number_input("Standard Water Height (cm)", min_value=0.0, max_value=1000.0, value=50.0, step=1.0)
    
    # ===========================
    # DATA LOADING & CLEANING
    # ===========================
    @st.cache_data(ttl=REFRESH_INTERVAL)
    def load_data():
        with buf_lock:
            df = pd.DataFrame(list(recent_buf))
        # if empty, optionally fall back to reading CSV once
        if df.empty and os.path.exists(CSV_FILE):
            df = pd.read_csv(CSV_FILE).sort_values("timestamp").tail(RECENT_MAX)
        # Range check & noise filter
        if not df.empty:
            df = df[(df["water_level_cm"].between(0, 1000)) &
                    (df["temperature_c"].between(-10, 80)) &
                    (df["humidity_pct"].between(0, 100))]
            df = df.fillna(method="ffill").fillna(method="bfill")  # handle missing data
            # Normalization
            df["water_level_norm"] = df["water_level_cm"] / STANDARD_WATER_HEIGHT
            # Water rise rate
            df["water_rise_rate"] = df["water_level_cm"].diff().fillna(0)
            # Rain binary
            df["rain"] = (df["rain_level"] > 0).astype(int)
        return df

    @st.cache_resource
    def load_model():
        try:
            model = pickle.load(open(MODEL_FILE, "rb"))
            return model
        except Exception:
            return None

    # ===========================
    # MQTT CLIENT SETUP
    # ===========================
    @st.cache_resource
    def start_mqtt_client(broker, port, data_topic, model_topic):
        def _ensure_csv_header():
            if not os.path.exists(CSV_FILE):
                df_init = pd.DataFrame(columns=[
                    "timestamp", "datetime", "water_level_cm",
                    "temperature_c", "humidity_pct", "danger_level", "rain_level"
                ])
                df_init.to_csv(CSV_FILE, index=False)

        _ensure_csv_header()
        client = mqtt.Client()
        client.on_message = lambda c,u,m: (
            mqtt_model_callback(c,u,m) if m.topic == model_topic else mqtt_data_callback(c,u,m)
        )
        try:
            client.connect(broker, int(port))
            client.subscribe(data_topic)
            client.subscribe(model_topic)
            client.loop_start()
            logging.info(f"MQTT client started: {broker}:{port}")
        except Exception as e:
            logging.exception("Failed to start MQTT client: %s", e)
        return client

    # Start MQTT client
    mqtt_client = start_mqtt_client(MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, MQTT_MODEL_TOPIC)

    # ===========================
    # STREAMLIT SETTINGS
    # ===========================
    st.set_page_config(page_title="River Monitor + MQTT + ML", layout="wide")
    st.title("🌊 River Monitoring Dashboard — Real-Time + Prediction")

    # Blue-ish background
    st.markdown("""
    <style>
    body {
        background-color: #e0f7fa;
    }
    </style>
    """, unsafe_allow_html=True)

    # ========== SIDEBAR ==========
    st.sidebar.title("Pengaturan")
    refresh = st.sidebar.button("Refresh Sekarang")

    # Manual Override Section
    st.sidebar.subheader("Manual Override")
    manual_water_level = st.sidebar.number_input("Manual Water Level (cm)", min_value=0.0, max_value=1000.0, value=None, step=1.0)
    manual_temp = st.sidebar.number_input("Manual Temperature (°C)", min_value=-10.0, max_value=80.0, value=None, step=0.1)
    manual_humidity = st.sidebar.number_input("Manual Humidity (%)", min_value=0.0, max_value=100.0, value=None, step=0.1)
    manual_rain = st.sidebar.selectbox("Manual Rain", [None, 0, 1])
    manual_danger = st.sidebar.selectbox("Manual Danger Override", [None, "Aman", "Waspada", "Bahaya"])
    submit_manual = st.sidebar.button("Apply Manual Override")

    # ========== MAIN CONTENT ==========
    df = load_data()

    if refresh:
        st.cache_data.clear()
        st.cache_resource.clear()
        df = load_data()
        log_event("Data refreshed manually")

    log_event(f"Dashboard accessed, data points: {len(df)}")

    if df.empty:
        st.info("⏳ Waiting for MQTT data or CSV file...")
        st.stop()

    # Use last row for current status
    last = df.iloc[-1]
    water = last["water_level_cm"]
    rain = last["rain"]
    danger = last.get("danger_level", 0)
    hum = last["humidity_pct"]

    # ===========================
    # DISPLAY UI
    # ===========================
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Water Level", f"{water:.1f} cm")
    with col2:
        status_box("Danger Level", int(danger), mode="danger")
    with col3:
        status_box("Rain Status", int(rain), mode="rain")

    # Charts
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📈 Water Level Trend")
        st.line_chart(df.set_index("datetime")["water_level_cm"])
    with col2:
        st.subheader("🌡️ Temperature & Humidity")
        st.line_chart(df.set_index("datetime")[["temperature_c", "humidity_pct"]])

    # ========== PREDICTION ==========
    fitur = ["water_level_norm", "water_rise_rate", "rain", "humidity_pct"]
    model = load_model()

    if model is not None and not df.empty:
        st.subheader("🤖 ML Prediction (Decision Tree)")
        
        latest = df[fitur].tail(1).copy()
        
        # Apply manual overrides
        pred = None
        confidence = None
        if submit_manual and any([manual_water_level is not None, manual_temp is not None, 
                                manual_humidity is not None, manual_rain is not None, manual_danger is not None]):
            if manual_water_level is not None:
                latest["water_level_norm"] = manual_water_level / STANDARD_WATER_HEIGHT
            if manual_rain is not None:
                latest["rain"] = manual_rain
            if manual_humidity is not None:
                latest["humidity_pct"] = manual_humidity
            
            if manual_danger is not None:
                pred = manual_danger
                st.success("✅ Manual override applied")
            else:
                # Predict with modified data
                if hasattr(model, "predict_proba"):
                    proba = model.predict_proba(latest)
                    confidence = np.max(proba)
                    pred = model.predict(latest)[0]
                else:
                    pred = model.predict(latest)[0]
        else:
            # Normal prediction
            if hasattr(model, "predict_proba"):
                proba = model.predict_proba(latest)
                confidence = np.max(proba)
                pred = model.predict(latest)[0]
            else:
                pred = model.predict(latest)[0]

        # Display prediction
        temp_for_pred = manual_temp if manual_temp is not None else df["temperature_c"].iloc[-1]
        label = pred
        emoji = normalize_emoji(label)

        # Prediction horizon
        rise_rate = last["water_rise_rate"]
        current_level = water
        bahaya_threshold = 35.0
        if rise_rate > 0 and current_level < bahaya_threshold:
            minutes_ahead = ((bahaya_threshold - current_level) / rise_rate) * 60
            st.info(f"⏰ **Prediction Horizon:** {minutes_ahead:.1f} minutes to potential danger level")
        else:
            st.info("✅ Current trends stable or insufficient data for horizon prediction")

        # Hybrid alerts
        if temp_for_pred > 70:
            st.error("🚨 ALARM: Extreme temperature detected!")
            log_event("Force alarm - high temperature")
        elif pred == "Bahaya" and (confidence is None or confidence > 0.8):
            st.error(f"🚨 ALERT: River Status **{pred}** (confidence: {confidence:.1%})")
            log_event(f"ML ALERT: {pred} (confidence: {confidence})")
        else:
            st.markdown(f"""
                <div style="padding:25px; border-radius:15px; background:#0277bd; color:white; text-align:center;">
                    <h2>Current Prediction:</h2>
                    <h1 style="font-size:60px;">{emoji}</h1>
                    <h1>{label}</h1>
                    {f'<p><strong>Confidence:</strong> {confidence:.1%}</p>' if confidence else '<p>No confidence score</p>'}
                </div>
            """, unsafe_allow_html=True)
            log_event(f"Prediction: {pred} (confidence: {confidence})")
    else:
        st.warning("⚠️ Model file not found or data unavailable. Place 'decision_tree.pkl' in the same directory.")

if __name__ == "__main__":
    main()
