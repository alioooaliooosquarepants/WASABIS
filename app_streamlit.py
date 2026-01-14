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
# CONFIG (NO WIDGETS HERE)
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
    """🔥 CREATE/ENSURE CSV EXISTS WITH PROPER HEADERS"""
    if not os.path.exists(CSV_FILE):
        df_init = pd.DataFrame(columns=[
            "timestamp", "datetime", "water_level_cm", "temperature_c", 
            "humidity_pct", "danger_level", "rain_level"
        ])
        df_init.to_csv(CSV_FILE, index=False)
        st.sidebar.success(f"✅ Created {CSV_FILE} for GitHub sync")

# ===========================
# LOGGING
# ===========================
logging.basicConfig(filename="audit_log.txt", level=logging.INFO, format='%(asctime)s %(message)s')

def log_event(event):
    logging.info(event)

# ===========================
# 🔥 NEW: CSV APPENDING FUNCTION
# ===========================
def append_to_csv(row):
    """🔥 APPEND SINGLE ROW TO CSV (GitHub ready)"""
    try:
        ensure_csv_header()
        df_new = pd.DataFrame([row])
        df_new.to_csv(CSV_FILE, mode='a', header=False, index=False)
        return True
    except Exception as e:
        logging.error(f"CSV append error: {e}")
        return False

# ===========================
# MQTT CALLBACKS - NOW SAVES TO CSV
# ===========================
def mqtt_model_callback(client, userdata, message):
    try:
        payload = message.payload.decode()
        meta = json.loads(payload)
        model_b64 = meta.get("model_b64")
        if model_b64:
            sig = meta.get("signature")
            if sig and MQTT_MODEL_SECRET:
                calc = hmac.new(MQTT_MODEL_SECRET.encode('utf-8'), model_b64.encode('utf-8'), hashlib.sha256).hexdigest()
                if not hmac.compare_digest(calc, sig):
                    logging.error("Model signature mismatch")
                    return
            model_bytes = base64.b64decode(model_b64.encode('ascii'))
            with open(MODEL_FILE, 'wb') as mf:
                mf.write(model_bytes)
            try:
                st.cache_resource.clear()
            except:
                pass
            logging.info("Model updated via MQTT")
    except Exception as e:
        logging.exception("Model MQTT error: %s", e)

def mqtt_data_callback(client, userdata, message):
    """🔥 MODIFIED: NOW ALSO SAVES TO CSV FOR GITHUB"""
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
        
        # 🔥 ADD TO MEMORY BUFFER (for real-time display)
        with buf_lock:
            recent_buf.append(row)
        
        # 🔥 NEW: SAVE TO CSV (for GitHub persistence)
        append_to_csv(row)
        log_event(f"Data saved to CSV: water={row['water_level_cm']:.1f}cm")
        
    except Exception as e:
        logging.exception("MQTT data error: %s", e)

# ===========================
# MAIN FUNCTION
# ===========================
def main():
    # ===========================
    # STREAMLIT SETTINGS FIRST
    # ===========================
    st.set_page_config(page_title="River Monitor Dashboard", layout="wide")
    st.title("🌊 River Monitoring Dashboard — Real-Time + GitHub CSV")

    st.markdown("""
    <style>
    body { background-color: #e0f7fa; }
    </style>
    """, unsafe_allow_html=True)

    # ===========================
    # SIDEBAR - ALL WIDGETS HERE
    # ===========================
    st.sidebar.title("⚙️ Configuration")
    
    # MQTT Settings
    mqtt_broker = st.sidebar.text_input("MQTT Broker", value="broker.hivemq.com", key="mqtt_broker")
    mqtt_port = st.sidebar.number_input("MQTT Port", value=1883, min_value=1, max_value=65535, key="mqtt_port")
    mqtt_data_topic = st.sidebar.text_input("Data Topic", value="river/monitoring/data", key="mqtt_data_topic")
    
    # Dashboard Settings  
    standard_water_height = st.sidebar.number_input(
        "Standard Water Height (cm)", 
        min_value=0.0, max_value=1000.0, 
        value=50.0, step=1.0, 
        key="standard_height"
    )
    
    # 🔥 CSV STATUS
    st.sidebar.subheader("📊 CSV Status")
    if os.path.exists(CSV_FILE):
        csv_size = os.path.getsize(CSV_FILE)
        st.sidebar.metric("CSV Size", f"{csv_size/1000:.1f} KB")
        st.sidebar.success("✅ Ready for GitHub sync")
    else:
        st.sidebar.info("No CSV yet - waiting for MQTT data")
    
    # Manual Override
    st.sidebar.subheader("🔧 Manual Override")
    manual_water = st.sidebar.number_input("Water Level (cm)", min_value=0.0, max_value=1000.0, value=None, key="manual_water")
    manual_temp = st.sidebar.number_input("Temperature (°C)", min_value=-10.0, max_value=80.0, value=None, key="manual_temp")
    manual_humidity = st.sidebar.number_input("Humidity (%)", min_value=0.0, max_value=100.0, value=None, key="manual_humidity")
    manual_rain = st.sidebar.selectbox("Rain", [None, 0, 1], key="manual_rain")
    manual_danger = st.sidebar.selectbox("Danger Override", [None, "Aman", "Waspada", "Bahaya"], key="manual_danger")
    apply_manual = st.sidebar.button("Apply Override", key="apply_manual")
    
    refresh_btn = st.sidebar.button("🔄 Refresh Data", key="refresh")

    # ===========================
    # MQTT SETUP
    # ===========================
    @st.cache_resource
    def init_mqtt(broker, port, data_topic, model_topic):
        ensure_csv_header()  # 🔥 ENSURE CSV EXISTS ON START
        client = mqtt.Client()
        client.on_message = lambda c,u,m: (
            mqtt_model_callback(c,u,m) if m.topic == model_topic else mqtt_data_callback(c,u,m)
        )
        try:
            client.connect(broker, int(port))
            client.subscribe(data_topic)
            client.subscribe(model_topic)
            client.loop_start()
            st.sidebar.success("✅ MQTT Connected + CSV Ready")
            return client
        except Exception as e:
            st.sidebar.error(f"❌ MQTT Error: {e}")
            return None

    mqtt_client = init_mqtt(mqtt_broker, mqtt_port, mqtt_data_topic, MQTT_MODEL_TOPIC)

    # ===========================
    # DATA LOADING
    # ===========================
    @st.cache_data(ttl=REFRESH_INTERVAL)
    def load_data(std_height):
        with buf_lock:
            df = pd.DataFrame(list(recent_buf))
        
        # 🔥 ALSO LOAD FROM CSV AS BACKUP
        if df.empty and os.path.exists(CSV_FILE):
            csv_df = pd.read_csv(CSV_FILE)
            df = pd.concat([df, csv_df.tail(RECENT_MAX)], ignore_index=True)
        
        if not df.empty:
            # Data validation
            df = df[(df["water_level_cm"].between(0, 1000)) &
                   (df["temperature_c"].between(-10, 80)) &
                   (df["humidity_pct"].between(0, 100))]
            
            # Fill missing data
            df = df.fillna(method="ffill").fillna(method="bfill")
            
            # Feature engineering
            df["water_level_norm"] = df["water_level_cm"] / std_height
            df["water_rise_rate"] = df["water_level_cm"].diff().fillna(0)
            df["rain"] = (df["rain_level"] > 0).astype(int)
            
        return df

    # Load data
    if refresh_btn:
        st.cache_data.clear()
    
    df = load_data(standard_water_height)
    
    if df.empty:
        st.info("⏳ Waiting for MQTT data... CSV will be created automatically.")
        st.stop()

    # ===========================
    # CURRENT STATUS
    # ===========================
    last = df.iloc[-1]
    water = last["water_level_cm"]
    rain = last["rain"]
    danger = last.get("danger_level", 0)

    # Status cards
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Water Level", f"{water:.1f} cm")
    with col2:
        status_box("Danger Level", int(danger), "danger")
    with col3:
        status_box("Rain", int(rain), "rain")

    # ===========================
    # CHARTS
    # ===========================
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📈 Water Level")
        chart_data = df.set_index("datetime")["water_level_cm"] if "datetime" in df else df["water_level_cm"]
        st.line_chart(chart_data)
    
    with col2:
        st.subheader("🌡️ Environment")
        env_data = pd.DataFrame({
            "Temp": df["temperature_c"],
            "Humidity": df["humidity_pct"]
        })
        st.line_chart(env_data)

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
    features = ["water_level_norm", "water_rise_rate", "rain", "humidity_pct"]
    
    if model and not df.empty:
        st.subheader("🤖 ML Prediction")
        
        latest = df[features].iloc[-1:].copy()
        
        # Manual override
        if apply_manual:
            if manual_water is not None:
                latest["water_level_norm"] = manual_water / standard_water_height
            if manual_rain is not None:
                latest["rain"] = manual_rain
            if manual_humidity is not None:
                latest["humidity_pct"] = manual_humidity
        
        try:
            if hasattr(model, 'predict_proba'):
                proba = model.predict_proba(latest)
                confidence = np.max(proba)
                prediction = model.predict(latest)[0]
            else:
                prediction = model.predict(latest)[0]
                confidence = None
            
            # Display
            emoji = normalize_emoji(prediction)
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""
                <div style="padding:25px; border-radius:15px; background:#0277bd; color:white; text-align:center;">
                    <h2>Prediction</h2>
                    <h1 style="font-size:60px;">{emoji}</h1>
                    <h2>{prediction}</h2>
                    {f'<p>Confidence: {confidence:.1%}</p>' if confidence else ''}
                </div>
                """, unsafe_allow_html=True)
            
            # Risk horizon
            rise_rate = df["water_rise_rate"].iloc[-1]
            if rise_rate > 0:
                time_to_danger = (35 - water) / rise_rate * 60
                st.info(f"⏰ Estimated {time_to_danger:.0f} min to danger level")
                
        except Exception as e:
            st.error(f"Prediction error: {e}")
    else:
        st.warning("⚠️ No ML model found. Place 'decision_tree.pkl' in app directory.")

if __name__ == "__main__":
    main()
