import streamlit as st
import psycopg2
import pandas as pd
from time import sleep
from dotenv import load_dotenv
import os
from streamlit_autorefresh import st_autorefresh
import time


load_dotenv()

host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DB_NAME')
user = os.getenv('USER')
password = os.getenv('PASSWORD')

# Database Connection
def get_connection():
    return psycopg2.connect(
        host='localhost',
        port='5433',
        dbname="sensors",
        user="maodo",
        password="pass123"
    )

# Fetch Data
def fetch_sensor_data(conn):
    query = """
    SELECT sensor_id, timestamp, temperature, humidity, pressure, location
    FROM sensors
    ORDER BY timestamp DESC LIMIT 100; -- Fetch latest 100 readings
    """
    return pd.read_sql_query(query, conn)

# Detect Malfunctioning Sensors
def detect_malfunctioning_sensors(data):
    TEMP_THRESHOLD = (10, 60)  # Threshold for temperature
    
    # Malfunction condition
    malfunctions = data[
        (data['temperature'] < TEMP_THRESHOLD[0]) | 
        (data['temperature'] > TEMP_THRESHOLD[1])
    ]
    print(f"MalFUNC : {malfunctions}")
    return malfunctions

# Streamlit Dashboard
def update_data():
    # Placeholder for Alerts
    alert_placeholder = st.empty()
    
    # Placeholder for Sensor Data Table
    table_placeholder = st.empty()
     # Placeholder to display last refresh time
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    # Real-Time Updates
    conn = get_connection()
    # Fetch latest sensor data
    data = fetch_sensor_data(conn)
    
    # Detect malfunctions
    malfunctions = detect_malfunctioning_sensors(data)
    malfunctions.reset_index(inplace=True)

    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Sensors", len(data))
    col2.metric("Total Malfunctioning sensors", len(malfunctions))
    
    # Update Alerts
    if not malfunctions.empty:
        alert_placeholder.warning(
            f"⚠️ {len(malfunctions)} malfunctioning sensor(s) detected!"
        )
        st.header("List of malfunctioning sensors")
        # st.write(malfunctions)
        st.table(malfunctions)
        # table_placeholder.table(malfunctions)
    else:
        alert_placeholder.success("All sensors are functioning normally!")
    
        # Display all sensors
    # st.header("All deployed sensors")
    # st.table(data)
    # Refresh every 5 seconds
    # Close connection to DB
    st.line_chart(data[['timestamp', 'temperature']].set_index('timestamp'))
    conn.close()
    # Update the last refresh time
    st.session_state['last_update'] = time.time()

# Sidebar layout
def sidebar():
    # Initialize last update time if not present in session state
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    # Slider to control refresh interval
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    # Button to manually refresh data
    if st.sidebar.button('Refresh Data'):
        update_data()

st.title("Real-Time Sensor Monitoring Dashboard")
# Display sidebar
sidebar()
update_data()
