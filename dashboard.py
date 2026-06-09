import streamlit as st
import requests
import pandas as pd
import time
import os

st.set_page_config(page_title="Monitorowanie Floty IoT", page_icon="🚚", layout="wide")

API_URL = "http://127.0.0.1:5000"
VEHICLES = ["VOLVO_001", "SCANIA_002", "MAN_003", "DAF_004", "MERCEDES_005", "IVECO_006", "RENAULT_007", "VOLVO_008", "SCANIA_009", "MAN_010", "DAF_011", "MERCEDES_012", "IVECO_013", "RENAULT_014", "FORD_015", "SCANIA_016", "VOLVO_017", "MAN_018", "DAF_019", "IVECO_020"]

def fetch_alerts():
    try:
        response = requests.get(f"{API_URL}/alerts")
        if response.status_code == 200:
            return response.json().get("data", [])
    except:
        pass
    return []

def fetch_vehicles():
    vehicle_data = []
    for v in VEHICLES:
        try:
            response = requests.get(f"{API_URL}/vehicle/{v}/status")
            if response.status_code == 200:
                vehicle_data.append(response.json().get("data", {}))
        except:
            continue
    return vehicle_data

st.title("🚚 Inteligentny System Telemetrii Floty")

tab_live, tab_history = st.tabs(["🔴 Live Dashboard (Strumień)", "📊 Analiza Historyczna (Batch)"])

with tab_live:
    st.sidebar.header("Ustawienia Live")
    auto_refresh = st.sidebar.checkbox("Auto-odświeżanie (co 3 sekundy)", value=False)
    if st.sidebar.button("Odśwież ręcznie"):
        st.rerun()

    alerts = fetch_alerts()
    vehicles = fetch_vehicles()

    col1, col2, col3 = st.columns(3)
    col1.metric("Monitorowane pojazdy (Aktywne)", len(vehicles))
    col2.metric("Suma wygenerowanych alertów", len(alerts))
    critical_alerts = len([a for a in alerts if a.get("alert_level") == "HIGH" or a.get("typ_alarmu") == "CRITICAL_ALERT"])
    col3.metric("Alerty Krytyczne", critical_alerts, delta_color="inverse")
    st.divider()

    map_col, alerts_col = st.columns([2, 1])
    with map_col:
        st.subheader("📍 Bieżąca lokalizacja GPS pojazdów")
        if vehicles:
            df_vehicles = pd.DataFrame(vehicles)
            if 'GPS_lat' in df_vehicles.columns and 'GPS_lon' in df_vehicles.columns:
                df_map = df_vehicles[['GPS_lat', 'GPS_lon', 'id_pojazdu']].rename(columns={'GPS_lat': 'lat', 'GPS_lon': 'lon'})
                st.map(df_map, zoom=5)
        else:
            st.info("Brak danych GPS. Upewnij się, że symulator i API działają.")

    with alerts_col:
        st.subheader("⚠️ Dziennik Zdarzeń (Live)")
        if alerts:
            df_alerts = pd.DataFrame(alerts)
            display_cols = [c for c in ['id_pojazdu', 'event_type', 'typ_alarmu', 'temperatura_silnika', 'max_temperatura'] if c in df_alerts.columns]
            st.dataframe(df_alerts[display_cols].tail(15).iloc[::-1], use_container_width=True, hide_index=True)
        else:
            st.success("Brak aktywnych alertów! Flota bezpieczna.")

    st.subheader("📋 Szczegóły bieżącej telemetrii")
    if vehicles:
        st.dataframe(pd.DataFrame(vehicles), use_container_width=True, hide_index=True)

with tab_history:
    st.header("Archiwum i Raportowanie (Batch Processing)")
    
    file_path = "dane_historyczne.json"
    if os.path.exists(file_path):
        # Wczytanie pliku JSON Lines za pomocą Pandas
        df_hist = pd.read_json(file_path, lines=True)
        df_hist['timestamp'] = pd.to_datetime(df_hist['timestamp'])
        
        st.success(f"Pomyślnie załadowano historyczny wsad danych: {len(df_hist)} rekordów.")
        
        col_hist1, col_hist2 = st.columns(2)
        
        with col_hist1:
            st.subheader("🚨 Top 5 piratów drogowych (> 80 km/h)")
            speeders = df_hist[df_hist['predkosc_kmh'] > 80]
            top_speeders = speeders['id_pojazdu'].value_counts().head(5)
            st.bar_chart(top_speeders, color="#ff4b4b")
            
        with col_hist2:
            st.subheader("🌡️ Średnia temperatura silnika i prędkość")
            avg_stats = df_hist.groupby('id_pojazdu')[['temperatura_silnika', 'predkosc_kmh']].mean().round(1)
            st.dataframe(avg_stats, use_container_width=True)
            
        st.subheader("📈 Profil bazowy kierowców (Prędkość w oknach czasowych)")
        # Symulacja okien czasowych w Pandas
        df_hist.set_index('timestamp', inplace=True)
        windows = df_hist.groupby(['id_pojazdu', pd.Grouper(freq='5min')])['predkosc_kmh'].mean().round(1).reset_index()
        windows.rename(columns={'timestamp': 'okno_czasowe', 'predkosc_kmh': 'srednia_predkosc_w_oknie'}, inplace=True)
        st.dataframe(windows.sort_values(by=['id_pojazdu', 'okno_czasowe']), use_container_width=True, hide_index=True)
        
    else:
        st.warning(f"Brak pliku '{file_path}'. Uruchom najpierw skrypt 'zbieracz_danych.py', aby zbudować Data Lake!")

if auto_refresh:
    time.sleep(3)
    st.rerun()