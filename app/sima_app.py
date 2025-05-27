#%% Libraries
import streamlit as st
import os
import requests
from datetime import datetime, timedelta
import pytz
from function_cocteles import coctel_dashboard, cargar_coctel_completo
from function_users import usarios_acontecimientos_dashboard

#%% Crear funcion para saber el ip
@st.cache_data(ttl=3600)
def get_egress_ip():
    try:
        # api.ipify.org devuelve tu IP pública en texto plano
        return requests.get("https://api.ipify.org").text.strip()
    except Exception:
        return "no disponible"
    
#%% Formateo del nombre del sitio y site config
st.set_page_config(page_title="Dashboard SIMA", layout="centered")
st.sidebar.title("Opciones")
ip = get_egress_ip()
st.sidebar.write(f"IP de salida: {ip}")
st.title("Dashboard SIMA")
st.divider()

#%%% Agregar opción al menú lateral
# Envolvemos la carga en un try/except para ver el error en pantalla
with st.spinner("Cargando datos desde la base de datos…"):
    try:
        df_coctel, *_ = cargar_coctel_completo()
    except Exception as e:
        st.error("❌ Error al cargar la tabla de cócteles:")
        st.exception(e)
        st.stop()
        
if not df_coctel.empty:
    ut = df_coctel['fecha_registro'].max()
    if ut.tzinfo is None:
        ut = ut.replace(tzinfo=pytz.UTC)
    else:
        ut = ut.tz_convert(pytz.UTC)
    peru_tz = pytz.timezone("America/Lima")
    ut_peru = ut.astimezone(peru_tz)
    last_updated_date = ut_peru.strftime('%d/%m/%Y')
else:
    last_updated_date = "Sin datos disponibles"

st.sidebar.write(f"Fecha de última actualización de datos: {last_updated_date}")
menu = st.sidebar.radio(
    "Selecciona una sección",
    ["Análisis de Cocteles", "Usuarios y Acontecimientos"]
)

if menu == "Análisis de Cocteles":
    coctel_dashboard()

elif menu == "Usuarios y Acontecimientos":
    usarios_acontecimientos_dashboard()

