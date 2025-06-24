#%% Libraries
import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from utils import get_query 
from function_cocteles import coctel_dashboard
from function_users import usarios_acontecimientos_dashboard

from dotenv import load_dotenv
load_dotenv()
    
#%% Formateo del nombre del sitio y site config
st.set_page_config(page_title="Dashboard SIMA", layout="centered")
st.sidebar.title("Opciones")
st.title("Dashboard SIMA")
st.divider()

#%%% Agregar opción al menú lateral
# Envolvemos la carga en un try/except para ver el error en pantalla
with st.spinner("Obteniendo fecha de última actualización…"):
    try:
        df_fecha = get_query("cocteles", "ultima_fecha")
    except Exception as e:
        st.error("❌ Error al obtener la fecha de actualización:")
        st.exception(e)
        st.stop()
        
if not df_fecha.empty:
    ut = pd.to_datetime(df_fecha["ultima_fecha"].iloc[0])
    if ut.tzinfo is None:
        ut = ut.tz_localize("UTC")
    ut = ut.tz_convert("America/Lima")
    last_updated_date = ut.strftime("%d/%m/%Y")
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

