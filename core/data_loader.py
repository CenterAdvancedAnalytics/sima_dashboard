import pandas as pd
import streamlit as st
import time
from datetime import datetime, timedelta
from typing import Tuple, List
import sys
import os

# Agregar el directorio raíz al path para poder importar utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_query

class DataLoader:
    """Gestor centralizado de carga de datos"""
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def load_coctel_data() -> Tuple:
        """Cargar todos los datos de cocteles con cacheo"""
        print("⏳ [START] load_coctel_data()", flush=True)
        t0 = time.time()
        
        # Cargar datos principales
        temp_coctel_completo = get_query("cocteles", "coctel_completo")
        
        # Procesar datos
        temp_coctel_completo['fecha_registro'] = pd.to_datetime(
            temp_coctel_completo['fecha_registro']
        ).dt.normalize()
        
        temp_coctel_completo['rebote_nombre'] = temp_coctel_completo['canal_nombre'].fillna(
            temp_coctel_completo['nombre_facebook_page']
        )
        
        temp_coctel_completo['coctel'] = pd.to_numeric(
            temp_coctel_completo['coctel'], errors='coerce'
        ).fillna(0.0)
        
        temp_coctel_completo['coctel'] = (temp_coctel_completo['coctel'] != 0).astype(float)
        temp_coctel_completo = temp_coctel_completo[
            temp_coctel_completo["acontecimiento"] != "pRUEBA"
        ]
        temp_coctel_completo['id_fuente'] = temp_coctel_completo['id_fuente'].fillna(3)
        
        # Crear vistas especializadas
        temp_coctel_fuente_notas = temp_coctel_completo[[
            'id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
            'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal', 
            'canal_nombre', 'rebote_nombre'
        ]].copy()
        
        temp_coctel_fuente = temp_coctel_completo[[
            'id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
            'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal', 
            'canal_nombre', 'programa_nombre', 'rebote_nombre'
        ]].copy().drop_duplicates()
        
        temp_coctel_fuente_programas = temp_coctel_fuente.copy()
        
        temp_coctel_fuente_fb = temp_coctel_completo[[
            'id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
            'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal',
            'canal_nombre', 'num_reacciones', 'num_comentarios', 'num_compartidos',
            'fecha_post', 'nombre_facebook_page'
        ]].copy()
        
        temp_coctel_fuente_actores = temp_coctel_completo[[
            'id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
            'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal',
            'canal_nombre', 'nombre'
        ]].copy()
        
        temp_coctel_temas = temp_coctel_completo[[
            'id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
            'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal',
            'canal_nombre', 'descripcion'
        ]].copy()

        # Normalizar nombres de canales
        temp_coctel_fuente_programas['nombre_canal'] = temp_coctel_fuente_programas['canal_nombre']
        temp_coctel_fuente_fb['nombre_canal'] = temp_coctel_fuente_fb['canal_nombre']

        lugares_uniques = temp_coctel_fuente['lugar'].unique().tolist()

        t1 = time.time()
        print(f"✅ [END] load_coctel_data() ({t1-t0:.1f}s)", flush=True)
        
        return (temp_coctel_completo, temp_coctel_fuente_notas, temp_coctel_fuente,
                temp_coctel_fuente_programas, temp_coctel_fuente_fb, 
                temp_coctel_fuente_actores, temp_coctel_temas, lugares_uniques)
    
    @staticmethod
    @st.cache_data(ttl=3600) 
    def load_user_data() -> Tuple:
        """Cargar datos de usuarios"""
        usuarios_por_dia = get_query("usuarios", "usuarios_por_dia")
        acontecimientos_por_dia = get_query("usuarios", "acontecimientos_por_dia")
        usuarios_ultimo_dia = get_query("usuarios", "usuarios_ultimo_dia")
        usuarios_semana = get_query("usuarios", "usuarios_7_dias")
        
        return usuarios_por_dia, acontecimientos_por_dia, usuarios_ultimo_dia, usuarios_semana
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_last_update_date() -> str:
        """Obtener fecha de última actualización"""
        try:
            df_fecha = get_query("cocteles", "ultima_fecha")
            if not df_fecha.empty:
                ut = pd.to_datetime(df_fecha["ultima_fecha"].iloc[0])
                if ut.tzinfo is None:
                    ut = ut.tz_localize("UTC")
                ut = ut.tz_convert("America/Lima")
                return ut.strftime("%d/%m/%Y")
            return "Sin datos disponibles"
        except Exception as e:
            st.error(f"Error al obtener fecha de actualización: {e}")
            return "Error al cargar fecha"