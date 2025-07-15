# main_app.py

import streamlit as st

# IMPORTANT: Set page config FIRST, before any other streamlit commands or imports that might use streamlit
st.set_page_config(
    page_title="Dashboard SIMA",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📊"
)

import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Imports locales
from core.auth import AuthManager
from core.data_loader import DataLoader
from core.filters import FilterManager
from sections.coctel_sections import CoctelSections
from function_users import usarios_acontecimientos_dashboard

class DashboardApp:
    """Aplicación principal del dashboard con todas las secciones migradas"""
    
    def __init__(self):
        self.auth_manager = AuthManager()
        self.data_loader = DataLoader()
        
    def show_header(self):
        """Mostrar header de la aplicación"""
        col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
        
        with col1:
            st.title("📊 Dashboard SIMA")
            
        with col2:
            # Mostrar usuario actual
            user = self.auth_manager.get_current_user()
            if user:
                st.success(f"👤 Bienvenido, {user['name']} ({user['role']})")
                
        with col4:
            # Botón de logout
            if st.button("🚪 Cerrar Sesión", type="secondary"):
                self.auth_manager.logout()
                st.rerun()
        
        st.divider()
    
    def show_last_update(self):
        """Mostrar fecha de última actualización"""
        with st.spinner("Obteniendo fecha de última actualización..."):
            last_updated_date = self.data_loader.get_last_update_date()
            
        st.sidebar.markdown("---")
        st.sidebar.info(f"📅 Última actualización: {last_updated_date}")
    
    def show_data_summary(self, filtered_data: pd.DataFrame):
        """Mostrar resumen de datos en sidebar"""
        st.sidebar.markdown("---")
        st.sidebar.subheader("📊 Resumen de Datos")
        
        if not filtered_data.empty:
            total_records = len(filtered_data)
            coctel_records = len(filtered_data[filtered_data['coctel'] == 1])
            coctel_percentage = (coctel_records / total_records * 100) if total_records > 0 else 0
            
            # Métricas principales
            col1, col2 = st.sidebar.columns(2)
            with col1:
                st.metric("Total Registros", f"{total_records:,}")
                st.metric("% Coctel", f"{coctel_percentage:.1f}%")
            with col2:
                st.metric("Con Coctel", f"{coctel_records:,}")
                st.metric("Sin Coctel", f"{total_records - coctel_records:,}")
                
            # Top 3 lugares por coctel
            if 'lugar' in filtered_data.columns:
                top_lugares = filtered_data[filtered_data['coctel'] == 1]['lugar'].value_counts().head(3)
                if not top_lugares.empty:
                    st.sidebar.markdown("**🏆 Top 3 Lugares:**")
                    for lugar, count in top_lugares.items():
                        st.sidebar.write(f"• {lugar}: {count}")
                        
            # Distribución por fuente
            if 'id_fuente' in filtered_data.columns:
                st.sidebar.markdown("**📊 Por Fuente:**")
                fuente_map = {1: 'Radio', 2: 'TV', 3: 'Redes'}
                for fuente_id, fuente_name in fuente_map.items():
                    count = len(filtered_data[filtered_data['id_fuente'] == fuente_id])
                    if count > 0:
                        st.sidebar.write(f"• {fuente_name}: {count}")
        else:
            st.sidebar.warning("No hay datos con los filtros actuales")
    
    def apply_filters_to_data(self, data: pd.DataFrame, global_filters: dict) -> pd.DataFrame:
        """Aplicar filtros globales a los datos"""
        filtered_data = data.copy()
        
        # Aplicar filtro de fechas
        if global_filters.get('use_global_dates'):
            inicio = global_filters['global_fecha_inicio']
            fin = global_filters['global_fecha_fin']
            filtered_data = filtered_data[
                (filtered_data['fecha_registro'] >= inicio) &
                (filtered_data['fecha_registro'] <= fin)
            ]
            
        # Aplicar filtro de ubicaciones
        if global_filters.get('use_global_locations'):
            lugares = global_filters['global_lugares']
            if lugares:  # Solo aplicar si hay lugares seleccionados
                filtered_data = filtered_data[filtered_data['lugar'].isin(lugares)]
                
        # Aplicar filtro de fuentes
        if global_filters.get('use_global_sources'):
            fuentes = global_filters['global_fuentes']
            if fuentes:  # Solo aplicar si hay fuentes seleccionadas
                # Mapear nombres de fuentes a IDs
                fuente_map = {'Radio': 1, 'TV': 2, 'Redes': 3}
                fuente_ids = [fuente_map[f] for f in fuentes if f in fuente_map]
                filtered_data = filtered_data[filtered_data['id_fuente'].isin(fuente_ids)]
                
        return filtered_data
    
    def show_section_navigation(self):
        """Mostrar navegación de secciones en la sidebar"""
        st.sidebar.markdown("---")
        st.sidebar.subheader("🧭 Navegación Rápida")
        st.sidebar.markdown("*Utiliza scroll down para ver todas las secciones*")
        
        # Información sobre las secciones disponibles
        st.sidebar.markdown("""
        **📊 Secciones Disponibles:**
        - SN-1: Proporciones básicas
        - 2-7: Análisis temporal y tendencias  
        - 8-10: Distribución de posiciones
        - 11-14: Análisis por región/fuente
        - 15-20: Análisis de contenido
        - 21-24: Análisis comparativo
        - 25-26: Reportes especializados
        """)
        
    
    def run_coctel_dashboard(self):
        """Ejecutar dashboard completo de cocteles con todas las secciones"""
        # Cargar datos
        with st.spinner("Cargando datos de cocteles..."):
            data_tuple = self.data_loader.load_coctel_data()
            
        temp_coctel_fuente = data_tuple[2]
        
        # Configurar filtros
        lugares_uniques = data_tuple[7]
        filter_manager = FilterManager(lugares_uniques)
        
        # Establecer límites de fechas basados en los datos
        min_date = temp_coctel_fuente['fecha_registro'].min().date()
        max_date = temp_coctel_fuente['fecha_registro'].max().date()
        
        st.write(f"min_date calculated: {min_date}")
        st.write(f"max_date calculated: {max_date}")
        
        filter_manager.set_date_bounds(min_date, max_date)
        
        st.write(f"Filter manager min_date after set_date_bounds: {filter_manager.min_date}")
        st.write(f"Filter manager max_date after set_date_bounds: {filter_manager.max_date}")
        st.write("==================")

                
        # Crear filtros globales
        global_filters = filter_manager.create_global_filters()
        if global_filters is None:
            st.stop()
            
        # Aplicar filtros globales a los datos
        filtered_data = self.apply_filters_to_data(temp_coctel_fuente, global_filters)
        
        # Mostrar resumen de datos
        self.show_data_summary(filtered_data)
        
        # Mostrar navegación de secciones
        self.show_section_navigation()
        
        # Crear secciones
        sections = CoctelSections(data_tuple, filter_manager)
        
        # Título principal y descripción
        st.header("🍸 Análisis Completo de Cocteles")
                
        # Instrucciones de uso
        with st.expander("💡 Cómo usar el dashboard"):
            st.markdown("""
            **🎯 Filtros Globales (Recomendado):**
            1. En el sidebar, activa "Usar fechas globales"
            2. Selecciona el rango de fechas deseado
            3. Activa "Usar ubicaciones globales" 
            4. Selecciona las ubicaciones de interés
            5. ¡Los filtros se aplicarán automáticamente a TODAS las secciones que puedan aplicar!
            
            **📊 Navegación:**
            - **Scroll down**: Para ver todas las secciones en orden
            - **Checkbox global**: "Mostrar todos los porcentajes" afecta los gráficos
            - **Filtros específicos**: Desactiva los globales para control por sección
            """)
        
        st.markdown("---")
        
        # Renderizar TODAS las secciones en orden secuencial
        sections.render_all_sections(global_filters)

    
    def run_users_dashboard(self):
        """Ejecutar dashboard de usuarios"""
        st.header("👥 Usuarios y Acontecimientos")
        
        # Verificar permisos
        user = self.auth_manager.get_current_user()
        if user['role'] == 'viewer':
            st.warning("⚠️ No tienes permisos para ver esta sección")
            return
            
        # Información sobre la sección
        st.info("📋 Esta sección mantiene la funcionalidad original sin modificaciones")
        
        # Ejecutar dashboard original de usuarios
        usarios_acontecimientos_dashboard()
    
    def run(self):
        """Ejecutar aplicación principal"""
        # Note: page config is now set at the top of the file
        
        # Verificar autenticación
        if not self.auth_manager.is_logged_in():
            self.auth_manager.login_form()
            return
            
        # Mostrar header
        self.show_header()
                
        # Mostrar fecha de actualización
        self.show_last_update()
        
        # Menú de navegación principal
        st.sidebar.title("🧭 Navegación Principal")
        menu = st.sidebar.radio(
            "Selecciona una sección:",
            ["🍸 Análisis de Cocteles", "👥 Usuarios y Acontecimientos"],
            key="main_menu"
        )
                
        # Ejecutar sección seleccionada
        if menu == "🍸 Análisis de Cocteles":
            self.run_coctel_dashboard()
        elif menu == "👥 Usuarios y Acontecimientos":
            self.run_users_dashboard()

# =====================================================
# Punto de entrada principal
# =====================================================

if __name__ == "__main__":
    # Configurar variables de entorno si es necesario
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # Si no está disponible python-dotenv, continuar sin cargar .env
    
    print("=" * 50)
    
    # Ejecutar aplicación
    app = DashboardApp()
    app.run()