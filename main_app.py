# main_app.py (Fixed version with proper page config placement)

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
        """Ejecutar dashboard de cocteles con selector de secciones"""
        st.header("🍸 Análisis de Cocteles")
        
        # Cargar datos
        with st.spinner("Cargando datos..."):
            data_tuple = self.data_loader.load_coctel_data()
            
        # Configurar filtros
        lugares_uniques = data_tuple[7]  # lugares_uniques está en la posición 7
        
        # FIX: Convertir lugares_uniques a lista si es DataFrame o Series
        if isinstance(lugares_uniques, pd.DataFrame):
            lugares_list = lugares_uniques.iloc[:, 0].unique().tolist()
        elif isinstance(lugares_uniques, pd.Series):
            lugares_list = lugares_uniques.unique().tolist()
        else:
            lugares_list = list(lugares_uniques) if hasattr(lugares_uniques, '__iter__') else []
        
        # Crear FilterManager con la lista limpia
        filter_manager = FilterManager(lugares_list)
        
        # Establecer límites de fechas basados en los datos
        temp_coctel_fuente = data_tuple[2]
        min_date = temp_coctel_fuente['fecha_registro'].min().date()
        max_date = temp_coctel_fuente['fecha_registro'].max().date()
        filter_manager.set_date_bounds(min_date, max_date)
        
        # Crear filtros globales
        global_filters = filter_manager.create_global_filters()
        if global_filters is None:
            st.stop()
        
        # Crear secciones
        sections = CoctelSections(data_tuple, filter_manager)
        
        # ============================================
        # SELECTOR DE SECCIÓN EN EL HEADER
        # ============================================
        st.markdown("### 📊 Seleccionar Análisis")
        
        # Diccionario de secciones disponibles
        secciones_disponibles = {
            "📋 Ver Todas las Secciones (Scroll)": "all",
            "SN. Proporción de cocteles en lugar y fecha específica": "sn",
            "1. Proporción de cocteles en lugar, fuentes y fechas específicas": "1",
            "2. Posición por fuente en lugar y fecha específica": "2",
            "3. Gráfico semanal por porcentaje de cocteles": "3",
            "4. Tendencia A Favor vs En Contra": "4",
            "5. Gráfico Acumulativo": "5",
            "TOP 3. Mejores lugares": "top3",
            "6. Top 3 mejores radios, redes, tv": "6",
            "7. Crecimiento por Macroregión": "7",
            "8. Gráfico de barras contando posiciones": "8",
            "9. Gráfico de dona - porcentaje de posiciones": "9",
            "10. Porcentaje de acontecimientos con coctel": "10",
            "11. Cantidad de cocteles por fuente y lugar": "11",
            "12. Medios que Generan Coctel": "12",
            "13. Conteo mensual de coctel utilizado": "13",
            "14. Notas A Favor, Neutral, En Contra": "14",
            "15. Proporción de Mensajes por Posición": "15",
            "16. Mensajes por Tema": "16",
            "17. Proporción por Tema": "17",
            "18. Tendencia por medio": "18",
            "19. Notas por Tiempo y Posición": "19",
            "20. Actores y Posiciones": "20",
            "21. Porcentaje de cóctel por medios": "21",
            "22. Últimos 3 Meses": "22",
            "23. Evolución mensual (Radio, Redes, TV)": "23",
            "24. Mensajes Fuerza": "24",
            "25. Impactos por programa": "25",
            "26. Distribución por medio": "26",
            "27. A Favor vs En Contra (Mensual)": "27",
        }
        
        # Selectbox para elegir la sección
        seccion_seleccionada = st.selectbox(
            "Elige el análisis que deseas visualizar:",
            options=list(secciones_disponibles.keys()),
            index=1,
            key="selector_seccion"
        )
        
        # Obtener código de la sección
        codigo_seccion = secciones_disponibles[seccion_seleccionada]
        
        # Checkbox global para mostrar valores (solo si no es "all")
        if codigo_seccion != "all":
            mostrar_todos = st.checkbox(
                "Mostrar todos los porcentajes en gráficos", 
                value=True, 
                key="global_mostrar"
            )
        else:
            mostrar_todos = True
        
        # Información de ayuda (colapsable)
        with st.expander("💡 Cómo usar el dashboard"):
            st.markdown("""
            **🔍 Filtros Globales:**
            1. En el sidebar, activa "Usar fechas globales"
            2. Selecciona el rango de fechas deseado
            3. Activa "Usar ubicaciones globales" 
            4. Selecciona las ubicaciones de interés
            5. ¡Los filtros se aplicarán automáticamente!
            
            **📊 Selección de Análisis:**
            - Usa el selector arriba para elegir un análisis específico
            - O selecciona "Ver Todas las Secciones" para scroll continuo (más lento)
            
            **⚡ Consejo de Rendimiento:**
            - Seleccionar análisis individuales carga **mucho más rápido**
            - "Ver Todas" puede tomar varios segundos en cargar 25+ gráficos
            """)
        
        st.markdown("---")
        
        # ============================================
        # RENDERIZAR SECCIÓN SELECCIONADA
        # ============================================
        
        if codigo_seccion == "all":
            # Renderizar todas las secciones (comportamiento original)
            with st.spinner("⏳ Cargando todas las secciones... esto puede tomar un momento"):
                sections.render_all_sections(global_filters)
        else:
            # Renderizar solo la sección seleccionada (¡MUCHO MÁS RÁPIDO!)
            sections.render_single_section(codigo_seccion, global_filters, mostrar_todos)    
        

    #def run_coctel_dashboard(self):
    #    """Ejecutar dashboard completo de cocteles con todas las secciones"""
    #    # Cargar datos
    #    with st.spinner("Cargando datos de cocteles..."):
    #        data_tuple = self.data_loader.load_coctel_data()
    #        
    #    # Configurar filtros
    #    lugares_uniques = data_tuple[7]  # lugares_uniques está en la posición 7
    #    filter_manager = FilterManager(lugares_uniques)
    #    
    #    # Establecer límites de fechas basados en los datos
    #    temp_coctel_fuente = data_tuple[2]
    #    min_date = temp_coctel_fuente['fecha_registro'].min().date()
    #    max_date = temp_coctel_fuente['fecha_registro'].max().date()
    #    filter_manager.set_date_bounds(min_date, max_date)
#
    #            
    #    # Crear filtros globales
    #    global_filters = filter_manager.create_global_filters()
    #    if global_filters is None:
    #        st.stop()
    #        
    #    # Aplicar filtros globales a los datos
    #    filtered_data = self.apply_filters_to_data(temp_coctel_fuente, global_filters)
    #    
    #    # Mostrar resumen de datos
    #    self.show_data_summary(filtered_data)
    #    
    #    # Mostrar navegación de secciones
    #    self.show_section_navigation()
    #    
    #    # Crear secciones
    #    sections = CoctelSections(data_tuple, filter_manager)
    #    
    #    # Título principal y descripción
    #    st.header("🍸 Análisis Completo de Cocteles")
    #            
    #    # Instrucciones de uso
    #    with st.expander("💡 Cómo usar el dashboard"):
    #        st.markdown("""
    #        **🎯 Filtros Globales (Recomendado):**
    #        1. En el sidebar, activa "Usar fechas globales"
    #        2. Selecciona el rango de fechas deseado
    #        3. Activa "Usar ubicaciones globales" 
    #        4. Selecciona las ubicaciones de interés
    #        5. ¡Los filtros se aplicarán automáticamente a TODAS las secciones que puedan aplicar!
    #        
    #        **📊 Navegación:**
    #        - **Scroll down**: Para ver todas las secciones en orden
    #        - **Checkbox global**: "Mostrar todos los porcentajes" afecta los gráficos
    #        - **Filtros específicos**: Desactiva los globales para control por sección
    #        """)
    #    
    #    st.markdown("---")
    #    
    #    # Renderizar TODAS las secciones en orden secuencial
    #    sections.render_all_sections(global_filters)
#
    #
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