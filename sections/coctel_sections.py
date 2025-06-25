# sections/coctel_sections.py

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.analytics import AnalyticsEngine
from core.filters import FilterManager
from config.constants import *
from typing import Dict, Any, Tuple
from datetime import datetime, timedelta

class CoctelSections:
    """Todas las secciones del dashboard de cocteles migradas"""
    
    def __init__(self, data_tuple: Tuple, filter_manager: FilterManager):
        (self.temp_coctel_completo, self.temp_coctel_fuente_notas, 
         self.temp_coctel_fuente, self.temp_coctel_fuente_programas,
         self.temp_coctel_fuente_fb, self.temp_coctel_fuente_actores,
         self.temp_coctel_temas, self.lugares_uniques) = data_tuple
        
        self.filter_manager = filter_manager
        self.analytics = AnalyticsEngine()
        
    def render_all_sections(self, global_filters: Dict[str, Any]):
        """Renderizar todas las secciones en orden secuencial (scroll down)"""
        
        # Checkbox global para mostrar valores
        mostrar_todos = st.checkbox("Mostrar todos los porcentajes en gráficos", value=True, key="global_mostrar")
        
        st.markdown("---")
        
        # SECCIÓN SN - Proporción básica de cocteles
        self.section_sn_proporcion_basica(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 1 - Proporción combinada
        self.section_1_proporcion_combinada(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 2 - Posición por fuente
        self.section_2_posicion_por_fuente(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 3 - Tendencia semanal
        self.section_3_tendencia_semanal(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 4 - Tendencia a favor vs en contra
        self.section_4_favor_vs_contra(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 5 - Gráfico acumulativo
        self.section_5_grafico_acumulativo(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN TOP 3 - Mejores lugares
        self.section_top3_mejores_lugares(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 6 - Top medios
        self.section_6_top_medios(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 7 - Crecimiento por macroregión
        self.section_7_macroregion(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 8 - Conteo de posiciones
        self.section_8_conteo_posiciones(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 9 - Distribución de posiciones (dona)
        self.section_9_distribucion_posiciones(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 10 - Eventos con coctel
        self.section_10_eventos_coctel(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 11 - Cocteles por fuente y lugar
        self.section_11_cocteles_fuente_lugar(global_filters)
        st.markdown("---")
        
        # SECCIÓN 12 - Medios que generan coctel
        self.section_12_medios_generan_coctel(global_filters)
        st.markdown("---")
        
        # SECCIÓN 13 - Conteo mensual
        self.section_13_conteo_mensual(global_filters)
        st.markdown("---")
        
        # SECCIÓN 14 - Notas a favor, neutral, en contra
        self.section_14_notas_favor_contra(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 15 - Proporción de mensajes por posición
        self.section_15_proporcion_mensajes(global_filters)
        st.markdown("---")
        
        # SECCIÓN 16 - Mensajes por tema
        self.section_16_mensajes_por_tema(global_filters)
        st.markdown("---")
        
        # SECCIÓN 17 - Proporción por tema
        self.section_17_proporcion_por_tema(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 18 - Tendencia por medio
        self.section_18_tendencia_por_medio(global_filters)
        st.markdown("---")
        
        # SECCIÓN 19 - Notas por tiempo y posición
        self.section_19_notas_tiempo_posicion(global_filters)
        st.markdown("---")
        
        # SECCIÓN 20 - Actores y posiciones
        self.section_20_actores_posiciones(global_filters)
        st.markdown("---")
        
        # SECCIÓN 21 - Porcentaje de cóctel por medios
        self.section_21_porcentaje_medios(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 22 - Últimos 3 meses
        self.section_22_ultimos_3_meses(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 23 - Evolución mensual
        self.section_23_evolucion_mensual(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 24 - Mensajes fuerza
        self.section_24_mensajes_fuerza(global_filters, mostrar_todos)
        st.markdown("---")
        
        # SECCIÓN 25 - Impactos por programa
        self.section_25_impactos_programa(global_filters)
        st.markdown("---")
        
        # SECCIÓN 26 - Distribución por medio
        self.section_26_distribucion_medio(global_filters)
        st.markdown("---")
        
        # SECCIÓN 27 - A favor vs en contra mensual
        self.section_27_favor_contra_mensual(global_filters)
        
    # =====================================================
    # IMPLEMENTACIÓN DE TODAS LAS SECCIONES
    # =====================================================
    
    def section_sn_proporcion_basica(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """SN.- Proporción de cocteles en lugar y fecha específica"""
        st.subheader("SN.- Proporción de cocteles en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("sn", global_filters)
        
        if not global_filters.get('use_global_locations'):
            option_lugar = st.selectbox("Lugar", self.lugares_uniques, key="lugar_sn")
        else:
            option_lugar = st.selectbox("Lugar", global_filters['global_lugares'], key="lugar_sn")
        
        # Filtrar datos
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            result = self.analytics.calculate_coctel_proportion(temp_data)
            
            st.write(f"Proporción de cocteles en {option_lugar} entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write("**Radio**")
                if "Radio" in result and not result["Radio"].empty:
                    st.dataframe(result["Radio"], hide_index=True)
                else:
                    st.write("Sin datos")
                    
            with col2:
                st.write("**TV**")
                if "TV" in result and not result["TV"].empty:
                    st.dataframe(result["TV"], hide_index=True)
                else:
                    st.write("Sin datos")
                    
            with col3:
                st.write("**Redes**")
                if "Redes" in result and not result["Redes"].empty:
                    st.dataframe(result["Redes"], hide_index=True)
                else:
                    st.write("Sin datos")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_8_conteo_posiciones(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """8.- Gráfico de barras contando posiciones"""
        st.subheader("8.- Gráfico de barras contando posiciones en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s8", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s8"
            )
        with col2:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s8")
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            conteo_data = self.analytics.calculate_position_count(temp_data, "Todos", option_nota)
            
            if not conteo_data.empty:
                titulo = f'Conteo de posiciones {option_nota.lower()} en {option_lugar} por tipo de medio'
                
                fig = px.bar(
                    conteo_data,
                    x='Posición',
                    y='count',
                    color='Tipo de Medio',
                    title=titulo,
                    barmode='group',
                    labels={'count': 'Conteo', 'Posición': 'Posición', 'Tipo de Medio': 'Tipo de Medio'},
                    color_discrete_map=FUENTE_COLORS,
                    text='count'
                )
                
                fig.update_layout(
                    xaxis_title='Posición',
                    yaxis_title='Conteo',
                    legend_title='Tipo de Medio'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_9_distribucion_posiciones(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """9.- Gráfico de dona que representa el porcentaje de posiciones"""
        st.subheader("9.- Gráfico de dona que representa el porcentaje de posiciones en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s9", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s9")
        with col2:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s9")
        
        option_lugares = self.filter_manager.get_section_locations("s9", global_filters, multi=True)
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        if not temp_data.empty:
            distrib_data = self.analytics.calculate_position_distribution(temp_data, option_fuente, option_nota)
            
            if not distrib_data.empty:
                if option_nota == 'Con coctel':
                    titulo = 'Porcentaje de posiciones con coctel respecto del total'
                elif option_nota == 'Sin coctel':
                    titulo = 'Porcentaje de posiciones sin coctel respecto del total'
                else:
                    titulo = 'Porcentaje de posiciones respecto del total'
                
                fig = px.pie(
                    distrib_data,
                    values='count',
                    names='Posición',
                    title=titulo,
                    color='Posición',
                    color_discrete_map=COLOR_POSICION_DICT,
                    hole=0.3
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_10_eventos_coctel(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """10.- Porcentaje de acontecimientos con coctel"""
        st.subheader("10.- Porcentaje de acontecimientos con coctel en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s10", global_filters)
        
        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s10")
        option_lugares = self.filter_manager.get_section_locations("s10", global_filters, multi=True)
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        if not temp_data.empty:
            event_data = self.analytics.calculate_coctel_events_distribution(temp_data, option_fuente)
            
            if not event_data.empty:
                st.write(f"Porcentaje de acontecimientos con coctel en {', '.join(option_lugares)}")
                
                fig = px.pie(
                    event_data,
                    values='count',
                    names='Coctel',
                    title='Porcentaje de acontecimientos con coctel',
                    hole=0.3,
                    color='Coctel',
                    color_discrete_map={'Sin coctel': 'orange', 'Con coctel': 'Blue'}
                )
                
                fig.update_traces(
                    textposition='inside' if mostrar_todos else 'none',
                    textinfo='label+percent' if mostrar_todos else 'label'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_11_cocteles_fuente_lugar(self, global_filters: Dict[str, Any]):
        """11.- Cantidad de cocteles por fuente y lugar"""
        st.subheader("11.- Cantidad de cocteles por fuente y lugar en fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s11", global_filters)
        option_lugares = self.filter_manager.get_section_locations("s11", global_filters, multi=True)
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        if not temp_data.empty:
            result = self.analytics.calculate_coctel_by_source_location(temp_data)
            
            if not result.empty:
                st.write(f"Cantidad de cocteles por fuente y lugar entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
                st.dataframe(result, hide_index=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_12_medios_generan_coctel(self, global_filters: Dict[str, Any]):
        """12.- Reporte semanal acerca de cuantas radios, redes y tv generaron coctel"""
        st.subheader("12.- Reporte semanal acerca de cuantas radios, redes y tv generaron coctel")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s12", global_filters)
        option_lugares = self.filter_manager.get_section_locations("s12", global_filters, multi=True)
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        temp_fb = self.temp_coctel_fuente_fb[
            (self.temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente_fb['lugar'].isin(option_lugares))
        ]
        
        if not temp_data.empty:
            tabla_data, chart_data = self.analytics.calculate_media_generating_coctel(temp_data, temp_fb)
            
            if not tabla_data.empty:
                st.dataframe(tabla_data, hide_index=True)
                
                y_max = chart_data['Cantidad'].max() * 1.1
                
                fig = px.bar(
                    chart_data,
                    x='lugar',
                    y='Cantidad',
                    color='Fuente',
                    text='Cantidad',
                    title="Cantidad de Medios (Canales) que generan Cocteles por Lugar",
                    labels={'Cantidad': 'Número de Medios', 'lugar': 'Lugar', 'Fuente': 'Fuente'},
                    color_discrete_map=FUENTE_COLORS,
                    barmode='stack'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_13_conteo_mensual(self, global_filters: Dict[str, Any]):
        """13.- Conteo mensual de la cantidad de coctel utilizado por región"""
        st.subheader("13.- Conteo mensual de la cantidad de coctel utilizado por región, dividido en redes, radio y tv")
        
        # Selectores de fecha
        ano_actual = datetime.now().year
        anos = list(range(ano_actual - 9, ano_actual + 1))
        
        col1, col2 = st.columns(2)
        with col1:
            year_inicio = st.selectbox("Año de inicio", anos, len(anos)-1, key="year_inicio_s13")
            month_inicio = st.selectbox("Mes de inicio", list(range(1,13)), index=0, key="month_inicio_s13")
        with col2:
            year_fin = st.selectbox("Año de fin", anos, index=len(anos)-1, key="year_fin_s13")
            month_fin = st.selectbox("Mes de fin", list(range(1,13)), index=11, key="month_fin_s13")
        
        option_lugares = self.filter_manager.get_section_locations("s13", global_filters, multi=True)
        
        # Calcular rango de fechas
        fecha_inicio = pd.to_datetime(f'{year_inicio}-{month_inicio}-01')
        fecha_fin = pd.to_datetime(f'{year_fin}-{month_fin}-01') + pd.offsets.MonthEnd(1)
        
        # Filtrar datos principales
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        # Filtrar datos de Facebook (si existe)
        temp_data_fb = None
        if hasattr(self, 'temp_coctel_fuente_fb') and self.temp_coctel_fuente_fb is not None:
            temp_data_fb = self.temp_coctel_fuente_fb[
                (self.temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio) &
                (self.temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin) &
                (self.temp_coctel_fuente_fb['lugar'].isin(option_lugares))
            ]
        
        if not temp_data.empty:
            # Usar el método actualizado
            by_location, by_month = self.analytics.calculate_monthly_coctel_count(temp_data, temp_data_fb)
            
            st.write(f"Conteo mensual de coctel en {len(option_lugares)} regiones entre {fecha_inicio.strftime('%m/%Y')} y {fecha_fin.strftime('%m/%Y')}")
            st.dataframe(by_location, hide_index=True)
            
            # Gráfico
            fig = px.bar(
                by_month,
                x='año_mes',
                y='coctel',
                color='Fuente',
                barmode='stack',
                title='Conteo de cocteles por mes y fuente',
                labels={'año_mes': 'Año y Mes', 'coctel': 'Número de Cocteles', 'Fuente': 'Fuente'},
                text='coctel',
                color_discrete_map=FUENTE_COLORS,
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos para mostrar")


    # 3. Verificar que tienes el dataset FB en tu clase principal
    def verify_facebook_dataset(self):
        """Verificar si el dataset de Facebook está disponible"""
        if hasattr(self, 'temp_coctel_fuente_fb'):
            if self.temp_coctel_fuente_fb is not None and not self.temp_coctel_fuente_fb.empty:
                print(f"✅ Dataset FB disponible: {self.temp_coctel_fuente_fb.shape[0]} registros")
                print(f"🔍 Fuentes en FB: {self.temp_coctel_fuente_fb['id_fuente'].unique()}")
                return True
            else:
                print("❌ Dataset FB está vacío o es None")
                return False
        else:
            print("❌ No existe atributo temp_coctel_fuente_fb")
            return False
    
    def section_14_notas_favor_contra(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """14.- Porcentaje de notas que sean a favor, neutral y en contra"""
        st.subheader("14.- Porcentaje de notas que sean a favor, neutral y en contra")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s14", global_filters)
        
        option_nota = st.selectbox("Notas", ("Con coctel", "Sin coctel", "Todos"), key="nota_s14")
        option_lugares = self.filter_manager.get_section_locations("s14", global_filters, multi=True)
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        if not temp_data.empty:
            conteo_pct, long_df = self.analytics.calculate_favor_contra_notes(temp_data, option_nota)
            
            if not conteo_pct.empty:
                if option_nota == "Con coctel":
                    titulo = "Porcentaje de notas que sean a favor, neutral y en contra con coctel"
                elif option_nota == "Sin coctel":
                    titulo = "Porcentaje de notas que sean a favor, neutral y en contra sin coctel"
                else:
                    titulo = "Porcentaje de notas que sean a favor, neutral y en contra"
                
                st.write(f"Análisis entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')}")
                
                # Mostrar tabla
                conteo_pct["A favor (%)"] = conteo_pct["a_favor_pct"].map("{:.1f}".format)
                conteo_pct["En contra (%)"] = conteo_pct["en_contra_pct"].map("{:.1f}".format)
                conteo_pct["Neutral (%)"] = conteo_pct["neutral_pct"].map("{:.1f}".format)
                
                st.dataframe(
                    conteo_pct[["año_mes", "A favor (%)", "Neutral (%)", "En contra (%)"]],
                    hide_index=True
                )
                
                # Mostrar gráfico
                fig = px.bar(
                    long_df,
                    x="año_mes",
                    y="Porcentaje",
                    color="Tipo de Nota",
                    barmode="stack",
                    title=titulo,
                    labels={"año_mes": "Año y Mes", "Porcentaje": "Porcentaje"},
                    text=long_df["Porcentaje"].map("{:.1f}%".format) if mostrar_todos else None,
                    color_discrete_map={
                        "a_favor_pct": "blue",
                        "en_contra_pct": "red",
                        "neutral_pct": "gray",
                    },
                )
                
                fig.update_layout(barmode='stack', xaxis={'categoryorder': 'category ascending'})
                fig.for_each_trace(lambda t: t.update(name=t.name.replace('_pct', ' (%)')))
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_15_proporcion_mensajes(self, global_filters: Dict[str, Any]):
        """15.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota"""
        st.subheader("15.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s15", global_filters)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s15")
        with col2:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s15"
            )
        with col3:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s15")
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            prop_data = self.analytics.calculate_message_proportion_by_position(temp_data, option_fuente, option_nota)
            
            if not prop_data.empty:
                if option_nota == 'Con coctel':
                    titulo = f"Proporción de mensajes emitidos con coctel por {option_fuente} en {option_lugar}"
                elif option_nota == 'Sin coctel':
                    titulo = f"Proporción de mensajes emitidos sin coctel por {option_fuente} en {option_lugar}"
                else:
                    titulo = f"Proporción de mensajes emitidos por {option_fuente} en {option_lugar}"
                
                fig = px.pie(
                    prop_data,
                    values='frecuencia',
                    names='id_posicion',
                    title=titulo,
                    color='id_posicion',
                    color_discrete_map=COLOR_POSICION_DICT,
                    hole=0.3
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(prop_data, hide_index=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_16_mensajes_por_tema(self, global_filters: Dict[str, Any]):
        """16.- Recuento de Mensajes Emitidos por Tema y Tipo de Nota"""
        st.subheader("16.- Recuento de Mensajes Emitidos por Tema y Tipo de Nota en Lugar y Fecha Específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s16", global_filters)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s16")
        with col2:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_temas['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s16"
            )
        with col3:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s16")
        
        temp_data = self.temp_coctel_temas[
            (self.temp_coctel_temas['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_temas['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_temas['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            topic_data = self.analytics.calculate_messages_by_topic(temp_data, option_fuente, option_nota, 10)
            
            if not topic_data.empty:
                # Sort the data by frequency in descending order (largest to smallest)
                topic_data_sorted = topic_data.groupby('descripcion')['frecuencia'].sum().reset_index()
                topic_data_sorted = topic_data_sorted.sort_values('frecuencia', ascending=False)
                
                # Merge back with original data to preserve all columns and maintain the order
                topic_data = topic_data.merge(
                    topic_data_sorted[['descripcion']], 
                    on='descripcion', 
                    how='inner'
                )
                # Set the category order for x-axis
                topic_data['descripcion'] = pd.Categorical(
                    topic_data['descripcion'], 
                    categories=topic_data_sorted['descripcion'].tolist(), 
                    ordered=True
                )
                topic_data = topic_data.sort_values('descripcion')
                
                if option_nota == 'Con coctel':
                    titulo = f"Recuento de mensajes emitidos con coctel por {option_fuente} en {option_lugar}"
                elif option_nota == 'Sin coctel':
                    titulo = f"Recuento de mensajes emitidos sin coctel por {option_fuente} en {option_lugar}"
                else:
                    titulo = f"Recuento de mensajes emitidos por {option_fuente} en {option_lugar}"
                
                fig = px.bar(
                    topic_data,
                    x='descripcion',
                    y='frecuencia',
                    title=titulo,
                    color='id_posicion',
                    text='frecuencia',
                    barmode='stack',
                    labels={'frecuencia': 'Frecuencia', 'descripcion': 'Tema', 'id_posicion': 'Posición'},
                    color_discrete_map=COLOR_POSICION_DICT,
                    height=500
                )
                
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_17_proporcion_por_tema(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """17.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota por Tema"""
        st.subheader("17.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específicos")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s17", global_filters)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s17")
        with col2:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_temas['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s17"
            )
        with col3:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s17")
        
        temp_data = self.temp_coctel_temas[
            (self.temp_coctel_temas['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_temas['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_temas['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            prop_data = self.analytics.calculate_topic_proportion(temp_data, option_fuente, option_nota, 10)
            
            if not prop_data.empty:
                if option_nota == 'Con coctel':
                    titulo = f"Proporción de mensajes emitidos con coctel por {option_fuente} en {option_lugar}"
                elif option_nota == 'Sin coctel':
                    titulo = f"Proporción de mensajes emitidos sin coctel por {option_fuente} en {option_lugar}"
                else:
                    titulo = f"Proporción de mensajes emitidos por {option_fuente} en {option_lugar}"
                
                fig = px.bar(
                    prop_data,
                    x="porcentaje",
                    y="descripcion",
                    title=titulo,
                    orientation='h',
                    text=prop_data["porcentaje"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                    labels={'porcentaje': 'Porcentaje %', 'descripcion': 'Temas'}
                )
                
                fig.update_traces(textposition="outside" if mostrar_todos else "none")
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_18_tendencia_por_medio(self, global_filters: Dict[str, Any]):
        """18.- Tendencia de las notas emitidas en lugar y fecha específica por fuente y tipo de nota"""
        st.subheader("18.- Tendencia de las notas emitidas en lugar y fecha específica por fuente y tipo de nota")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s18", global_filters)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_s18")
        with col2:
            # Local location selector - independent of global filters
            # Get available locations from both dataframes
            available_locations_programas = self.temp_coctel_fuente_programas['lugar'].dropna().unique()
            available_locations_fb = self.temp_coctel_fuente_fb['lugar'].dropna().unique()
            all_locations = sorted(set(list(available_locations_programas) + list(available_locations_fb)))
            
            option_lugar = st.selectbox(
                "Lugar", 
                options=all_locations, 
                key="lugar_s18"
            )
        with col3:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s18")
        
        temp_programas = self.temp_coctel_fuente_programas[
            (self.temp_coctel_fuente_programas['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente_programas['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente_programas['lugar'] == option_lugar)
        ]
        
        temp_fb = self.temp_coctel_fuente_fb[
            (self.temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente_fb['lugar'] == option_lugar)
        ]
        
        trend_data = self.analytics.calculate_notes_trend_by_medium(temp_programas, temp_fb, option_fuente, option_nota)
        
        if not trend_data.empty:
            if option_nota == 'Con coctel':
                titulo = f"Tendencia de las notas emitidas con coctel por {option_fuente} en {option_lugar}"
            elif option_nota == 'Sin coctel':
                titulo = f"Tendencia de las notas emitidas sin coctel por {option_fuente} en {option_lugar}"
            else:
                titulo = f"Tendencia de las notas emitidas por {option_fuente} en {option_lugar}"
            
            fig = px.bar(
                trend_data,
                x='medio_nombre',
                y='frecuencia',
                color='id_posicion',
                title=titulo,
                barmode='stack',
                color_discrete_map=COLOR_POSICION_DICT,
                labels={'frecuencia': 'Frecuencia', 'medio_nombre': 'Canal/Medio', 'id_posicion': 'Posición'},
                text='frecuencia',
            )
            
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos para mostrar")
    
    def section_19_notas_tiempo_posicion(self, global_filters: Dict[str, Any]):
        """19.- Notas emitidas en un rango de tiempo segun posicion y coctel"""
        st.subheader("19.- Notas emitidas en un rango de tiempo segun posicion y coctel")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s19", global_filters)
        option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s19")
        
        temp_data = self.temp_coctel_temas[
            (self.temp_coctel_temas['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_temas['fecha_registro'] <= fecha_fin)
        ]
        
        if not temp_data.empty:
            time_data = self.analytics.calculate_notes_by_time_position(temp_data, option_nota)
            
            if not time_data.empty:
                if option_nota == 'Con coctel':
                    titulo = f"Notas emitidas con coctel entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')} según posición"
                elif option_nota == 'Sin coctel':
                    titulo = f"Notas emitidas sin coctel entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')} según posición"
                else:
                    titulo = f"Notas emitidas entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')} según posición"
                
                fig = px.bar(
                    time_data,
                    x='id_posicion',
                    y='frecuencia',
                    title=titulo,
                    labels={'frecuencia': 'Frecuencia', 'id_posicion': 'Posición'},
                    color='id_posicion',
                    color_discrete_map=COLOR_POSICION_DICT,
                    text='frecuencia'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_20_actores_posiciones(self, global_filters: Dict[str, Any]):
        """20.- Recuento de posiciones emitidas por actor en lugar y fecha específica"""
        st.subheader("20.- Recuento de posiciones emitidas por actor en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s20", global_filters)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s20")
        with col2:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_fuente_actores['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s20"
            )
        with col3:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s20")
        
        temp_data = self.temp_coctel_fuente_actores[
            (self.temp_coctel_fuente_actores['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente_actores['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente_actores['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            actor_data = self.analytics.calculate_actor_positions(temp_data, option_fuente, option_nota, 10)
            
            if not actor_data.empty:
                if option_nota == 'Con coctel':
                    titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}. Notas con coctel"
                elif option_nota == 'Sin coctel':
                    titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}. Notas sin coctel"
                else:
                    titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}"
                
                fig = px.bar(
                    actor_data,
                    x='nombre',
                    y='frecuencia',
                    title=titulo,
                    color='posicion',
                    barmode='stack',
                    color_discrete_map=COLOR_POSICION_DICT,
                    labels={'frecuencia': 'Frecuencia', 'nombre': 'Actor', 'posicion': 'Posición'},
                    text='frecuencia'
                )
                
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_21_porcentaje_medios(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """21.- Porcentaje de cóctel de todos los medios"""
        st.subheader("21.- Porcentaje de cóctel de todos los medios")
        
        # Para esta sección usamos selectores de año/mes
        ano_actual = datetime.now().year
        anos = list(range(ano_actual - 9, ano_actual + 1))
        
        col1, col2 = st.columns(2)
        with col1:
            ano_inicio = st.selectbox("Año de inicio", anos, len(anos)-1, key="ano_inicio_s21")
            mes_inicio = st.selectbox("Mes de inicio", MESES_ES, index=11, key="mes_inicio_s21")
        with col2:
            ano_fin = st.selectbox("Año de fin", anos, index=len(anos)-1, key="ano_fin_s21")
            mes_fin = st.selectbox("Mes de fin", MESES_ES, index=11, key="mes_fin_s21")
        
        option_regiones = self.filter_manager.get_section_locations("s21", global_filters, multi=True)
        
        year_month_start = f"{ano_inicio}-{MESES_ES.index(mes_inicio) + 1:02d}-01"
        year_month_end = f"{ano_fin}-{MESES_ES.index(mes_fin) + 1:02d}-01"
        
        if not self.temp_coctel_fuente.empty:
            result = self.analytics.calculate_coctel_percentage_by_media(
                self.temp_coctel_fuente, year_month_start, year_month_end
            )
            
            if result.empty:
                st.warning("No hay datos para mostrar en el período seleccionado")
                return
            
            # Check if 'lugar' column exists before filtering
            if 'lugar' not in result.columns:
                st.error(f"❌ Column 'lugar' not found in result. Available columns: {result.columns.tolist()}")
                return
            
            # Check if we have any matching regions
            available_places = result['lugar'].unique()
            matching_regions = [region for region in option_regiones if region in available_places]
            
            if not matching_regions:
                st.warning(f"⚠️ No hay datos para las regiones seleccionadas: {option_regiones}")
                st.info(f"💡 Regiones disponibles: {', '.join(available_places)}")
                return
            
            # Filtrar por regiones seleccionadas
            result = result[result['lugar'].isin(option_regiones)]
            
            if not result.empty:
                promedios = result.groupby('Fuente')['porcentaje_coctel'].mean().to_dict()
                
                fig = px.bar(
                    result,
                    x="lugar",
                    y="porcentaje_coctel",
                    color="Fuente",
                    barmode="group",
                    title=f"Porcentaje de cóctel de todos los medios - {mes_inicio} {ano_inicio} hasta {mes_fin} {ano_fin}",
                    labels={"lugar": "Regiones", "porcentaje_coctel": "Porcentaje de Cóctel"},
                    text=result["porcentaje_coctel"].map("{:.1f}%".format) if mostrar_todos else None,
                    color_discrete_map={"Radio": "blue", "Redes": "red", "TV": "gray"},
                )
                
                fig.update_layout(font=dict(size=8))
                fig.update_traces(textposition="outside" if mostrar_todos else "none")
                
                # Agregar líneas de promedio por fuente
                for fuente, promedio in promedios.items():
                    fig.add_hline(
                        y=promedio,
                        line_dash="dash",
                        annotation_text=f"Promedio de {fuente}: {promedio:.2f}%",
                        annotation_position="right",
                        line_color={"Radio": "blue", "Redes": "orange", "TV": "gray"}[fuente],
                    )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para las regiones seleccionadas después del filtrado")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_22_ultimos_3_meses(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """22.- Porcentaje de cóctel en los últimos 3 meses por fuente"""
        st.subheader("22.- Porcentaje de cóctel en los últimos 3 meses por fuente")
        
        ano_actual = datetime.now().year
        anos = list(range(ano_actual - 9, ano_actual + 1))
        
        col1, col2 = st.columns(2)
        with col1:
            ano_fin = st.selectbox("Año de referencia", anos, index=len(anos)-1, key="ano_fin_s22")
            mes_fin = st.selectbox("Mes de referencia", MESES_ES, index=11, key="mes_fin_s22")
        with col2:
            fuente = st.selectbox("Fuente", ['Radio', 'Redes', 'TV'], key="fuente_s22")
        
        option_regiones = self.filter_manager.get_section_locations("s22", global_filters, multi=True)
        
        end_date = f"{ano_fin}-{MESES_ES.index(mes_fin) + 1:02d}-01"
        
        if not self.temp_coctel_fuente.empty:
            result = self.analytics.calculate_last_3_months_coctel(
                self.temp_coctel_fuente, end_date, fuente
            )
            
            if result.empty:
                st.warning("No hay datos para mostrar en el período seleccionado")
                return
            
            # Check if 'lugar' column exists before filtering
            if 'lugar' not in result.columns:
                st.error(f"❌ Column 'lugar' not found in result. Available columns: {result.columns.tolist()}")
                return
            
            # Filtrar por regiones seleccionadas
            result = result[result['lugar'].isin(option_regiones)]
            
            if result.empty:
                st.warning("No hay datos para mostrar en las regiones seleccionadas")
                return
            
            # Check if 'mes' column exists
            if 'mes' not in result.columns:
                st.error(f"❌ Column 'mes' not found in result. Available columns: {result.columns.tolist()}")
                return
            
            unique_months = result['mes'].unique()
            
            # Check if we have any months data
            if len(unique_months) == 0:
                st.warning("No hay datos de meses disponibles para la selección actual.")
                return
            
            if len(unique_months) >= 3:
                color_mapping = {
                    unique_months[-3]: "lightblue",
                    unique_months[-2]: "cornflowerblue",
                    unique_months[-1]: "navy"
                }
            elif len(unique_months) == 2:
                color_mapping = {
                    unique_months[-2]: "lightblue",
                    unique_months[-1]: "navy"
                }
            elif len(unique_months) == 1:
                color_mapping = {unique_months[-1]: "navy"}
            
            fig = px.bar(
                result,
                x="lugar",
                y="porcentaje_coctel",
                color="mes",
                barmode="group",
                title=f"Porcentaje de cóctel {fuente} - Últimos 3 meses",
                labels={"lugar": "Región", "porcentaje_coctel": "Porcentaje de Cóctel", "mes": "Mes"},
                color_discrete_map=color_mapping,
                text=result["porcentaje_coctel"].map("{:.1f}%".format) if mostrar_todos else None
            )
            
            fig.update_layout(font=dict(size=15))
            fig.update_traces(textposition="outside" if mostrar_todos else "none")
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos para mostrar")
    
    def section_23_evolucion_mensual(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """23.- Gráfico Mensual Lineal sobre la evolución de Radio, Redes y TV"""
        st.subheader("23.- Gráfico Mensual Lineal sobre la evolución de Radio, Redes y TV")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s23", global_filters)
        option_lugares = self.filter_manager.get_section_locations("s23", global_filters, multi=True)
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        if not temp_data.empty:
            by_source, combined_data = self.analytics.calculate_monthly_evolution(temp_data)
            
            if not combined_data.empty:
                fig = px.line(
                    combined_data,
                    x='fecha_mes',
                    y='coctel',
                    color='Fuente',
                    markers=True,
                    color_discrete_map={'Radio': 'gray', 'Redes': 'red', 'TV': 'blue', 'Total': 'green'},
                    title="Gráfico Mensual Lineal sobre la evolución de Radio, Redes y TV",
                    text=combined_data["coctel"].map(str) if mostrar_todos else None
                )
                
                fig.update_traces(textposition="top center")
                fig.update_layout(
                    xaxis_title="Mes y Año",
                    yaxis_title="Impactos de Coctel",
                    xaxis=dict(tickangle=45, showgrid=False),
                    yaxis=dict(showgrid=True),
                    font=dict(size=12),
                    margin=dict(l=50, r=50, t=50, b=50)
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_24_mensajes_fuerza(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """24.- Porcentaje de cocteles por mensajes fuerza"""
        st.subheader("24.- Porcentaje de cocteles por mensajes fuerza")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s24", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s24")
        with col2:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s24")
        
        temp_data = self.temp_coctel_completo[
            (self.temp_coctel_completo['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_completo['fecha_registro'] <= fecha_fin)
        ]
        
        if not temp_data.empty:
            message_data = self.analytics.calculate_coctel_by_message_force(temp_data, option_fuente, option_nota)
            
            if not message_data.empty:
                if option_nota == "Con coctel":
                    titulo = "Porcentaje de notas por mensaje de fuerza con coctel"
                elif option_nota == "Sin coctel":
                    titulo = "Porcentaje de notas por mensaje de fuerza sin coctel"
                else:
                    titulo = "Porcentaje de notas por mensaje de fuerza"
                
                fig = px.bar(
                    message_data,
                    x='coctel',
                    y='mensaje_fuerza',
                    orientation='h',
                    text=message_data.apply(lambda row: f"{row['coctel']} ({row['porcentaje']:.1f}%)", axis=1) if mostrar_todos else None,
                    labels={'coctel': '', 'mensaje_fuerza': ''},
                    title=titulo,
                    color_discrete_sequence=['red']
                )
                
                fig.update_layout(font=dict(size=8))
                fig.update_traces(textposition="outside" if mostrar_todos else "none")
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_25_impactos_programa(self, global_filters: Dict[str, Any]):
        """25.- Impactos por programa"""
        st.subheader("25.- Impactos por programa")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s32", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            medio = st.selectbox("Medio", ("Radio", "TV", "Redes"), key="medio_s32")
        with col2:
            # Local location selector - independent of global filters
            # Get available locations from both dataframes
            available_locations_programas = self.temp_coctel_fuente_programas['lugar'].dropna().unique()
            available_locations_fb = self.temp_coctel_fuente_fb['lugar'].dropna().unique()
            all_locations = sorted(set(list(available_locations_programas) + list(available_locations_fb)))
            
            region = st.selectbox(
                "Lugar", 
                options=all_locations, 
                key="lugar_s32"
            )
        
        if medio in ("Radio", "TV"):
            temp_data = self.temp_coctel_fuente_programas[
                (self.temp_coctel_fuente_programas["fecha_registro"] >= fecha_inicio) &
                (self.temp_coctel_fuente_programas["fecha_registro"] <= fecha_fin) &
                (self.temp_coctel_fuente_programas["lugar"] == region)
            ]
        else:
            temp_data = self.temp_coctel_fuente_fb[
                (self.temp_coctel_fuente_fb["fecha_registro"] >= fecha_inicio) &
                (self.temp_coctel_fuente_fb["fecha_registro"] <= fecha_fin) &
                (self.temp_coctel_fuente_fb["lugar"] == region)
            ]
        
        if not temp_data.empty:
            result = self.analytics.calculate_program_impacts(temp_data, medio)
            
            if not result.empty:
                st.write("Total de impactos por programa")
                
                # Renombrar columnas para mejor presentación
                if medio in ("Radio", "TV"):
                    column_mapping = {"nombre_canal": "Canal", "programa_nombre": "Programa"}
                else:
                    column_mapping = {"nombre_facebook_page": "Página Facebook"}
                
                st.dataframe(result.rename(columns=column_mapping), hide_index=True)
            else:
                st.warning("No hay datos para la selección actual.")
        else:
            st.warning("No hay datos para la selección actual.")
    
    def section_26_distribucion_medio(self, global_filters: Dict[str, Any]):
        """26.- Distribución de cócteles por medio"""
        st.subheader("26.- Distribución de cócteles por medio")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s33", global_filters)
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente["fecha_registro"] >= fecha_inicio) &
            (self.temp_coctel_fuente["fecha_registro"] <= fecha_fin)
        ]
        
        if not temp_data.empty:
            conteo_data = self.analytics.calculate_coctel_distribution_by_media(temp_data)
            
            if not conteo_data.empty:
                fig = px.pie(
                    conteo_data,
                    values="count",
                    names="Fuente",
                    color="Fuente",
                    color_discrete_map={
                        "Radio": "blue",
                        "Redes": "red",
                        "TV": "gray"
                    },
                    title="Distribución de cócteles por medio"
                )
                
                fig.update_traces(
                    textposition="inside",
                    textinfo="value+percent"
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para la selección actual.")
        else:
            st.warning("No hay datos para la selección actual.")
    def section_27_favor_contra_mensual(self, global_filters: Dict[str, Any]):
        """27.- Notas a favor vs en contra"""
        st.subheader("27.- Notas a favor vs en contra")

        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s34", global_filters)

        col1, col2 = st.columns(2)
        with col1:
            medio = st.selectbox("Medio", ("Radio", "TV", "Redes", "Todos"), key="medio_s34")
        with col2:
            regiones = self.filter_manager.get_section_locations("s34", global_filters, multi=True)

        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente["fecha_registro"] >= fecha_inicio) &
            (self.temp_coctel_fuente["fecha_registro"] <= fecha_fin) &
            (self.temp_coctel_fuente["lugar"].isin(regiones))
        ]

        if not temp_data.empty:
            long_data = self.analytics.calculate_favor_vs_contra_monthly(temp_data, medio)

            if not long_data.empty:
                fig = px.line(
                    long_data,
                    x="mes_str",
                    y="Porcentaje",
                    color="Tipo",
                    markers=True,
                    color_discrete_map={
                        "A favor (%)": "blue",
                        "En contra (%)": "red"
                    },
                    labels={"mes_str": "Mes", "Porcentaje": "% sobre total", "Tipo": ""},
                    title="Notas a favor vs en contra por mes"
                )

                fig.update_traces(
                    text=long_data["Porcentaje"].round(0).astype(int).astype(str) + "%",
                    textposition="top center"
                )
                fig.update_layout(xaxis_tickangle=45)

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para la selección actual.")
        else:
            st.warning("No hay datos para la selección actual.")
        
    def section_1_proporcion_combinada(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """1.- Proporción de cocteles en lugar, fuentes y fechas específicas"""
        st.subheader("1.- Proporción de cocteles en lugar, fuentes y fechas específicas")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s1", global_filters)
        
        option_fuente = st.multiselect(
            "Fuente", ["Radio", "TV", "Redes"], 
            ["Radio", "TV", "Redes"], key="fuente_s1"
        )
            
        option_lugares = self.filter_manager.get_section_locations("s1", global_filters, multi=True)
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        st.write(f"Proporción de cocteles en {', '.join(option_lugares)} entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
        
        if not temp_data.empty:
            result = self.analytics.calculate_coctel_proportion_combined(temp_data, option_fuente, option_lugares)
            if not result.empty:
                st.dataframe(result, hide_index=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_2_posicion_por_fuente(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """2.- Posición por fuente en lugar y fecha específica"""
        st.subheader("2.- Posición por fuente en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s2", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            option_coctel = st.selectbox(
                "Notas", ("Coctel noticias", "Otras fuentes", "Todas"), key="coctel_s2"
            )
        with col2:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s2"
            )
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'] == option_lugar)
        ]
        
        st.write(f"Posición por {option_coctel} en {option_lugar} entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
        
        if not temp_data.empty:
            result = self.analytics.calculate_position_by_source(temp_data, option_coctel)
            if not result.empty:
                st.dataframe(result, hide_index=True)
            else:
                st.warning("No hay datos para mostrar")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_3_tendencia_semanal(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """3.- Gráfico semanal por porcentaje de cocteles"""
        st.subheader("3.- Gráfico semanal por porcentaje de cocteles en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s3", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s3")
        with col2:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s3"
            )
        
        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s3")
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            weekly_data = self.analytics.calculate_weekly_percentage(temp_data, option_fuente)
            
            if not weekly_data.empty:
                if usar_fechas_viernes:
                    weekly_data["eje_x"] = weekly_data["viernes"].dt.strftime("%Y-%m-%d")
                else:
                    weekly_data["eje_x"] = weekly_data["fecha_registro"].dt.strftime("%Y-%m") + "-S" + (
                        (weekly_data["fecha_registro"].dt.day - 1) // 7 + 1
                    ).astype(str)
                
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=weekly_data["eje_x"],
                        y=weekly_data["porcentaje"],
                        mode="lines+markers+text" if mostrar_todos else "lines+markers",
                        text=weekly_data["porcentaje"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                        textposition="top center",
                        name="Porcentaje Coctel"
                    )
                )
                
                fig.update_xaxes(
                    title_text="Fecha (Viernes)" if usar_fechas_viernes else "Semana",
                    tickangle=45
                )
                fig.update_yaxes(title_text="Porcentaje de cocteles %")
                fig.update_layout(title=f"Tendencia semanal - {option_fuente} en {option_lugar}")
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos suficientes para la tendencia semanal")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_4_favor_vs_contra(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """4.- Gráfico semanal de noticias a favor y en contra"""
        st.subheader("4.- Gráfico semanal de noticias a favor y en contra en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s4", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_fuente_notas['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s4"
            )
        with col2:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s4")
        
        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s4")
        
        temp_data = self.temp_coctel_fuente_notas[
            (self.temp_coctel_fuente_notas['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente_notas['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente_notas['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            weekly_data = self.analytics.calculate_weekly_favor_contra(temp_data, option_fuente)
            
            if not weekly_data.empty:
                if usar_fechas_viernes:
                    weekly_data["eje_x"] = weekly_data["viernes"].dt.strftime("%d-%m-%Y")
                else:
                    weekly_data["eje_x"] = weekly_data["fecha_registro"].dt.strftime("%Y-%m") + "-S" + (
                        (weekly_data["fecha_registro"].dt.day - 1) // 7 + 1
                    ).astype(str)
                
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=weekly_data["eje_x"],
                        y=weekly_data["a_favor"],
                        mode="lines+markers+text" if mostrar_todos else "lines+markers",
                        name="A favor",
                        text=weekly_data["a_favor"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                        textposition="top center",
                        line=dict(color="blue"),
                    )
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=weekly_data["eje_x"],
                        y=weekly_data["en_contra"],
                        mode="lines+markers+text" if mostrar_todos else "lines+markers",
                        name="En contra",
                        text=weekly_data["en_contra"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                        textposition="top center",
                        line=dict(color="red"),
                    )
                )
                
                fig.update_xaxes(
                    title_text="Fecha (Viernes)" if usar_fechas_viernes else "Semana",
                    tickangle=45
                )
                fig.update_yaxes(title_text="Porcentaje de noticias %")
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos suficientes")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_5_grafico_acumulativo(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """5.- Gráfico acumulativo porcentaje de cocteles"""
        st.subheader("5.- Gráfico acumulativo porcentaje de cocteles en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s5", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s5")
        with col2:
            option_lugares = self.filter_manager.get_section_locations("s5", global_filters, multi=True)
        
        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s5")
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'].isin(option_lugares))
        ]
        
        if not temp_data.empty:
            cumulative_data = self.analytics.calculate_cumulative_percentage(temp_data, option_fuente)
            
            if not cumulative_data.empty:
                if usar_fechas_viernes:
                    cumulative_data["eje_x"] = cumulative_data["viernes"].dt.strftime("%d-%m-%Y")
                else:
                    cumulative_data["eje_x"] = cumulative_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                        (cumulative_data["viernes"].dt.day - 1) // 7 + 1
                    ).astype(str)
                
                fig = px.line(
                    cumulative_data,
                    x="eje_x",
                    y="coctel_mean",
                    color="lugar",
                    title="Porcentaje de cocteles por semana %",
                    labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", "coctel_mean": "Porcentaje de cocteles %"},
                    markers=True,
                    text=cumulative_data["coctel_mean"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                )
                
                fig.update_traces(textposition="top center")
                fig.update_xaxes(tickangle=45)
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Mostrar tabla resumen
                st.write(f"Porcentaje de cocteles por lugar en la última semana")
                last_week = cumulative_data.sort_values("semana").groupby("lugar").last().reset_index()
                last_week['coctel_mean'] = last_week['coctel_mean'].map(lambda x: f"{x:.1f}")
                last_week = last_week[["lugar", "coctel_mean"]].rename(columns={"coctel_mean": "pct_cocteles"})
                st.dataframe(last_week, hide_index=True)
            else:
                st.warning("No hay datos suficientes")
        else:
            st.warning("No hay datos para mostrar")
    
    def section_top3_mejores_lugares(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """Top 3 mejores porcentajes de coctel semanal por lugar"""
        st.subheader("Top 3 mejores porcentajes de coctel semanal por lugar en fuente y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("stop3", global_filters)
        
        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_stop3")
        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_stop3")
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin)
        ]
        
        if not temp_data.empty:
            top_data, top_lugares_list = self.analytics.calculate_top_lugares(temp_data, option_fuente, 3)
            
            if not top_data.empty:
                if usar_fechas_viernes:
                    top_data["eje_x"] = top_data["viernes"].dt.strftime("%d-%m-%Y")
                else:
                    top_data["eje_x"] = top_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                        (top_data["viernes"].dt.day - 1) // 7 + 1
                    ).astype(str)
                
                fig = px.line(
                    top_data,
                    x="eje_x",
                    y="coctel",
                    color="lugar",
                    title="Top 3 lugares con mayor porcentaje de cocteles",
                    labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", "coctel": "Porcentaje de cocteles %"},
                    markers=True,
                    text=top_data["coctel"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                )
                
                fig.update_traces(textposition="top center")
                fig.update_xaxes(tickangle=45)
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Mostrar tabla de top lugares
                st.write(f"Top 3 lugares con mayor porcentaje de cocteles según {option_fuente}")
                top_summary = top_data.sort_values("semana").groupby("lugar").last().reset_index()
                top_summary['coctel'] = top_summary['coctel'].map(lambda x: f"{x:.1f}")
                st.dataframe(top_summary[["lugar", "coctel"]], hide_index=True)
            else:
                st.warning("No hay datos suficientes")
        else:
            st.warning("No hay datos para mostrar")
        
    def section_6_top_medios(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """6.- Top 3 mejores radios, redes, tv"""
        st.subheader("6.- Top 3 mejores radios, redes, tv en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s6", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_s6")
        with col2:
            # Local location selector - independent of global filters
            # Get available locations from both dataframes
            available_locations_programas = self.temp_coctel_fuente_programas['lugar'].dropna().unique()
            available_locations_fb = self.temp_coctel_fuente_fb['lugar'].dropna().unique()
            all_locations = sorted(set(list(available_locations_programas) + list(available_locations_fb)))
            
            option_lugar = st.selectbox(
                "Lugar", 
                options=all_locations, 
                key="lugar_s6"
            )
        
        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s6")
        
        temp_programas = self.temp_coctel_fuente_programas[
            (self.temp_coctel_fuente_programas['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente_programas['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente_programas['lugar'] == option_lugar)
        ]
        
        temp_fb = self.temp_coctel_fuente_fb[
            (self.temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente_fb['lugar'] == option_lugar)
        ]
        
        top_data = self.analytics.calculate_top_medios(temp_programas, temp_fb, option_fuente, 3)
        
        if not top_data.empty:
            if usar_fechas_viernes:
                top_data["eje_x"] = top_data["viernes"].dt.strftime("%d-%m-%Y")
            else:
                top_data["eje_x"] = top_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                    (top_data["viernes"].dt.day - 1) // 7 + 1
                ).astype(str)
            
            fig = px.line(
                top_data,
                x="eje_x",
                y="coctel",
                color="nombre_medio",
                title=f"Top 3 {option_fuente.lower()} con mayor porcentaje de cocteles",
                labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", "coctel": "Porcentaje de cocteles %"},
                markers=True,
                text=top_data["coctel"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
            )
            
            fig.update_traces(textposition="top center")
            fig.update_xaxes(tickangle=45)
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos para mostrar")
    
    def section_7_macroregion(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """7.- Crecimiento de cocteles por macroregión"""
        st.subheader("7.- Crecimiento de cocteles por macroregión en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s7", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_s7")
        with col2:
            if option_fuente in ["Radio", "Redes"]:
                option_macroregion = st.selectbox("Macroregión", MACROREGIONES_RADIO_REDES, key="macro_s7")
            else:
                option_macroregion = st.selectbox("Macroregión", MACROREGIONES_TV, key="macro_s7")
        
        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s7")
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin)
        ]
        
        if not temp_data.empty:
            macro_data = self.analytics.calculate_macroregion_growth(temp_data, option_fuente, option_macroregion)
            
            if not macro_data.empty:
                if usar_fechas_viernes:
                    macro_data["eje_x"] = macro_data["viernes"].dt.strftime("%d-%m-%Y")
                else:
                    macro_data["eje_x"] = macro_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                        (macro_data["viernes"].dt.day - 1) // 7 + 1
                    ).astype(str)
                
                fig = px.line(
                    macro_data,
                    x="eje_x",
                    y="coctel_mean",
                    color="lugar",
                    title=f"Crecimiento de cocteles por macroregión en {option_macroregion}",
                    labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", "coctel_mean": "Porcentaje de cocteles %"},
                    markers=True,
                    text=macro_data["coctel_mean"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                )
                
                fig.update_traces(textposition="top center")
                fig.update_xaxes(tickangle=45)
                
                st.plotly_chart(fig, use_container_width=True)
                st.write("Nota: Los valores muestran el porcentaje de cocteles en cada semana")
            else:
                st.warning("No hay datos suficientes")
        else:
            st.warning("No hay datos para mostrar")