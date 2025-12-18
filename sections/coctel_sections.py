# sections/coctel_sections.py

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
from sections.functions.sn import data_section_sn_proporcion_simple_sql


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

        # SECCIÓN 28 - Registros creados por usuarios
        self.section_28_registros_usuarios()
        
        
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
  
      # Obtener datos de la base de datos usando la nueva función
      try:
          resultado_radio, resultado_tv, resultado_redes_sociales = data_section_sn_proporcion_simple_sql(
              fecha_inicio, fecha_fin, option_lugar
          )
          
          # Verificar si se obtuvieron datos válidos
          datos_disponibles = (
              not resultado_radio.empty or 
              not resultado_tv.empty or 
              not resultado_redes_sociales.empty
          )
          
          if datos_disponibles:
              st.write(f"Proporción de cocteles en {option_lugar} entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
  
              col1, col2, col3 = st.columns(3)
              
              with col1:
                  st.write("Radio")
                  if not resultado_radio.empty:
                      # Convertir la estructura para que sea más legible
                      display_radio = resultado_radio.copy()
                      display_radio['tipo_coctel'] = display_radio['tipo_coctel'].replace({
                          'CON_COCTEL': 'Coctel noticias',
                          'SIN_COCTEL': 'Otras fuentes'
                      })
                      display_radio = display_radio.rename(columns={
                          'tipo_coctel': 'Fuente',
                          'cantidad': 'Total',
                          'porcentaje': 'Proporción (%)'
                      })
                      # Formatear porcentaje
                      display_radio['Proporción (%)'] = display_radio['Proporción (%)'].apply(lambda x: f"{x:.1f}%")
                      st.dataframe(display_radio[['Fuente', 'Total', 'Proporción (%)']], hide_index=True)
                  else:
                      st.write("Sin datos")
  
              with col2:
                  st.write("TV")
                  if not resultado_tv.empty:
                      # Convertir la estructura para que sea más legible
                      display_tv = resultado_tv.copy()
                      display_tv['tipo_coctel'] = display_tv['tipo_coctel'].replace({
                          'CON_COCTEL': 'Coctel noticias',
                          'SIN_COCTEL': 'Otras fuentes'
                      })
                      display_tv = display_tv.rename(columns={
                          'tipo_coctel': 'Fuente',
                          'cantidad': 'Total',
                          'porcentaje': 'Proporción (%)'
                      })
                      # Formatear porcentaje
                      display_tv['Proporción (%)'] = display_tv['Proporción (%)'].apply(lambda x: f"{x:.1f}%")
                      st.dataframe(display_tv[['Fuente', 'Total', 'Proporción (%)']], hide_index=True)
                  else:
                      st.write("Sin datos")
  
              with col3:
                  st.write("Redes")
                  if not resultado_redes_sociales.empty:
                      # Convertir la estructura para que sea más legible
                      display_redes = resultado_redes_sociales.copy()
                      display_redes['tipo_coctel'] = display_redes['tipo_coctel'].replace({
                          'CON_COCTEL': 'Coctel noticias',
                          'SIN_COCTEL': 'Otras fuentes'
                      })
                      display_redes = display_redes.rename(columns={
                          'tipo_coctel': 'Fuente',
                          'cantidad': 'Total',
                          'porcentaje': 'Proporción (%)'
                      })
                      # Formatear porcentaje
                      display_redes['Proporción (%)'] = display_redes['Proporción (%)'].apply(lambda x: f"{x:.1f}%")
                      st.dataframe(display_redes[['Fuente', 'Total', 'Proporción (%)']], hide_index=True)
                  else:
                      st.write("Sin datos")
          else:
              st.warning("No hay datos para mostrar")
              
      except Exception as e:
          st.error(f"Error al obtener los datos: {e}")
          print(f"Error en section_sn_proporcion_basica: {e}")
          
          # Fallback al código anterior si hay problemas con la nueva función
          st.info("Intentando con método alternativo...")
          
          # Código de respaldo (tu lógica anterior)
          temp_data = self.temp_coctel_fuente[
              (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
              (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
              (self.temp_coctel_fuente['lugar'] == option_lugar)
          ]
  
          fb_data = self.temp_coctel_fuente_fb[
              (self.temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio) &
              (self.temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin) &
              (self.temp_coctel_fuente_fb['lugar'] == option_lugar)
          ]
  
          # Combine data for Radio/TV (keep existing logic)
          if not fb_data.empty:
              common_cols = list(set(temp_data.columns) & set(fb_data.columns))
              if common_cols:
                  combined_data = pd.concat([temp_data[common_cols], fb_data[common_cols]], ignore_index=True)
              else:
                  combined_data = temp_data.copy()
                  st.warning("⚠️ no common columns between data sources, using main source only")
          else:
              combined_data = temp_data.copy()
  
          if not combined_data.empty or not fb_data.empty:
              # Calculate results for Radio/TV using combined data
              result_radio_tv = self.analytics.calculate_coctel_proportion(combined_data) if not combined_data.empty else {}
              
              # For Redes, use S25 logic to get disaggregated data
              redes_temp_data = self.temp_coctel_fuente_fb[
                  (self.temp_coctel_fuente_fb["fecha_registro"] >= fecha_inicio) &
                  (self.temp_coctel_fuente_fb["fecha_registro"] <= fecha_fin) &
                  (self.temp_coctel_fuente_fb["lugar"] == option_lugar)
              ]
              
              # Filter and dedupe like S25
              redes_temp_data = redes_temp_data[['id', 'fecha_registro', 'lugar', 'coctel', 'nombre_facebook_page']].drop_duplicates()
              redes_temp_data = redes_temp_data[
                  redes_temp_data['nombre_facebook_page'].notna() &
                  (redes_temp_data['nombre_facebook_page'] != '')
              ]
              
              # Get S25-style results
              if not redes_temp_data.empty:
                  result_coctel_s25, result_total_s25 = self.analytics.calculate_program_impacts_complete(redes_temp_data, "Redes")
                  
                  # Sum the totals from S25 results
                  coctel_total = 0
                  total_total = 0
                  
                  if not result_coctel_s25.empty:
                      numeric_cols_coctel = result_coctel_s25.select_dtypes(include=['number']).columns
                      if len(numeric_cols_coctel) > 0:
                          coctel_total = result_coctel_s25[numeric_cols_coctel].sum().sum()
                  
                  if not result_total_s25.empty:
                      numeric_cols_total = result_total_s25.select_dtypes(include=['number']).columns
                      if len(numeric_cols_total) > 0:
                          total_total = result_total_s25[numeric_cols_total].sum().sum()
                  
                  # Calculate otras fuentes
                  otras_fuentes = total_total - coctel_total
                  
                  # Create proportion dataframe
                  if total_total > 0:
                      coctel_prop = (coctel_total / total_total) * 100
                      otras_prop = (otras_fuentes / total_total) * 100
                      
                      redes_proportion_df = pd.DataFrame({
                          'Fuente': ['Coctel noticias', 'Otras fuentes'],
                          'Total': [coctel_total, otras_fuentes],
                          'Proporción (%)': [f"{coctel_prop:.1f}%", f"{otras_prop:.1f}%"]
                      })
                  else:
                      redes_proportion_df = pd.DataFrame()
              else:
                  redes_proportion_df = pd.DataFrame()
  
              st.write(f"Proporción de cocteles en {option_lugar} entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
  
              col1, col2, col3 = st.columns(3)
              with col1:
                  st.write("Radio")
                  if "Radio" in result_radio_tv and not result_radio_tv["Radio"].empty:
                      st.dataframe(result_radio_tv["Radio"], hide_index=True)
                  else:
                      st.write("Sin datos")
  
              with col2:
                  st.write("TV")
                  if "TV" in result_radio_tv and not result_radio_tv["TV"].empty:
                      st.dataframe(result_radio_tv["TV"], hide_index=True)
                  else:
                      st.write("Sin datos")
  
              with col3:
                  st.write("Redes")
                  if not redes_proportion_df.empty:
                      st.dataframe(redes_proportion_df, hide_index=True)
                  else:
                      st.write("Sin datos")
          else:
              st.warning("No hay datos para mostrar")
    
    def section_1_proporcion_combinada(self, global_filters: Dict[str, Any], mostrar_todos: bool):
       from sections.functions.grafico1 import data_section_1_proporcion_combinada_sql, convertir_a_formato_streamlit
       
       """1.- Proporción de cocteles en lugar, fuentes y fechas específicas"""
       st.subheader("1.- Proporción de cocteles en lugar, fuentes y fechas específicas")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s1", global_filters)
       
       option_fuente = st.multiselect(
           "Fuente", ["Radio", "TV", "Redes"], 
           ["Radio", "TV", "Redes"], key="fuente_s1"
       )
           
       option_lugares = self.filter_manager.get_section_locations("s1", global_filters, multi=True)
       
       st.write(f"Proporción de cocteles en {', '.join(option_lugares)} entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
       
       # Usar tu función SQL
       resultado = data_section_1_proporcion_combinada_sql(
           fecha_inicio.strftime('%Y-%m-%d'),
           fecha_fin.strftime('%Y-%m-%d'),
           option_lugares,
           option_fuente
       )  
       
       if not resultado.empty:
           df_resultado = convertir_a_formato_streamlit(resultado)
           st.dataframe(df_resultado, hide_index=True)
       else:
           st.warning("No hay datos para mostrar")
   
    def section_8_conteo_posiciones(self, global_filters: Dict[str, Any], mostrar_todos: bool):
     """8.- Gráfico de barras contando posiciones"""
     from sections.functions.grafico8 import data_section_8_conteo_posiciones_sql, convertir_posicion_a_nombre
     
     st.subheader("8.- Gráfico de barras contando posiciones en lugar y fecha específica")
     
     fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s8", global_filters)
     
     col1, col2, col3 = st.columns(3)
     with col1:
         # Obtener lugares disponibles (puedes usar self.lugares_uniques o una consulta)
         available_locations = self.lugares_uniques
         option_lugar = st.selectbox(
             "Lugar", 
             options=sorted(available_locations), 
             key="lugar_s8"
         )
     with col2:
         option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s8")
     with col3:
         option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s8")
     
     # Usar la nueva función SQL
     conteo_data = data_section_8_conteo_posiciones_sql(
         fecha_inicio.strftime('%Y-%m-%d'),
         fecha_fin.strftime('%Y-%m-%d'),
         option_lugar,
         option_fuente,
         option_nota
     )
     
     if not conteo_data.empty:
         # Convertir posiciones a nombres si es necesario
         conteo_data = convertir_posicion_a_nombre(conteo_data)
         
         # Crear el título dinámico
         if option_nota == 'Con coctel':
             titulo = f'Conteo de posiciones con coctel en {option_lugar}'
         elif option_nota == 'Sin coctel':
             titulo = f'Conteo de posiciones sin coctel en {option_lugar}'
         else:
             titulo = f'Conteo de posiciones en {option_lugar}'
         
         if option_fuente != "Todos":
             titulo += f' - {option_fuente}'
         else:
             titulo += ' por tipo de medio'
         
         # Crear el gráfico de barras
         fig = px.bar(
             conteo_data,
             x='Posición',
             y='count',
             color='Tipo de Medio',
             title=titulo,
             barmode='group',
             labels={'count': 'Conteo', 'Posición': 'Posición', 'Tipo de Medio': 'Tipo de Medio'},
             color_discrete_map={'Radio': '#3F6EC3', 'TV': '#A1A1A1', 'Redes': '#C00000'},
             text='count'
         )
         
         fig.update_layout(
             xaxis_title='Posición',
             yaxis_title='Conteo',
             legend_title='Tipo de Medio'
         )
         
         st.plotly_chart(fig, use_container_width=True)
         
         # Mostrar mensaje descriptivo
         st.write(f"Gráfico de barras contando posiciones en {option_lugar} entre {fecha_inicio.strftime('%Y-%m-%d')} y {fecha_fin.strftime('%Y-%m-%d')}")
         
     else:
         st.warning("No hay datos para mostrar")
    '''
    def section_8_conteo_posiciones(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """8.- Gráfico de barras contando posiciones"""
        st.subheader("8.- Gráfico de barras contando posiciones en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s8", global_filters)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            # Local location selector - independent of global filters
            available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s8"
            )
        with col2:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s8")
        with col3:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s8")
        
        temp_data = self.temp_coctel_fuente[
            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
            (self.temp_coctel_fuente['lugar'] == option_lugar)
        ]
        
        if not temp_data.empty:
            conteo_data = self.analytics.calculate_position_count(temp_data, option_fuente, option_nota)
            
            if not conteo_data.empty:
                titulo = f'Conteo de posiciones {option_nota.lower()} en {option_lugar} por tipo de medio'
                if option_fuente != "Todos":
                    titulo = f'Conteo de posiciones {option_nota.lower()} en {option_lugar} - {option_fuente}'
                
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
    ''' 

    '''
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
    '''
    def section_9_distribucion_posiciones(self, global_filters: Dict[str, Any], mostrar_todos: bool):
     """9.- Gráfico de dona que representa el porcentaje de posiciones"""
     from sections.functions.grafico9 import data_section_9_distribucion_posiciones_sql, convertir_posicion_a_nombre
     
     st.subheader("9.- Gráfico de dona que representa el porcentaje de posiciones en lugar y fecha específica")
     
     fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s9", global_filters)
     
     col1, col2 = st.columns(2)
     with col1:
         option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s9")
     with col2:
         option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s9")
     
     option_lugares = self.filter_manager.get_section_locations("s9", global_filters, multi=True)
     
     # Usar la nueva función SQL
     distrib_data = data_section_9_distribucion_posiciones_sql(
         fecha_inicio.strftime('%Y-%m-%d'),
         fecha_fin.strftime('%Y-%m-%d'),
         option_lugares,
         option_fuente,
         option_nota
     )
     
     if not distrib_data.empty:
         # Convertir posiciones a nombres si es necesario
         distrib_data = convertir_posicion_a_nombre(distrib_data)
         
         # Crear el título dinámico
         if option_nota == 'Con coctel':
             titulo = 'Porcentaje de posiciones con coctel respecto del total'
         elif option_nota == 'Sin coctel':
             titulo = 'Porcentaje de posiciones sin coctel respecto del total'
         else:
             titulo = 'Porcentaje de posiciones respecto del total'
         
         # Crear el gráfico de dona
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
    
     
    def section_10_eventos_coctel(self, global_filters: Dict[str, Any], mostrar_todos: bool):
      """10.- Porcentaje de acontecimientos con coctel"""
      from sections.functions.grafico10 import data_section_10_eventos_coctel_sql, convertir_a_formato_grafico
      
      st.subheader("10.- Porcentaje de acontecimientos con coctel en lugar y fecha específica")
      
      fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s10", global_filters)
      
      option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s10")
      option_lugares = self.filter_manager.get_section_locations("s10", global_filters, multi=True)
      
      # Usar la nueva función SQL
      resultado_sql = data_section_10_eventos_coctel_sql(
          fecha_inicio.strftime('%Y-%m-%d'),
          fecha_fin.strftime('%Y-%m-%d'),
          option_lugares
      )
      
      if not resultado_sql.empty:
          # Filtrar por fuente si no es "Todos"
          if option_fuente != "Todos":
              resultado_filtrado = resultado_sql[resultado_sql['fuente'] == option_fuente]
          else:
              resultado_filtrado = resultado_sql
          
          if not resultado_filtrado.empty:
              # Convertir a formato para gráfico
              event_data = convertir_a_formato_grafico(resultado_filtrado)
              
              # Si es "Todos", agregar por tipo de coctel (sumar todas las fuentes)
              if option_fuente == "Todos":
                  event_data = event_data.groupby('Coctel').agg({
                      'count': 'sum'
                  }).reset_index()
              
              st.write(f"Porcentaje de acontecimientos con coctel en {', '.join(option_lugares)}")
              
              # Crear título dinámico
              if option_fuente == "Todos":
                  titulo = 'Porcentaje de acontecimientos con coctel (Todas las fuentes)'
              else:
                  titulo = f'Porcentaje de acontecimientos con coctel - {option_fuente}'
              
              fig = px.pie(
                  event_data,
                  values='count',
                  names='Coctel',
                  title=titulo,
                  hole=0.3,
                  color='Coctel',
                  color_discrete_map={'Sin coctel': 'orange', 'Con coctel': 'Blue'}
              )
              
              fig.update_traces(
                  textposition='inside' if mostrar_todos else 'none',
                  textinfo='label+percent' if mostrar_todos else 'label'
              )
              
              st.plotly_chart(fig, use_container_width=True)
              
              # Mostrar tabla de datos (opcional)
              '''
              if mostrar_todos:
                  st.write("Detalle de datos:")
                  if option_fuente == "Todos":
                      st.dataframe(event_data, hide_index=True)
                  else:
                      # Mostrar datos detallados por fuente
                      resultado_detalle = convertir_a_formato_grafico(resultado_filtrado)
                      st.dataframe(resultado_detalle, hide_index=True)
              '''        
          else:
              st.warning(f"No hay datos para mostrar para la fuente: {option_fuente}")
      else:
          st.warning("No hay datos para mostrar")
    '''
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
    '''

    def section_11_cocteles_fuente_lugar(self, global_filters: Dict[str, Any]):
      """11.- Cantidad de cocteles por fuente y lugar"""
      from sections.functions.grafico11 import data_section_11_conteo_integrado_sql
      
      st.subheader("11.- Cantidad de cocteles por fuente y lugar en fecha específica")
      
      fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s11", global_filters)
      option_lugares = self.filter_manager.get_section_locations("s11", global_filters, multi=True)
      
      # Usar la nueva función SQL de grafico11
      try:
          resultado = data_section_11_conteo_integrado_sql(
              fecha_inicio.strftime('%Y-%m-%d'),
              fecha_fin.strftime('%Y-%m-%d'),
              option_lugares
          )
      except Exception as e:
          st.error(f"ERROR al ejecutar la consulta: {e}")
          st.warning("No hay datos para mostrar")
          return
      
      if not resultado.empty:
          # Mostrar solo las columnas de cocteles como solicitaste
          result_display = resultado[['Lugar', 'Radio_Con_Coctel', 'TV_Con_Coctel', 'Redes_Con_Coctel']].copy()
          result_display = result_display.rename(columns={
              'Lugar': 'lugar',
              'Radio_Con_Coctel': 'radio con coctel', 
              'TV_Con_Coctel': 'tv con coctel',
              'Redes_Con_Coctel': 'redes con coctel'
          })
          
          # Filtrar columnas que tienen datos para mantener formato limpio
          columns_to_show = ['lugar']
          if result_display['radio con coctel'].sum() > 0:
              columns_to_show.append('radio con coctel')
          if result_display['tv con coctel'].sum() > 0:
              columns_to_show.append('tv con coctel') 
          if result_display['redes con coctel'].sum() > 0:
              columns_to_show.append('redes con coctel')
          
          result_final = result_display[columns_to_show]
          
          st.write(f"Cantidad de cocteles por fuente y lugar entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
          st.dataframe(result_final, hide_index=True)
      else:
          st.warning("No hay datos para mostrar")
    '''
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

    '''        

    def section_12_medios_generan_coctel(self, global_filters: Dict[str, Any]):
       """12.- Reporte semanal acerca de cuantas radios, redes y tv generaron coctel"""
       st.subheader("12.- Reporte semanal acerca de cuantas radios, redes y tv generaron coctel")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s12", global_filters)
       option_lugares = self.filter_manager.get_section_locations("s12", global_filters, multi=True)
       
       # Convertir fechas a string format para SQL
       fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
       fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
       
       # Importar la función SQL
       from sections.functions.grafico12 import data_section_12_medios_generan_coctel_sql
       
       # Obtener los tres DataFrames: resumen, desagregado y gráfico
       tabla_resumen, tabla_desagregada, datos_grafico = data_section_12_medios_generan_coctel_sql(
           fecha_inicio_str, 
           fecha_fin_str, 
           option_lugares
       )
       
       if not tabla_resumen.empty:
           # 1. TABLA RESUMEN (agregada por lugar y fuente)
           st.write("### 📊 Resumen: Cantidad de medios únicos que generaron cóctel")
           st.write(f"**Período:** {fecha_inicio.strftime('%d.%m.%Y')} al {fecha_fin.strftime('%d.%m.%Y')}")
           st.dataframe(tabla_resumen, hide_index=True)
           
           # 2. GRÁFICO DE BARRAS APILADAS
           if not datos_grafico.empty:
               fig = px.bar(
                   datos_grafico,
                   x='lugar',
                   y='Cantidad',
                   color='fuente',
                   text='Cantidad',
                   title="Cantidad de Medios (Canales/Páginas) que generan Cocteles por Lugar",
                   labels={'Cantidad': 'Número de Medios', 'lugar': 'Lugar', 'fuente': 'Fuente'},
                   color_discrete_map=FUENTE_COLORS,
                   barmode='stack'
               )
               
               fig.update_traces(textposition='inside')
               fig.update_layout(
                   xaxis_title="Lugar",
                   yaxis_title="Cantidad de Medios Únicos",
                   showlegend=True,
                   legend_title="Fuente"
               )
               
               st.plotly_chart(fig, use_container_width=True)
           
           # 3. TABLA DESAGREGADA (detalle por cada canal/página)
           if not tabla_desagregada.empty:
               st.write("### 📋 Detalle: Medios que generaron cóctel")
               st.write("*Lista completa de canales y páginas de Facebook que generaron al menos un cóctel*")
               
               # Agregar filtro por fuente en la tabla desagregada
               col1, col2 = st.columns([1, 3])
               with col1:
                   fuentes_disponibles = ['Todos'] + sorted(tabla_desagregada['fuente'].unique().tolist())
                   filtro_fuente = st.selectbox(
                       "Filtrar por fuente:", 
                       fuentes_disponibles,
                       key="filtro_fuente_s12"
                   )
               
               # Aplicar filtro si no es "Todos"
               if filtro_fuente != 'Todos':
                   tabla_mostrar = tabla_desagregada[tabla_desagregada['fuente'] == filtro_fuente].copy()
               else:
                   tabla_mostrar = tabla_desagregada.copy()
               
               # Renombrar columnas para mejor visualización
               tabla_mostrar = tabla_mostrar.rename(columns={
                   'fuente': 'Fuente',
                   'lugar': 'Lugar'
               })
               
               # Mostrar estadísticas
               total_medios = len(tabla_mostrar)
               total_cocteles = tabla_mostrar['Cantidad de Cocteles'].sum()
               
               col1, col2, col3 = st.columns(3)
               with col1:
                   st.metric("Total Medios", total_medios)
               with col2:
                   st.metric("Total Cocteles", int(total_cocteles))
               with col3:
                   promedio = total_cocteles / total_medios if total_medios > 0 else 0
                   st.metric("Promedio por Medio", f"{promedio:.1f}")
               
               # Mostrar tabla con opción de búsqueda
               st.dataframe(
                   tabla_mostrar,
                   hide_index=True,
                   use_container_width=True,
                   height=400
               )
               
               # Opción para descargar
               csv = tabla_mostrar.to_csv(index=False).encode('utf-8')
               st.download_button(
                   label="📥 Descargar tabla completa (CSV)",
                   data=csv,
                   file_name=f"medios_coctel_{fecha_inicio_str}_{fecha_fin_str}.csv",
                   mime="text/csv",
                   key="download_s12"
               )
           else:
               st.info("No hay datos desagregados para mostrar")
       else:
           st.warning("No hay datos para mostrar en el período seleccionado")


    #def section_12_medios_generan_coctel(self, global_filters: Dict[str, Any]):
    #    """12.- Reporte semanal acerca de cuantas radios, redes y tv generaron coctel"""
    #    st.subheader("12.- Reporte semanal acerca de cuantas radios, redes y tv generaron coctel")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s12", global_filters)
    #    option_lugares = self.filter_manager.get_section_locations("s12", global_filters, multi=True)
    #    
    #    temp_data = self.temp_coctel_fuente[
    #        (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente['lugar'].isin(option_lugares))
    #    ]
    #    
    #    temp_fb = self.temp_coctel_fuente_fb[
    #        (self.temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente_fb['lugar'].isin(option_lugares))
    #    ]
    #    
    #    if not temp_data.empty:
    #        tabla_data, chart_data = self.analytics.calculate_media_generating_coctel(temp_data, temp_fb)
    #        
    #        if not tabla_data.empty:
    #            st.dataframe(tabla_data, hide_index=True)
    #            
    #            y_max = chart_data['Cantidad'].max() * 1.1
    #            
    #            fig = px.bar(
    #                chart_data,
    #                x='lugar',
    #                y='Cantidad',
    #                color='Fuente',
    #                text='Cantidad',
    #                title="Cantidad de Medios (Canales) que generan Cocteles por Lugar",
    #                labels={'Cantidad': 'Número de Medios', 'lugar': 'Lugar', 'Fuente': 'Fuente'},
    #                color_discrete_map=FUENTE_COLORS,
    #                barmode='stack'
    #            )
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para mostrar")
    #    else:
    #        st.warning("No hay datos para mostrar")
    #
    #

    # Agregar este import al inicio del archivo coctel_sections.py, junto con los otros imports


# Y reemplazar toda la función section_13_conteo_mensual con esto:

    def section_13_conteo_mensual(self, global_filters: Dict[str, Any]):
        from sections.functions.grafico13 import data_section_13_acontecimientos_por_lugar_mes
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
        fecha_inicio = f'{year_inicio}-{month_inicio:02d}-01'
        fecha_fin = pd.to_datetime(f'{year_fin}-{month_fin}-01') + pd.offsets.MonthEnd(1)
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
        
        # Usar la nueva función de grafico13.py
        resultado = data_section_13_acontecimientos_por_lugar_mes(fecha_inicio, fecha_fin_str, option_lugares)
        
        if not resultado.empty:
            st.write(f"Conteo mensual de coctel en {len(option_lugares)} regiones entre {month_inicio:02d}/{year_inicio} y {month_fin:02d}/{year_fin}")
            
            # Mostrar tabla
            st.dataframe(resultado, hide_index=True)
            resultado_agregado = resultado.groupby(['año_mes', 'fuente'])['coctel'].sum().reset_index()
            # Crear gráfico de barras apiladas
            color_map = {'RADIO': '#3F6EC3', 'TV': '#A1A1A1', 'REDES': '#C00000'}
            
            fig = px.bar(
                resultado_agregado,
                x='año_mes',
                y='coctel',
                color='fuente',
                barmode='stack',
                title='Conteo de cocteles por mes y fuente',
                labels={'año_mes': 'Año y Mes', 'coctel': 'Número de Cocteles', 'fuente': 'Fuente'},
                text='coctel',
                color_discrete_map=color_map,
            )
            
            fig.update_traces(textposition="inside")
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
        from sections.functions.grafico14 import data_section_14_favor_contra_neutral_sql
        
        st.subheader("14.- Porcentaje de notas que sean a favor, neutral y en contra")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s14", global_filters)
        
        option_nota = st.selectbox("Notas", ("Con coctel", "Sin coctel", "Todos"), key="nota_s14")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # ✅ USAR FILTROS GLOBALES CON SELECCIÓN MÚLTIPLE
            option_lugares = self.filter_manager.get_section_locations("s14", global_filters, multi=True)
        
        with col2:
            # filtro de fuente con labels amigables
            fuente_display_to_real = {
                "Radio": "RADIO",
                "TV": "TV",
                "Redes": "REDES"
            }
            
            option_fuente_display = st.multiselect(
                "Fuente", ["Radio", "TV", "Redes"],
                ["Radio", "TV", "Redes"], key="fuente_s14"
            )
            
            option_fuente = [fuente_display_to_real[f] for f in option_fuente_display]
        
        # Convertir fechas a string formato YYYY-MM-DD
        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
        
        # ✅ Llamar a la función SQL con LISTA de lugares
        conteo_pct, long_df, conteo_abs = data_section_14_favor_contra_neutral_sql(
            fecha_inicio_str, 
            fecha_fin_str, 
            option_lugares,  # ✅ Ahora es una lista
            option_fuente, 
            option_nota
        )
        
        if not conteo_pct.empty:
            if option_nota == "Con coctel":
                titulo = "Porcentaje de notas que sean a favor, neutral y en contra con coctel"
            elif option_nota == "Sin coctel":
                titulo = "Porcentaje de notas que sean a favor, neutral y en contra sin coctel"
            else:
                titulo = "Porcentaje de notas que sean a favor, neutral y en contra"
            
            # encabezado de análisis
            if not option_lugares:
                lugar_texto = "todas las regiones"
            elif len(option_lugares) == 1:
                lugar_texto = option_lugares[0]
            else:
                lugar_texto = f"{len(option_lugares)} lugares seleccionados"
            
            st.write(f"Análisis en **{lugar_texto}** entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')}")
            
            # preparar data para tablas
            conteo_pct["A favor (%)"] = conteo_pct["a_favor_pct"].map("{:.2f}".format)
            conteo_pct["En contra (%)"] = conteo_pct["en_contra_pct"].map("{:.2f}".format)
            conteo_pct["Neutral (%)"] = conteo_pct["neutral_pct"].map("{:.2f}".format)
            
            conteo_abs = conteo_abs.rename(columns={
                "a_favor": "A favor",
                "en_contra": "En contra",
                "neutral": "Neutral"
            })
            
            # mostrar tablas lado a lado
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("Porcentajes por tipo de nota")
                st.dataframe(
                    conteo_pct[["año_mes", "A favor (%)", "Neutral (%)", "En contra (%)"]],
                    hide_index=True
                )
            
            with col2:
                st.write("Conteo absoluto de notas")
                st.dataframe(
                    conteo_abs[["año_mes", "A favor", "Neutral", "En contra"]],
                    hide_index=True
                )
            
            # gráfico
            fig = px.bar(
                long_df,
                x="año_mes",
                y="Porcentaje",
                color="Tipo de Nota",
                barmode="stack",
                title=titulo,
                labels={"año_mes": "Año y Mes", "Porcentaje": "Porcentaje"},
                text=long_df["Porcentaje"].map("{:.2f}%".format) if mostrar_todos else None,
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

    #def section_14_notas_favor_contra(self, global_filters: Dict[str, Any], mostrar_todos: bool):
    #    """14.- Porcentaje de notas que sean a favor, neutral y en contra"""
    #    st.subheader("14.- Porcentaje de notas que sean a favor, neutral y en contra")
#
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s14", global_filters)
#
    #    option_nota = st.selectbox("Notas", ("Con coctel", "Sin coctel", "Todos"), key="nota_s14")
#
    #    col1, col2 = st.columns(2)
#
    #    with col1:
    #        # filtro de lugar con opción "Todas las regiones"
    #        available_locations = sorted(self.temp_coctel_fuente['lugar'].dropna().unique())
    #        location_options = ["Todas las regiones"] + available_locations
    #        option_lugar = st.selectbox("Lugar", options=location_options, key="lugar_s14")
#
    #    with col2:
    #        # filtro de fuente con labels amigables
    #        fuente_display_to_real = {
    #            "Radio": "RADIO",
    #            "TV": "TV",
    #            "Redes": "REDES"
    #        }
#
    #        option_fuente_display = st.multiselect(
    #            "Fuente", ["Radio", "TV", "Redes"],
    #            ["Radio", "TV", "Redes"], key="fuente_s14"
    #        )
#
    #        option_fuente = [fuente_display_to_real[f] for f in option_fuente_display]
#
    #    # aplicar filtros
    #    temp_data = self.temp_coctel_fuente[
    #        (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
    #        ((self.temp_coctel_fuente['lugar'] == option_lugar) if option_lugar != "Todas las regiones" else True) &
    #        (self.temp_coctel_fuente['fuente_nombre'].isin(option_fuente))
    #    ]
#
    #    if not temp_data.empty:
    #        conteo_pct, long_df, conteo_abs = self.analytics.calculate_favor_contra_notes(temp_data, option_nota)
#
    #        if not conteo_pct.empty:
    #            if option_nota == "Con coctel":
    #                titulo = "Porcentaje de notas que sean a favor, neutral y en contra con coctel"
    #            elif option_nota == "Sin coctel":
    #                titulo = "Porcentaje de notas que sean a favor, neutral y en contra sin coctel"
    #            else:
    #                titulo = "Porcentaje de notas que sean a favor, neutral y en contra"
#
    #            # encabezado de análisis
    #            lugar_texto = "todas las regiones" if option_lugar == "Todas las regiones" else option_lugar
    #            st.write(f"Análisis en **{lugar_texto}** entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')}")
#
    #            # preparar data para tablas
    #            conteo_pct["A favor (%)"] = conteo_pct["a_favor_pct"].map("{:.1f}".format)
    #            conteo_pct["En contra (%)"] = conteo_pct["en_contra_pct"].map("{:.1f}".format)
    #            conteo_pct["Neutral (%)"] = conteo_pct["neutral_pct"].map("{:.1f}".format)
#
    #            conteo_abs = conteo_abs.rename(columns={
    #                "a_favor": "A favor",
    #                "en_contra": "En contra",
    #                "neutral": "Neutral"
    #            })
#
    #            # mostrar tablas lado a lado
    #            col1, col2 = st.columns(2)
#
    #            with col1:
    #                st.write("Porcentajes por tipo de nota")
    #                st.dataframe(
    #                    conteo_pct[["año_mes", "A favor (%)", "Neutral (%)", "En contra (%)"]],
    #                    hide_index=True
    #                )
#
    #            with col2:
    #                st.write("Conteo absoluto de notas")
    #                st.dataframe(
    #                    conteo_abs[["año_mes", "A favor", "Neutral", "En contra"]],
    #                    hide_index=True
    #                )
#
    #            # gráfico
    #            fig = px.bar(
    #                long_df,
    #                x="año_mes",
    #                y="Porcentaje",
    #                color="Tipo de Nota",
    #                barmode="stack",
    #                title=titulo,
    #                labels={"año_mes": "Año y Mes", "Porcentaje": "Porcentaje"},
    #                text=long_df["Porcentaje"].map("{:.1f}%".format) if mostrar_todos else None,
    #                color_discrete_map={
    #                    "a_favor_pct": "blue",
    #                    "en_contra_pct": "red",
    #                    "neutral_pct": "gray",
    #                },
    #            )
#
    #            fig.update_layout(barmode='stack', xaxis={'categoryorder': 'category ascending'})
    #            fig.for_each_trace(lambda t: t.update(name=t.name.replace('_pct', ' (%)')))
#
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para mostrar")
    #    else:
    #        st.warning("No hay datos para mostrar")
     

    
    
    def section_15_proporcion_mensajes(self, global_filters: Dict[str, Any]):
        from sections.functions.grafico15 import data_section_15_proporcion_mensajes_sql
        """15.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota"""
        st.subheader("15.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s15", global_filters)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s15")
        with col2:
            available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
            option_lugar = st.selectbox(
                "Lugar", 
                options=sorted(available_locations), 
                key="lugar_s15"
            )
        with col3:
            option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s15")
        
        # Convertir fechas a string
        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
        
        # Llamar función SQL
        prop_data = data_section_15_proporcion_mensajes_sql(
            fecha_inicio_str,
            fecha_fin_str,
            option_lugar,
            option_fuente,
            option_nota
        )
        
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
    #     
#
    #def section_15_proporcion_mensajes(self, global_filters: Dict[str, Any]):
    #    """15.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota"""
    #    st.subheader("15.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específica")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s15", global_filters)
    #    
    #    col1, col2, col3 = st.columns(3)
    #    with col1:
    #        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s15")
    #    with col2:
    #        # Local location selector - independent of global filters
    #        available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
    #        option_lugar = st.selectbox(
    #            "Lugar", 
    #            options=sorted(available_locations), 
    #            key="lugar_s15"
    #        )
    #    with col3:
    #        option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s15")
    #    
    #    temp_data = self.temp_coctel_fuente[
    #        (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente['lugar'] == option_lugar)
    #    ]
    #    
    #    if not temp_data.empty:
    #        prop_data = self.analytics.calculate_message_proportion_by_position(temp_data, option_fuente, option_nota)
    #        
    #        if not prop_data.empty:
    #            if option_nota == 'Con coctel':
    #                titulo = f"Proporción de mensajes emitidos con coctel por {option_fuente} en {option_lugar}"
    #            elif option_nota == 'Sin coctel':
    #                titulo = f"Proporción de mensajes emitidos sin coctel por {option_fuente} en {option_lugar}"
    #            else:
    #                titulo = f"Proporción de mensajes emitidos por {option_fuente} en {option_lugar}"
    #            
    #            fig = px.pie(
    #                prop_data,
    #                values='frecuencia',
    #                names='id_posicion',
    #                title=titulo,
    #                color='id_posicion',
    #                color_discrete_map=COLOR_POSICION_DICT,
    #                hole=0.3
    #            )
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #            st.dataframe(prop_data, hide_index=True)
    #        else:
    #            st.warning("No hay datos para mostrar")
    #    else:
    #        st.warning("No hay datos para mostrar")
    


    def section_16_mensajes_por_tema(self, global_filters: Dict[str, Any]):

        from sections.functions.grafico16 import data_section_16_mensajes_por_tema_sql
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
        
        # ====================================
        # CAMBIO PRINCIPAL: Usar la función SQL
        # ====================================
        
        # Convertir fechas a string formato YYYY-MM-DD
        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
        
        # Llamar a la función SQL
        topic_data = data_section_16_mensajes_por_tema_sql(
            fecha_inicio_str,
            fecha_fin_str,
            option_lugar,
            option_fuente,
            option_nota,
            top_n=10
        )
        
        # ====================================
        # RESTO DEL CÓDIGO IGUAL (visualización)
        # ====================================
        
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

    #def section_16_mensajes_por_tema(self, global_filters: Dict[str, Any]):
    #    """16.- Recuento de Mensajes Emitidos por Tema y Tipo de Nota"""
    #    st.subheader("16.- Recuento de Mensajes Emitidos por Tema y Tipo de Nota en Lugar y Fecha Específica")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s16", global_filters)
    #    
    #    col1, col2, col3 = st.columns(3)
    #    with col1:
    #        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s16")
    #    with col2:
    #        # Local location selector - independent of global filters
    #        available_locations = self.temp_coctel_temas['lugar'].dropna().unique()
    #        option_lugar = st.selectbox(
    #            "Lugar", 
    #            options=sorted(available_locations), 
    #            key="lugar_s16"
    #        )
    #    with col3:
    #        option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s16")
    #    
    #    temp_data = self.temp_coctel_temas[
    #        (self.temp_coctel_temas['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_temas['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_temas['lugar'] == option_lugar)
    #    ]
    #    
    #    if not temp_data.empty:
    #        topic_data = self.analytics.calculate_messages_by_topic(temp_data, option_fuente, option_nota, 10)
    #        
    #        if not topic_data.empty:
    #            # Sort the data by frequency in descending order (largest to smallest)
    #            topic_data_sorted = topic_data.groupby('descripcion')['frecuencia'].sum().reset_index()
    #            topic_data_sorted = topic_data_sorted.sort_values('frecuencia', ascending=False)
    #            
    #            # Merge back with original data to preserve all columns and maintain the order
    #            topic_data = topic_data.merge(
    #                topic_data_sorted[['descripcion']], 
    #                on='descripcion', 
    #                how='inner'
    #            )
    #            # Set the category order for x-axis
    #            topic_data['descripcion'] = pd.Categorical(
    #                topic_data['descripcion'], 
    #                categories=topic_data_sorted['descripcion'].tolist(), 
    #                ordered=True
    #            )
    #            topic_data = topic_data.sort_values('descripcion')
    #            
    #            if option_nota == 'Con coctel':
    #                titulo = f"Recuento de mensajes emitidos con coctel por {option_fuente} en {option_lugar}"
    #            elif option_nota == 'Sin coctel':
    #                titulo = f"Recuento de mensajes emitidos sin coctel por {option_fuente} en {option_lugar}"
    #            else:
    #                titulo = f"Recuento de mensajes emitidos por {option_fuente} en {option_lugar}"
    #            
    #            fig = px.bar(
    #                topic_data,
    #                x='descripcion',
    #                y='frecuencia',
    #                title=titulo,
    #                color='id_posicion',
    #                text='frecuencia',
    #                barmode='stack',
    #                labels={'frecuencia': 'Frecuencia', 'descripcion': 'Tema', 'id_posicion': 'Posición'},
    #                color_discrete_map=COLOR_POSICION_DICT,
    #                height=500
    #            )
    #            
    #            fig.update_xaxes(tickangle=45)
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para mostrar")
    #    else:
    #        st.warning("No hay datos para mostrar")
    

    

    def section_17_proporcion_por_tema(self, global_filters: Dict[str, Any], mostrar_todos: bool):
       
       from sections.functions.grafico17 import data_section_17_proporcion_por_tema_sql
       """17.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota por Tema"""
       st.subheader("17.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específicos")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s17", global_filters)
       
       col1, col2, col3 = st.columns(3)
       with col1:
           option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s17")
       with col2:
           available_locations = self.temp_coctel_temas['lugar'].dropna().unique()
           option_lugar = st.selectbox(
               "Lugar", 
               options=sorted(available_locations), 
               key="lugar_s17"
           )
       with col3:
           option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s17")
       
       # Convertir fechas a string
       fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
       fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
       
       # Llamar función SQL
       prop_data = data_section_17_proporcion_por_tema_sql(
           fecha_inicio_str,
           fecha_fin_str,
           option_lugar,
           option_fuente,
           option_nota,
           top_n=10
       )
       
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
   
           
    #def section_17_proporcion_por_tema(self, global_filters: Dict[str, Any], mostrar_todos: bool):
    #    """17.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota por Tema"""
    #    st.subheader("17.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específicos")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s17", global_filters)
    #    
    #    col1, col2, col3 = st.columns(3)
    #    with col1:
    #        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s17")
    #    with col2:
    #        # Local location selector - independent of global filters
    #        available_locations = self.temp_coctel_temas['lugar'].dropna().unique()
    #        option_lugar = st.selectbox(
    #            "Lugar", 
    #            options=sorted(available_locations), 
    #            key="lugar_s17"
    #        )
    #    with col3:
    #        option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s17")
    #    
    #    temp_data = self.temp_coctel_temas[
    #        (self.temp_coctel_temas['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_temas['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_temas['lugar'] == option_lugar)
    #    ]
    #    
    #    if not temp_data.empty:
    #        prop_data = self.analytics.calculate_topic_proportion(temp_data, option_fuente, option_nota, 10)
    #        
    #        if not prop_data.empty:
    #            if option_nota == 'Con coctel':
    #                titulo = f"Proporción de mensajes emitidos con coctel por {option_fuente} en {option_lugar}"
    #            elif option_nota == 'Sin coctel':
    #                titulo = f"Proporción de mensajes emitidos sin coctel por {option_fuente} en {option_lugar}"
    #            else:
    #                titulo = f"Proporción de mensajes emitidos por {option_fuente} en {option_lugar}"
    #            
    #            fig = px.bar(
    #                prop_data,
    #                x="porcentaje",
    #                y="descripcion",
    #                title=titulo,
    #                orientation='h',
    #                text=prop_data["porcentaje"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
    #                labels={'porcentaje': 'Porcentaje %', 'descripcion': 'Temas'}
    #            )
    #            
    #            fig.update_traces(textposition="outside" if mostrar_todos else "none")
    #            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para mostrar")
    #    else:
    #        st.warning("No hay datos para mostrar")
    

    

    def section_18_tendencia_por_medio(self, global_filters: Dict[str, Any]):
       from sections.functions.grafico18 import data_section_18_tendencia_por_medio_sql
       """18.- Tendencia de las notas emitidas en lugar y fecha específica por fuente y tipo de nota"""
       st.subheader("18.- Tendencia de las notas emitidas en lugar y fecha específica por fuente y tipo de nota")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s18", global_filters)
       
       col1, col2, col3 = st.columns(3)
       with col1:
           option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_s18")
       with col2:
           # Puedes usar temp_coctel_fuente para obtener lugares
           available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
           option_lugar = st.selectbox(
               "Lugar", 
               options=sorted(available_locations), 
               key="lugar_s18"
           )
       with col3:
           option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s18")
       
       # Convertir fechas a string
       fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
       fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
       
       # Llamar función SQL
       trend_data = data_section_18_tendencia_por_medio_sql(
           fecha_inicio_str,
           fecha_fin_str,
           option_lugar,
           option_fuente,
           option_nota
       )
       
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
   
           
    #def section_18_tendencia_por_medio(self, global_filters: Dict[str, Any]):
    #    """18.- Tendencia de las notas emitidas en lugar y fecha específica por fuente y tipo de nota"""
    #    st.subheader("18.- Tendencia de las notas emitidas en lugar y fecha específica por fuente y tipo de nota")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s18", global_filters)
    #    
    #    col1, col2, col3 = st.columns(3)
    #    with col1:
    #        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_s18")
    #    with col2:
    #        # Local location selector - independent of global filters
    #        # Get available locations from both dataframes
    #        available_locations_programas = self.temp_coctel_fuente_programas['lugar'].dropna().unique()
    #        available_locations_fb = self.temp_coctel_fuente_fb['lugar'].dropna().unique()
    #        all_locations = sorted(set(list(available_locations_programas) + list(available_locations_fb)))
    #        
    #        option_lugar = st.selectbox(
    #            "Lugar", 
    #            options=all_locations, 
    #            key="lugar_s18"
    #        )
    #    with col3:
    #        option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s18")
    #    
    #    temp_programas = self.temp_coctel_fuente_programas[
    #        (self.temp_coctel_fuente_programas['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente_programas['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente_programas['lugar'] == option_lugar)
    #    ]
    #    
    #    temp_fb = self.temp_coctel_fuente_fb[
    #        (self.temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente_fb['lugar'] == option_lugar)
    #    ]
    #    
    #    trend_data = self.analytics.calculate_notes_trend_by_medium(temp_programas, temp_fb, option_fuente, option_nota)
    #    
    #    if not trend_data.empty:
    #        if option_nota == 'Con coctel':
    #            titulo = f"Tendencia de las notas emitidas con coctel por {option_fuente} en {option_lugar}"
    #        elif option_nota == 'Sin coctel':
    #            titulo = f"Tendencia de las notas emitidas sin coctel por {option_fuente} en {option_lugar}"
    #        else:
    #            titulo = f"Tendencia de las notas emitidas por {option_fuente} en {option_lugar}"
    #        
    #        fig = px.bar(
    #            trend_data,
    #            x='medio_nombre',
    #            y='frecuencia',
    #            color='id_posicion',
    #            title=titulo,
    #            barmode='stack',
    #            color_discrete_map=COLOR_POSICION_DICT,
    #            labels={'frecuencia': 'Frecuencia', 'medio_nombre': 'Canal/Medio', 'id_posicion': 'Posición'},
    #            text='frecuencia',
    #        )
    #        
    #        fig.update_xaxes(tickangle=45)
    #        st.plotly_chart(fig, use_container_width=True)
    #    else:
    #        st.warning("No hay datos para mostrar")
    #

   

# ====================================
# PASO 2: Modificar el método section_19_notas_tiempo_posicion
# ====================================

    def section_19_notas_tiempo_posicion(self, global_filters: Dict[str, Any]):
          from sections.functions.grafico19 import data_section_19_notas_tiempo_posicion_sql
          """19.- Notas emitidas en un rango de tiempo segun posicion y coctel"""
          st.subheader("19.- Notas emitidas en un rango de tiempo segun posicion y coctel")
          
          fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s19", global_filters)
          option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s19")
          
          # ====================================
          # CAMBIO PRINCIPAL: Usar la función SQL
          # ====================================
          
          # Convertir fechas a string formato YYYY-MM-DD
          fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
          fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
          
          # Llamar a la función SQL
          time_data = data_section_19_notas_tiempo_posicion_sql(
              fecha_inicio_str,
              fecha_fin_str,
              option_nota
          )
          
          # ====================================
          # RESTO DEL CÓDIGO IGUAL (visualización)
          # ====================================
          
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
              
    #def section_19_notas_tiempo_posicion(self, global_filters: Dict[str, Any]):
    #    """19.- Notas emitidas en un rango de tiempo segun posicion y coctel"""
    #    st.subheader("19.- Notas emitidas en un rango de tiempo segun posicion y coctel")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s19", global_filters)
    #    option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s19")
    #    
    #    temp_data = self.temp_coctel_temas[
    #        (self.temp_coctel_temas['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_temas['fecha_registro'] <= fecha_fin)
    #    ]
    #    
    #    if not temp_data.empty:
    #        time_data = self.analytics.calculate_notes_by_time_position(temp_data, option_nota)
    #        
    #        if not time_data.empty:
    #            if option_nota == 'Con coctel':
    #                titulo = f"Notas emitidas con coctel entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')} según posición"
    #            elif option_nota == 'Sin coctel':
    #                titulo = f"Notas emitidas sin coctel entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')} según posición"
    #            else:
    #                titulo = f"Notas emitidas entre {fecha_inicio.strftime('%d-%m-%Y')} y {fecha_fin.strftime('%d-%m-%Y')} según posición"
    #            
    #            fig = px.bar(
    #                time_data,
    #                x='id_posicion',
    #                y='frecuencia',
    #                title=titulo,
    #                labels={'frecuencia': 'Frecuencia', 'id_posicion': 'Posición'},
    #                color='id_posicion',
    #                color_discrete_map=COLOR_POSICION_DICT,
    #                text='frecuencia'
    #            )
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para mostrar")
    #    else:
    #        st.warning("No hay datos para mostrar")
    

    

    def section_20_actores_posiciones(self, global_filters: Dict[str, Any]):
       
       from sections.functions.grafico20 import data_section_20_actores_posiciones_sql
       """20.- Recuento de posiciones emitidas por actor en lugar y fecha específica"""
       st.subheader("20.- Recuento de posiciones emitidas por actor en lugar y fecha específica")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s20", global_filters)
       
       col1, col2, col3 = st.columns(3)
       with col1:
           option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s20")
       with col2:
           available_locations = self.temp_coctel_fuente_actores['lugar'].dropna().unique()
           option_lugar = st.selectbox(
               "Lugar", 
               options=sorted(available_locations), 
               key="lugar_s20"
           )
       with col3:
           option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s20")
       
       # Convertir fechas a string
       fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
       fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
       
       # Llamar función SQL
       actor_data = data_section_20_actores_posiciones_sql(
           fecha_inicio_str,
           fecha_fin_str,
           option_lugar,
           option_fuente,
           option_nota,
           top_n=10
       )
       
       if not actor_data.empty:
           if option_nota == 'Con coctel':
               titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}. Notas con coctel"
           elif option_nota == 'Sin coctel':
               titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}. Notas sin coctel"
           else:
               titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}"
           
           # Obtener el orden correcto de los actores (de mayor a menor frecuencia)
           actor_order = actor_data.groupby('nombre')['frecuencia'].sum().sort_values(ascending=False).index.tolist()
           
           fig = px.bar(
               actor_data,
               x='nombre',
               y='frecuencia',
               title=titulo,
               color='posicion',
               barmode='stack',
               color_discrete_map=COLOR_POSICION_DICT,
               labels={'frecuencia': 'Frecuencia', 'nombre': 'Actor', 'posicion': 'Posición'},
               text='frecuencia',
               category_orders={'nombre': actor_order}
           )
           
           fig.update_xaxes(tickangle=45)
           st.plotly_chart(fig, use_container_width=True)
       else:
           st.warning("No hay datos para mostrar")
   
           
    #def section_20_actores_posiciones(self, global_filters: Dict[str, Any]):
    #    """20.- Recuento de posiciones emitidas por actor en lugar y fecha específica"""
    #    st.subheader("20.- Recuento de posiciones emitidas por actor en lugar y fecha específica")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s20", global_filters)
    #    
    #    col1, col2, col3 = st.columns(3)
    #    with col1:
    #        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s20")
    #    with col2:
    #        # Local location selector - independent of global filters
    #        available_locations = self.temp_coctel_fuente_actores['lugar'].dropna().unique()
    #        option_lugar = st.selectbox(
    #            "Lugar", 
    #            options=sorted(available_locations), 
    #            key="lugar_s20"
    #        )
    #    with col3:
    #        option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s20")
    #    
    #    temp_data = self.temp_coctel_fuente_actores[
    #        (self.temp_coctel_fuente_actores['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente_actores['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente_actores['lugar'] == option_lugar)
    #    ]
    #    
    #    if not temp_data.empty:
    #        actor_data = self.analytics.calculate_actor_positions(temp_data, option_fuente, option_nota, 10)
    #        
    #        if not actor_data.empty:
    #            if option_nota == 'Con coctel':
    #                titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}. Notas con coctel"
    #            elif option_nota == 'Sin coctel':
    #                titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}. Notas sin coctel"
    #            else:
    #                titulo = f"Recuento de posiciones emitidas por actor en {option_lugar}"
    #            
    #            fig = px.bar(
    #                actor_data,
    #                x='nombre',
    #                y='frecuencia',
    #                title=titulo,
    #                color='posicion',
    #                barmode='stack',
    #                color_discrete_map=COLOR_POSICION_DICT,
    #                labels={'frecuencia': 'Frecuencia', 'nombre': 'Actor', 'posicion': 'Posición'},
    #                text='frecuencia'
    #            )
    #            
    #            fig.update_xaxes(tickangle=45)
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para mostrar")
    #    else:
    #        st.warning("No hay datos para mostrar")
    #


    def section_21_porcentaje_medios(self, global_filters: Dict[str, Any], mostrar_todos: bool):
         from sections.functions.grafico21 import data_section_21_porcentaje_medios_sql, calcular_promedios_por_fuente
         
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
         
         # Calcular fechas de inicio y fin del período
         fecha_inicio = f"{ano_inicio}-{MESES_ES.index(mes_inicio) + 1:02d}-01"
         # Para fecha_fin, necesitamos el último día del mes
         import calendar
         ultimo_dia = calendar.monthrange(ano_fin, MESES_ES.index(mes_fin) + 1)[1]
         fecha_fin = f"{ano_fin}-{MESES_ES.index(mes_fin) + 1:02d}-{ultimo_dia:02d}"
         
         # Llamar a la función SQL
         resultado = data_section_21_porcentaje_medios_sql(
             fecha_inicio,
             fecha_fin,
             option_regiones
         )
         
         if not resultado.empty:
             # Calcular promedios por fuente
             promedios = calcular_promedios_por_fuente(resultado)
             
             # Mapeo de colores
             color_map = {
                 "RADIO": "#3F6EC3",  # Azul
                 "REDES": "#C00000",  # Rojo
                 "TV": "#A1A1A1"      # Gris
             }
             
             # Crear el gráfico de barras agrupadas
             fig = px.bar(
                 resultado,
                 x="lugar",
                 y="porcentaje_coctel",
                 color="fuente",
                 barmode="group",
                 title=f"Porcentaje de cóctel de todos los medios - {mes_inicio}/{ano_inicio} hasta {mes_fin}/{ano_fin}",
                 labels={
                     "lugar": "Regiones", 
                     "porcentaje_coctel": "Porcentaje de Cóctel (%)",
                     "fuente": "Fuente"
                 },
                 text=resultado["porcentaje_coctel"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                 color_discrete_map=color_map,
             )
             
             fig.update_layout(
                 font=dict(size=8),
                 xaxis_tickangle=-45
             )
             
             fig.update_traces(
                 textposition="outside" if mostrar_todos else "none"
             )
             
             # Agregar líneas de promedio por fuente
             for fuente, promedio in promedios.items():
                 # Determinar color de la línea
                 line_color = color_map.get(fuente, "black")
                 
                 fig.add_hline(
                     y=promedio,
                     line_dash="dash",
                     annotation_text=f"Promedio de {fuente}: {promedio:.2f}%",
                     annotation_position="right",
                     line_color=line_color,
                 )
             
             st.plotly_chart(fig, use_container_width=True)
         else:
             st.warning("No hay datos para mostrar")

    #def section_21_porcentaje_medios(self, global_filters: Dict[str, Any], mostrar_todos: bool):
    #    """21.- Porcentaje de cóctel de todos los medios"""
    #    st.subheader("21.- Porcentaje de cóctel de todos los medios")
    #    
    #    # Para esta sección usamos selectores de año/mes
    #    ano_actual = datetime.now().year
    #    anos = list(range(ano_actual - 9, ano_actual + 1))
    #    
    #    col1, col2 = st.columns(2)
    #    with col1:
    #        ano_inicio = st.selectbox("Año de inicio", anos, len(anos)-1, key="ano_inicio_s21")
    #        mes_inicio = st.selectbox("Mes de inicio", MESES_ES, index=11, key="mes_inicio_s21")
    #    with col2:
    #        ano_fin = st.selectbox("Año de fin", anos, index=len(anos)-1, key="ano_fin_s21")
    #        mes_fin = st.selectbox("Mes de fin", MESES_ES, index=11, key="mes_fin_s21")
    #    
    #    option_regiones = self.filter_manager.get_section_locations("s21", global_filters, multi=True)
    #    
    #    year_month_start = f"{ano_inicio}-{MESES_ES.index(mes_inicio) + 1:02d}-01"
    #    year_month_end = f"{ano_fin}-{MESES_ES.index(mes_fin) + 1:02d}-01"
    #    
    #    if not self.temp_coctel_fuente.empty:
    #        result = self.analytics.calculate_coctel_percentage_by_media(
    #            self.temp_coctel_fuente, year_month_start, year_month_end
    #        )
    #        
    #        if result.empty:
    #            st.warning("No hay datos para mostrar en el período seleccionado")
    #            return
    #        
    #        # Check if 'lugar' column exists before filtering
    #        if 'lugar' not in result.columns:
    #            st.error(f"❌ Column 'lugar' not found in result. Available columns: {result.columns.tolist()}")
    #            return
    #        
    #        # Check if we have any matching regions
    #        available_places = result['lugar'].unique()
    #        matching_regions = [region for region in option_regiones if region in available_places]
    #        
    #        if not matching_regions:
    #            st.warning(f"⚠️ No hay datos para las regiones seleccionadas: {option_regiones}")
    #            st.info(f"💡 Regiones disponibles: {', '.join(available_places)}")
    #            return
    #        
    #        # Filtrar por regiones seleccionadas
    #        result = result[result['lugar'].isin(option_regiones)]
    #        
    #        if not result.empty:
    #            promedios = result.groupby('Fuente')['porcentaje_coctel'].mean().to_dict()
    #            
    #            fig = px.bar(
    #                result,
    #                x="lugar",
    #                y="porcentaje_coctel",
    #                color="Fuente",
    #                barmode="group",
    #                title=f"Porcentaje de cóctel de todos los medios - {mes_inicio} {ano_inicio} hasta {mes_fin} {ano_fin}",
    #                labels={"lugar": "Regiones", "porcentaje_coctel": "Porcentaje de Cóctel"},
    #                text=result["porcentaje_coctel"].map("{:.1f}%".format) if mostrar_todos else None,
    #                color_discrete_map={"Radio": "blue", "Redes": "red", "TV": "gray"},
    #            )
    #            
    #            fig.update_layout(font=dict(size=8))
    #            fig.update_traces(textposition="outside" if mostrar_todos else "none")
    #            
    #            # Agregar líneas de promedio por fuente
    #            for fuente, promedio in promedios.items():
    #                fig.add_hline(
    #                    y=promedio,
    #                    line_dash="dash",
    #                    annotation_text=f"Promedio de {fuente}: {promedio:.2f}%",
    #                    annotation_position="right",
    #                    line_color={"Radio": "blue", "Redes": "orange", "TV": "gray"}[fuente],
    #                )
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para las regiones seleccionadas después del filtrado")
    #    else:
    #        st.warning("No hay datos para mostrar")
    
    def section_22_ultimos_3_meses(self, global_filters: Dict[str, Any], mostrar_todos: bool):
       from sections.functions.grafico22 import data_section_22_ultimos_3_meses_sql
       
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
       
       # Convertir índice de mes a número (MESES_ES es 0-indexed, necesitamos 1-12)
       mes_fin_numero = MESES_ES.index(mes_fin) + 1
       
       # Llamar a la función SQL
       resultado = data_section_22_ultimos_3_meses_sql(
           ano_fin,
           mes_fin_numero,
           option_regiones,
           fuente
       )
       
       if not resultado.empty:
           # Verificar que tengamos meses únicos
           unique_months = resultado['mes'].unique()
           
           if len(unique_months) == 0:
               st.warning("No hay datos de meses disponibles para la selección actual.")
               return
           
           # Mapeo de colores por mes (del más antiguo al más reciente)
           if len(unique_months) >= 3:
               color_mapping = {
                   unique_months[0]: "lightblue",      # Mes más antiguo
                   unique_months[1]: "cornflowerblue", # Mes medio
                   unique_months[2]: "navy"            # Mes más reciente
               }
           elif len(unique_months) == 2:
               color_mapping = {
                   unique_months[0]: "lightblue",
                   unique_months[1]: "navy"
               }
           elif len(unique_months) == 1:
               color_mapping = {unique_months[0]: "navy"}
           else:
               color_mapping = {}
           
           # Crear el gráfico de barras agrupadas
           fig = px.bar(
               resultado,
               x="lugar",
               y="porcentaje_coctel",
               color="mes",
               barmode="group",
               title=f"Porcentaje de cóctel {fuente} - Últimos 3 meses (hasta {mes_fin} {ano_fin})",
               labels={
                   "lugar": "Región", 
                   "porcentaje_coctel": "Porcentaje de Cóctel (%)", 
                   "mes": "Mes"
               },
               color_discrete_map=color_mapping,
               text=resultado["porcentaje_coctel"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None
           )
           
           fig.update_layout(
               font=dict(size=15),
               xaxis_tickangle=-45
           )
           
           fig.update_traces(
               textposition="outside" if mostrar_todos else "none"
           )
           
           st.plotly_chart(fig, use_container_width=True)
           
           # Mostrar información adicional
           meses_mostrados = ', '.join(unique_months)
           st.caption(f"📊 Mostrando datos de: {meses_mostrados}")
           
       else:
           st.warning("No hay datos para mostrar en la selección actual.")
   
   
   

    #def section_22_ultimos_3_meses(self, global_filters: Dict[str, Any], mostrar_todos: bool):
    #    """22.- Porcentaje de cóctel en los últimos 3 meses por fuente"""
    #    st.subheader("22.- Porcentaje de cóctel en los últimos 3 meses por fuente")
    #    
    #    ano_actual = datetime.now().year
    #    anos = list(range(ano_actual - 9, ano_actual + 1))
    #    
    #    col1, col2 = st.columns(2)
    #    with col1:
    #        ano_fin = st.selectbox("Año de referencia", anos, index=len(anos)-1, key="ano_fin_s22")
    #        mes_fin = st.selectbox("Mes de referencia", MESES_ES, index=11, key="mes_fin_s22")
    #    with col2:
    #        fuente = st.selectbox("Fuente", ['Radio', 'Redes', 'TV'], key="fuente_s22")
    #    
    #    option_regiones = self.filter_manager.get_section_locations("s22", global_filters, multi=True)
    #    
    #    end_date = f"{ano_fin}-{MESES_ES.index(mes_fin) + 1:02d}-01"
    #    
    #    if not self.temp_coctel_fuente.empty:
    #        result = self.analytics.calculate_last_3_months_coctel(
    #            self.temp_coctel_fuente, end_date, fuente
    #        )
    #        
    #        if result.empty:
    #            st.warning("No hay datos para mostrar en el período seleccionado")
    #            return
    #        
    #        # Check if 'lugar' column exists before filtering
    #        if 'lugar' not in result.columns:
    #            st.error(f"❌ Column 'lugar' not found in result. Available columns: {result.columns.tolist()}")
    #            return
    #        
    #        # Filtrar por regiones seleccionadas
    #        result = result[result['lugar'].isin(option_regiones)]
    #        
    #        if result.empty:
    #            st.warning("No hay datos para mostrar en las regiones seleccionadas")
    #            return
    #        
    #        # Check if 'mes' column exists
    #        if 'mes' not in result.columns:
    #            st.error(f"❌ Column 'mes' not found in result. Available columns: {result.columns.tolist()}")
    #            return
    #        
    #        unique_months = result['mes'].unique()
    #        
    #        # Check if we have any months data
    #        if len(unique_months) == 0:
    #            st.warning("No hay datos de meses disponibles para la selección actual.")
    #            return
    #        
    #        if len(unique_months) >= 3:
    #            color_mapping = {
    #                unique_months[-3]: "lightblue",
    #                unique_months[-2]: "cornflowerblue",
    #                unique_months[-1]: "navy"
    #            }
    #        elif len(unique_months) == 2:
    #            color_mapping = {
    #                unique_months[-2]: "lightblue",
    #                unique_months[-1]: "navy"
    #            }
    #        elif len(unique_months) == 1:
    #            color_mapping = {unique_months[-1]: "navy"}
    #        
    #        fig = px.bar(
    #            result,
    #            x="lugar",
    #            y="porcentaje_coctel",
    #            color="mes",
    #            barmode="group",
    #            title=f"Porcentaje de cóctel {fuente} - Últimos 3 meses",
    #            labels={"lugar": "Región", "porcentaje_coctel": "Porcentaje de Cóctel", "mes": "Mes"},
    #            color_discrete_map=color_mapping,
    #            text=result["porcentaje_coctel"].map("{:.1f}%".format) if mostrar_todos else None
    #        )
    #        
    #        fig.update_layout(font=dict(size=15))
    #        fig.update_traces(textposition="outside" if mostrar_todos else "none")
    #        
    #        st.plotly_chart(fig, use_container_width=True)
    #    else:
    #        st.warning("No hay datos para mostrar")
    '''
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
    '''
    def section_23_evolucion_mensual(self, global_filters: Dict[str, Any], mostrar_todos: bool):
       """23.- Gráfico Mensual Lineal sobre la evolución de Radio, Redes y TV"""
       from sections.functions.grafico23 import data_section_23_evolucion_mensual_sql, data_section_23_add_total_line
       
       st.subheader("23.- Gráfico Mensual Lineal sobre la evolución de Radio, Redes y TV")
       
       # Selector de fechas por año/mes (IGUAL que gráfico 13)
       from datetime import datetime
       ano_actual = datetime.now().year
       anos = list(range(ano_actual - 9, ano_actual + 1))
       
       col1, col2 = st.columns(2)
       with col1:
           year_inicio = st.selectbox("Año de inicio", anos, len(anos)-1, key="year_inicio_s23")
           month_inicio = st.selectbox("Mes de inicio", list(range(1,13)), index=0, key="month_inicio_s23")
       with col2:
           year_fin = st.selectbox("Año de fin", anos, index=len(anos)-1, key="year_fin_s23")
           month_fin = st.selectbox("Mes de fin", list(range(1,13)), index=11, key="month_fin_s23")
       
       option_lugares = self.filter_manager.get_section_locations("s23", global_filters, multi=True)
       
       # Calcular rango de fechas (IGUAL que gráfico 13)
       fecha_inicio = pd.to_datetime(f'{year_inicio}-{month_inicio}-01')
       fecha_fin = pd.to_datetime(f'{year_fin}-{month_fin}-01') + pd.offsets.MonthEnd(1)
       
       # Usar la función SQL directa (como los otros gráficos)
       resultado_sql = data_section_23_evolucion_mensual_sql(
           fecha_inicio.strftime('%Y-%m-%d'),
           fecha_fin.strftime('%Y-%m-%d'),
           option_lugares
       )
       
       if not resultado_sql.empty:
           # Agregar línea Total
           combined_data = data_section_23_add_total_line(resultado_sql)
           
           if not combined_data.empty:
               # Crear el gráfico de LÍNEAS 
               fig = px.line(
                   combined_data,
                   x='año_mes',
                   y='coctel',
                   color='fuente',
                   markers=True,
                   color_discrete_map={
                       'Radio': 'gray', 
                       'Redes': 'red', 
                       'TV': 'blue', 
                       'Total': 'green'
                   },
                   title="Gráfico Mensual Lineal sobre la evolución de Radio, Redes y TV",
                   labels={
                       'año_mes': 'Año y Mes',
                       'coctel': 'Número de Cocteles',
                       'fuente': 'Fuente'
                   },
                   text=combined_data["coctel"].map(str) if mostrar_todos else None
               )
               
               fig.update_traces(textposition="top center")
               fig.update_layout(
                   xaxis_title="Año y Mes",
                   yaxis_title="Número de Cocteles",
                   xaxis=dict(tickangle=45, showgrid=False),
                   yaxis=dict(showgrid=True),
                   font=dict(size=12),
                   margin=dict(l=50, r=50, t=50, b=50)
               )
               
               st.plotly_chart(fig, use_container_width=True)
               
               # Mostrar información del rango
               st.write(f"Evolución mensual de coctel en {len(option_lugares)} regiones entre {fecha_inicio.strftime('%m/%Y')} y {fecha_fin.strftime('%m/%Y')}")
               st.dataframe(resultado_sql, hide_index=True)
               
           else:
               st.warning("No hay datos para mostrar el gráfico")
       else:
           st.warning("No hay datos para mostrar")
   
    def section_24_mensajes_fuerza(self, global_filters: Dict[str, Any], mostrar_todos: bool):
       from sections.functions.grafico24 import data_section_24_mensajes_fuerza_sql
       
       """24.- Porcentaje de cocteles por mensajes fuerza"""
       st.subheader("24.- Porcentaje de cocteles por mensajes fuerza")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s24", global_filters)
       
       col1, col2 = st.columns(2)
       with col1:
           option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s24")
       with col2:
           option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s24")
       
       # Convertir fechas a string formato YYYY-MM-DD
       fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
       fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
       
       # Llamar a la función SQL
       message_data = data_section_24_mensajes_fuerza_sql(
           fecha_inicio_str,
           fecha_fin_str,
           option_fuente,
           option_nota
       )
       
       if not message_data.empty:
           # Determinar título
           if option_nota == "Con coctel":
               titulo = "Porcentaje de notas por mensaje de fuerza con coctel"
           elif option_nota == "Sin coctel":
               titulo = "Porcentaje de notas por mensaje de fuerza sin coctel"
           else:
               titulo = "Porcentaje de notas por mensaje de fuerza"
           
           # Crear gráfico de barras horizontales
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
   
   
   
   
    #def section_24_mensajes_fuerza(self, global_filters: Dict[str, Any], mostrar_todos: bool):
    #    """24.- Porcentaje de cocteles por mensajes fuerza"""
    #    st.subheader("24.- Porcentaje de cocteles por mensajes fuerza")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s24", global_filters)
    #    
    #    col1, col2 = st.columns(2)
    #    with col1:
    #        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s24")
    #    with col2:
    #        option_nota = st.selectbox("Nota", ("Con coctel", "Sin coctel", "Todos"), key="nota_s24")
    #    
    #    temp_data = self.temp_coctel_completo[
    #        (self.temp_coctel_completo['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_completo['fecha_registro'] <= fecha_fin)
    #    ]
    #    
    #    if not temp_data.empty:
    #        message_data = self.analytics.calculate_coctel_by_message_force(temp_data, option_fuente, option_nota)
    #        
    #        if not message_data.empty:
    #            if option_nota == "Con coctel":
    #                titulo = "Porcentaje de notas por mensaje de fuerza con coctel"
    #            elif option_nota == "Sin coctel":
    #                titulo = "Porcentaje de notas por mensaje de fuerza sin coctel"
    #            else:
    #                titulo = "Porcentaje de notas por mensaje de fuerza"
    #            
    #            fig = px.bar(
    #                message_data,
    #                x='coctel',
    #                y='mensaje_fuerza',
    #                orientation='h',
    #                text=message_data.apply(lambda row: f"{row['coctel']} ({row['porcentaje']:.1f}%)", axis=1) if mostrar_todos else None,
    #                labels={'coctel': '', 'mensaje_fuerza': ''},
    #                title=titulo,
    #                color_discrete_sequence=['red']
    #            )
    #            
    #            fig.update_layout(font=dict(size=8))
    #            fig.update_traces(textposition="outside" if mostrar_todos else "none")
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para mostrar")
    #    else:
    #        st.warning("No hay datos para mostrar")
    

    def section_25_impactos_programa(self, global_filters: Dict[str, Any]):
       from sections.functions.grafico25 import data_section_25_impactos_programa_sql
       
       """25.- Impactos por programa"""
       st.subheader("25.- Impactos por programa")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s32", global_filters)
       
       col1, col2 = st.columns(2)
       with col1:
           medio = st.selectbox("Medio", ("Radio", "TV", "Redes"), key="medio_s32")
       with col2:
           # Obtener lugares disponibles
           available_locations_programas = self.temp_coctel_fuente_programas['lugar'].dropna().unique()
           available_locations_fb = self.temp_coctel_fuente_fb['lugar'].dropna().unique()
           all_locations = sorted(set(list(available_locations_programas) + list(available_locations_fb)))
           
           region = st.selectbox("Lugar", options=all_locations, key="lugar_s32")
       
       # Convertir fechas a string formato YYYY-MM-DD
       fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
       fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
       
       # Llamar a la función SQL
       result_coctel, result_total = data_section_25_impactos_programa_sql(
           fecha_inicio_str,
           fecha_fin_str,
           region,
           medio
       )
       
       # Mapeo de columnas para display
       if medio in ("Radio", "TV"):
           column_mapping = {"nombre_canal": "Canal", "programa_nombre": "Programa"}
       else:
           column_mapping = {"nombre_facebook_page": "Página Facebook"}
       
       # Mostrar resultados de impactos con cóctel
       if not result_coctel.empty:
           st.write("**Impactos con cóctel por programa**")
           st.dataframe(result_coctel.rename(columns=column_mapping), hide_index=True)
           
           # Mostrar totales
           if medio in ("Radio", "TV"):
               total_coctel = result_coctel['impactos_con_coctel'].sum()
               total_df_coctel = pd.DataFrame({
                   'nombre_canal': ['TOTAL'],
                   'programa_nombre': ['TOTAL'],
                   'impactos_con_coctel': [total_coctel]
               })
           else:
               total_coctel = result_coctel['impactos_con_coctel'].sum()
               total_df_coctel = pd.DataFrame({
                   'nombre_facebook_page': ['TOTAL'],
                   'impactos_con_coctel': [total_coctel]
               })
           
           st.dataframe(total_df_coctel.rename(columns=column_mapping), hide_index=True)
       else:
           st.warning("No hay impactos con cóctel para la selección actual.")
       
       st.markdown("---")
       
       # Mostrar resultados de total de impactos
       if not result_total.empty:
           st.write("**Total de impactos por programa**")
           st.dataframe(result_total.rename(columns=column_mapping), hide_index=True)
           
           # Mostrar totales
           if medio in ("Radio", "TV"):
               total_impactos = result_total['total_impactos'].sum()
               total_df_total = pd.DataFrame({
                   'nombre_canal': ['TOTAL'],
                   'programa_nombre': ['TOTAL'],
                   'total_impactos': [total_impactos]
               })
           else:
               total_impactos = result_total['total_impactos'].sum()
               total_df_total = pd.DataFrame({
                   'nombre_facebook_page': ['TOTAL'],
                   'total_impactos': [total_impactos]
               })
           
           st.dataframe(total_df_total.rename(columns=column_mapping), hide_index=True)
       else:
           st.warning("No hay datos de impactos totales para la selección actual.")
   
           
    #def section_25_impactos_programa(self, global_filters: Dict[str, Any]):
    #    """25.- Impactos por programa"""
    #    st.subheader("25.- Impactos por programa")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s32", global_filters)
    #    
    #    col1, col2 = st.columns(2)
    #    with col1:
    #        medio = st.selectbox("Medio", ("Radio", "TV", "Redes"), key="medio_s32")
    #    with col2:
    #        available_locations_programas = self.temp_coctel_fuente_programas['lugar'].dropna().unique()
    #        available_locations_fb = self.temp_coctel_fuente_fb['lugar'].dropna().unique()
    #        all_locations = sorted(set(list(available_locations_programas) + list(available_locations_fb)))
    #        
    #        region = st.selectbox("Lugar", options=all_locations, key="lugar_s32")
    #    
    #    if medio in ("Radio", "TV"):
    #        temp_data = self.temp_coctel_fuente_programas[
    #            (self.temp_coctel_fuente_programas["fecha_registro"] >= fecha_inicio) &
    #            (self.temp_coctel_fuente_programas["fecha_registro"] <= fecha_fin) &
    #            (self.temp_coctel_fuente_programas["lugar"] == region)
    #        ]
    #    else:
    #        fb_data_minimal = self.temp_coctel_fuente_fb[['id', 'fecha_registro', 'lugar', 'coctel', 'nombre_facebook_page']]
    #        
    #        temp_data = fb_data_minimal[
    #            (fb_data_minimal["fecha_registro"] >= fecha_inicio) &
    #            (fb_data_minimal["fecha_registro"] <= fecha_fin) &
    #            (fb_data_minimal["lugar"] == region)
    #        ].drop_duplicates()
    #        
    #        temp_data = temp_data[
    #            temp_data['nombre_facebook_page'].notna() &
    #            (temp_data['nombre_facebook_page'] != '')
    #        ]
    #    
    #    if not temp_data.empty:
    #        result_coctel, result_total = self.analytics.calculate_program_impacts_complete(temp_data, medio)
    #        
    #        # column mapping for display
    #        if medio in ("Radio", "TV"):
    #            column_mapping = {"nombre_canal": "Canal", "programa_nombre": "Programa"}
    #        else:
    #            column_mapping = {"nombre_facebook_page": "Página Facebook"}
    #            
    #        if not result_coctel.empty:
    #            st.write("**Impactos con cóctel por programa**")
    #            st.dataframe(result_coctel.rename(columns=column_mapping), hide_index=True)
    #            
    #            # show totals
    #            numeric_cols = result_coctel.select_dtypes(include=['number']).columns
    #            if len(numeric_cols) > 0:
    #                total_data = {}
    #                for col in result_coctel.columns:
    #                    if col in numeric_cols:
    #                        total_data[col] = [result_coctel[col].sum()]
    #                    else:
    #                        total_data[col] = ["TOTAL"]
    #                
    #                total_df_coctel = pd.DataFrame(total_data)
    #                st.dataframe(total_df_coctel.rename(columns=column_mapping), hide_index=True)
    #        else:
    #            st.warning("No hay impactos con cóctel para la selección actual.")
    #        
    #        st.write("")
    #        
    #        # show total impacts
    #        if not result_total.empty:
    #            st.write("**Total de impactos por programa**")
    #            st.dataframe(result_total.rename(columns=column_mapping), hide_index=True)
    #            
    #            # show totals
    #            numeric_cols = result_total.select_dtypes(include=['number']).columns
    #            if len(numeric_cols) > 0:
    #                total_data = {}
    #                for col in result_total.columns:
    #                    if col in numeric_cols:
    #                        total_data[col] = [result_total[col].sum()]
    #                    else:
    #                        total_data[col] = ["TOTAL"]
    #                
    #                total_df_total = pd.DataFrame(total_data)
    #                st.dataframe(total_df_total.rename(columns=column_mapping), hide_index=True)
    #        else:
    #            st.warning("No hay datos para la selección actual.")
    #    else:
    #        st.warning("No hay datos para la selección actual.")
    #
    
    
    #def section_26_distribucion_medio(self, global_filters: Dict[str, Any]):
    #    """26.- Distribución de cócteles por medio"""
    #    st.subheader("26.- Distribución de cócteles por medio")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s33", global_filters)
    #    
    #    temp_data = self.temp_coctel_fuente[
    #        (self.temp_coctel_fuente["fecha_registro"] >= fecha_inicio) &
    #        (self.temp_coctel_fuente["fecha_registro"] <= fecha_fin)
    #    ]
    #    
    #    if not temp_data.empty:
    #        conteo_data = self.analytics.calculate_coctel_distribution_by_media(temp_data)
    #        
    #        if not conteo_data.empty:
    #            fig = px.pie(
    #                conteo_data,
    #                values="count",
    #                names="Fuente",
    #                color="Fuente",
    #                color_discrete_map={
    #                    "Radio": "blue",
    #                    "Redes": "red",
    #                    "TV": "gray"
    #                },
    #                title="Distribución de cócteles por medio"
    #            )
    #            
    #            fig.update_traces(
    #                textposition="inside",
    #                textinfo="value+percent"
    #            )
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #        else:
    #            st.warning("No hay datos para la selección actual.")
    #    else:
    #        st.warning("No hay datos para la selección actual.")
    #    


    # Código para reemplazar en coctel_sections.py en la función section_26_distribucion_medio

    def section_26_distribucion_medio(self, global_filters: Dict[str, Any]):
       from sections.functions.grafico26 import data_section_26_distribucion_medio_sql
       
       """26.- Distribución de cócteles por medio"""
       st.subheader("26.- Distribución de cócteles por medio")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s33", global_filters)
       
       # Convertir fechas a string formato YYYY-MM-DD
       fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
       fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
       
       # Llamar a la función SQL
       conteo_data = data_section_26_distribucion_medio_sql(
           fecha_inicio_str,
           fecha_fin_str
       )
       
       if not conteo_data.empty:
           # Renombrar columna para el gráfico
           conteo_data = conteo_data.rename(columns={'fuente': 'Fuente'})
           
           # Crear gráfico PIE
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
           
           # Mostrar tabla con datos
           st.write("**Detalle por medio:**")
           total = conteo_data['count'].sum()
           conteo_data['Porcentaje'] = (conteo_data['count'] / total * 100).round(1).astype(str) + '%'
           conteo_data = conteo_data.rename(columns={'count': 'Total de Cócteles'})
           st.dataframe(conteo_data[['Fuente', 'Total de Cócteles', 'Porcentaje']], hide_index=True)
           
       else:
           st.warning("No hay datos para la selección actual.")

   # Código para reemplazar en coctel_sections.py en la función section_27_favor_contra_mensual

    def section_27_favor_contra_mensual(self, global_filters: Dict[str, Any]):
        from sections.functions.grafico27 import data_section_27_favor_contra_mensual_sql
        
        """27.- Notas a favor vs en contra por mes"""
        st.subheader("27.- Notas a favor vs en contra por mes")
    
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s34", global_filters)
    
        col1, col2 = st.columns(2)
        with col1:
            medio = st.selectbox("Medio", ("Radio", "TV", "Redes", "Todos"), key="medio_s34")
        with col2:
            regiones = self.filter_manager.get_section_locations("s34", global_filters, multi=True)
    
        # Convertir fechas a string
        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
        
        # Llamar a la función SQL
        long_data = data_section_27_favor_contra_mensual_sql(
            fecha_inicio_str,
            fecha_fin_str,
            regiones,
            medio
        )
    
        if not long_data.empty:
            # Crear gráfico de líneas
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
            
            # Mostrar tabla con datos
            st.write("**Detalle por mes:**")
            pivot_data = long_data.pivot(index='mes_str', columns='Tipo', values='Porcentaje').reset_index()
            pivot_data = pivot_data.rename(columns={'mes_str': 'Mes'})
            pivot_data['A favor (%)'] = pivot_data['A favor (%)'].round(1).astype(str) + '%'
            pivot_data['En contra (%)'] = pivot_data['En contra (%)'].round(1).astype(str) + '%'
            st.dataframe(pivot_data, hide_index=True)
            
        else:
            st.warning("No hay datos para la selección actual.")
            
                         
   # def section_27_favor_contra_mensual(self, global_filters: Dict[str, Any]):
   #     """27.- Notas a favor vs en contra"""
   #     st.subheader("27.- Notas a favor vs en contra")
#
   #     fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s34", global_filters)
#
   #     col1, col2 = st.columns(2)
   #     with col1:
   #         medio = st.selectbox("Medio", ("Radio", "TV", "Redes", "Todos"), key="medio_s34")
   #     with col2:
   #         regiones = self.filter_manager.get_section_locations("s34", global_filters, multi=True)
#
   #     temp_data = self.temp_coctel_fuente[
   #         (self.temp_coctel_fuente["fecha_registro"] >= fecha_inicio) &
   #         (self.temp_coctel_fuente["fecha_registro"] <= fecha_fin) &
   #         (self.temp_coctel_fuente["lugar"].isin(regiones))
   #     ]
#
   #     if not temp_data.empty:
   #         long_data = self.analytics.calculate_favor_vs_contra_monthly(temp_data, medio)
#
   #         if not long_data.empty:
   #             fig = px.line(
   #                 long_data,
   #                 x="mes_str",
   #                 y="Porcentaje",
   #                 color="Tipo",
   #                 markers=True,
   #                 color_discrete_map={
   #                     "A favor (%)": "blue",
   #                     "En contra (%)": "red"
   #                 },
   #                 labels={"mes_str": "Mes", "Porcentaje": "% sobre total", "Tipo": ""},
   #                 title="Notas a favor vs en contra por mes"
   #             )
#
   #             fig.update_traces(
   #                 text=long_data["Porcentaje"].round(0).astype(int).astype(str) + "%",
   #                 textposition="top center"
   #             )
   #             fig.update_layout(xaxis_tickangle=45)
#
   #             st.plotly_chart(fig, use_container_width=True)
   #         else:
   #             st.warning("No hay datos para la selección actual.")
   #     else:
   #         st.warning("No hay datos para la selección actual.")
        
    def section_2_posicion_por_fuente(self, global_filters: Dict[str, Any], mostrar_todos: bool):
       """2.- Posición por fuente en lugar y fecha específica - con tabla y porcentajes"""
       from sections.functions.grafico2 import (
           data_section_2_posiciones_coctel_sql, 
           preparar_datos_para_grafico
       )
       
       st.subheader("2.- Posición por fuente en lugar y fecha específica")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s2", global_filters)
       
       col1, col2 = st.columns(2)
       with col1:
           option_coctel = st.selectbox(
               "Notas", ("Con coctel", "Sin coctel", "Todas"), key="coctel_s2"
           )
       with col2:
           available_locations = self.lugares_uniques
           option_lugar = st.selectbox(
               "Lugar", 
               options=sorted(available_locations), 
               key="lugar_s2"
           )
       
       st.write(f"Posición por fuente en {option_lugar} entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
       
       # Obtener datos usando las funciones SQL de grafico2.py
       df_radio, df_tv, df_redes = data_section_2_posiciones_coctel_sql(
           fecha_inicio.strftime('%Y-%m-%d'),
           fecha_fin.strftime('%Y-%m-%d'),
           option_lugar
       )
       
       # Preparar datos combinados
       df_combinado = preparar_datos_para_grafico(df_radio, df_tv, df_redes)
       
       if not df_combinado.empty:
           # Filtrar por tipo de coctel si no es "Todas"
           if option_coctel == "Con coctel":
               df_filtrado = df_combinado[df_combinado['Tipo_Coctel'] == 'Con coctel'].copy()
           elif option_coctel == "Sin coctel":
               df_filtrado = df_combinado[df_combinado['Tipo_Coctel'] == 'Sin coctel'].copy()
           else:  # "Todas"
               # Agrupar sumando con coctel y sin coctel
               df_filtrado = df_combinado.groupby(['Posición', 'Medio'], as_index=False)['Cantidad'].sum()
           
           if not df_filtrado.empty:
               # Calcular porcentajes por Medio
               df_filtrado['Total_Medio'] = df_filtrado.groupby('Medio')['Cantidad'].transform('sum')
               df_filtrado['Porcentaje'] = (df_filtrado['Cantidad'] / df_filtrado['Total_Medio'] * 100).round(0).astype(int)
               df_filtrado['Porcentaje'] = df_filtrado['Porcentaje'].astype(str) + '%'
               
               # Mapeo de colores por posición
               color_map = {
                  'A favor': 'Azul',
                  'Potencialmente a favor': 'Celeste',
                  'Neutral': 'Gris',
                  'Potencialmente en contra': 'Naranja',
                  'En contra': 'Rojo'
              }
               
               # Si la posición contiene "Potencialmente", separar
               df_filtrado['Color'] = df_filtrado['Posición'].map(color_map)
               
               # Preparar para mostrar
               df_display = df_filtrado[['Medio', 'Posición', 'Color', 'Cantidad', 'Porcentaje']].copy()
               df_display.columns = ['Medio', 'Posicion', 'Color', 'Cantidad', 'Porcentaje']
               
               # Ordenar por Medio y luego por orden de posiciones
               posicion_orden = ['A favor', 'Potencialmente', 'Neutral', 'En contra']
               df_display['orden'] = df_display['Posicion'].apply(
                   lambda x: next((i for i, p in enumerate(posicion_orden) if p in x), 999)
               )
               df_display = df_display.sort_values(['Medio', 'orden']).drop('orden', axis=1)
               
               # Mostrar tabla
               st.dataframe(df_display, hide_index=True, width=600)
           else:
               st.warning("No hay datos para la selección actual")
       else:
           st.warning("No hay datos para mostrar")
#    def section_2_posicion_por_fuente(self, global_filters: Dict[str, Any], mostrar_todos: bool):
#        """2.- Posición por fuente en lugar y fecha específica"""
#        st.subheader("2.- Posición por fuente en lugar y fecha específica")
#        
#        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s2", global_filters)
#        
#        col1, col2 = st.columns(2)
#        with col1:
#            option_coctel = st.selectbox(
#                "Notas", ("Coctel noticias", "Otras fuentes", "Todas"), key="coctel_s2"
#            )
#        with col2:
#            # Local location selector - independent of global filters
#            available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
#            option_lugar = st.selectbox(
#                "Lugar", 
#                options=sorted(available_locations), 
#                key="lugar_s2"
#            )
#        
#        temp_data = self.temp_coctel_fuente[
#            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
#            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
#            (self.temp_coctel_fuente['lugar'] == option_lugar)
#        ]
#        
#        st.write(f"Posición por {option_coctel} en {option_lugar} entre {fecha_inicio.strftime('%d.%m.%Y')} y {fecha_fin.strftime('%d.%m.%Y')}")
#        
#        if not temp_data.empty:
#            result = self.analytics.calculate_position_by_source(temp_data, option_coctel)
#            if not result.empty:
#                st.dataframe(result, hide_index=True)
#            else:
#                st.warning("No hay datos para mostrar")
#        else:
#            st.warning("No hay datos para mostrar")
#    

     # REEMPLAZO PARA section_3_tendencia_semanal en coctel_sections.py
# Busca la función section_3_tendencia_semanal y reemplázala con esto:

    def section_3_tendencia_semanal(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """3.- Gráfico semanal por porcentaje de cocteles"""
        from sections.functions.grafico3 import data_section_3_tendencia_semanal_sql, calcular_viernes_semana
        
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
        
        # Usar la nueva función SQL
        weekly_data = data_section_3_tendencia_semanal_sql(
            fecha_inicio.strftime('%Y-%m-%d'),
            fecha_fin.strftime('%Y-%m-%d'),
            option_lugar,
            option_fuente
        )
        
        if not weekly_data.empty:
            # Calcular viernes de cada semana
            weekly_data = calcular_viernes_semana(weekly_data)
            
            # Crear eje X según toggle
            if usar_fechas_viernes:
                weekly_data["eje_x"] = weekly_data["viernes"].dt.strftime("%d-%m-%Y")
            else:
                weekly_data["eje_x"] = weekly_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                    (weekly_data["viernes"].dt.day - 1) // 7 + 1
                ).astype(str)
            
            # Crear gráfico
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
                tickangle=45,
                tickmode="array" if usar_fechas_viernes else "linear",
                tickvals=weekly_data["eje_x"] if usar_fechas_viernes else None,
                ticktext=weekly_data["eje_x"] if usar_fechas_viernes else None,
            )
            fig.update_yaxes(title_text="Porcentaje de cocteles %")
            fig.update_layout(title=f"Tendencia semanal - {option_fuente} en {option_lugar}")
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos suficientes para la tendencia semanal") 
#    def section_3_tendencia_semanal(self, global_filters: Dict[str, Any], mostrar_todos: bool):
#        """3.- Gráfico semanal por porcentaje de cocteles"""
#        st.subheader("3.- Gráfico semanal por porcentaje de cocteles en lugar y fecha específica")
#        
#        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s3", global_filters)
#        
#        col1, col2 = st.columns(2)
#        with col1:
#            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s3")
#        with col2:
#            # Local location selector - independent of global filters
#            available_locations = self.temp_coctel_fuente['lugar'].dropna().unique()
#            option_lugar = st.selectbox(
#                "Lugar", 
#                options=sorted(available_locations), 
#                key="lugar_s3"
#            )
#        
#        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s3")
#        
#        temp_data = self.temp_coctel_fuente[
#            (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
#            (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
#            (self.temp_coctel_fuente['lugar'] == option_lugar)
#        ]
#        
#        if not temp_data.empty:
#            weekly_data = self.analytics.calculate_weekly_percentage(temp_data, option_fuente)
#            
#            if not weekly_data.empty:
#                if usar_fechas_viernes:
#                    weekly_data["eje_x"] = weekly_data["viernes"].dt.strftime("%Y-%m-%d")
#                else:
#                    weekly_data["eje_x"] = weekly_data["fecha_registro"].dt.strftime("%Y-%m") + "-S" + (
#                        (weekly_data["fecha_registro"].dt.day - 1) // 7 + 1
#                    ).astype(str)
#                
#                fig = go.Figure()
#                fig.add_trace(
#                    go.Scatter(
#                        x=weekly_data["eje_x"],
#                        y=weekly_data["porcentaje"],
#                        mode="lines+markers+text" if mostrar_todos else "lines+markers",
#                        text=weekly_data["porcentaje"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
#                        textposition="top center",
#                        name="Porcentaje Coctel"
#                    )
#                )
#                
#                fig.update_xaxes(
#                    title_text="Fecha (Viernes)" if usar_fechas_viernes else "Semana",
#                    tickangle=45
#                )
#                fig.update_yaxes(title_text="Porcentaje de cocteles %")
#                fig.update_layout(title=f"Tendencia semanal - {option_fuente} en {option_lugar}")
#                
#                st.plotly_chart(fig, use_container_width=True)
#            else:
#                st.warning("No hay datos suficientes para la tendencia semanal")
#        else:
#            st.warning("No hay datos para mostrar")
    
#    def section_4_favor_vs_contra(self, global_filters: Dict[str, Any], mostrar_todos: bool):
#        """4.- Gráfico semanal de noticias a favor y en contra"""
#        st.subheader("4.- Gráfico semanal de noticias a favor y en contra en lugar y fecha específica")
#        
#        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s4", global_filters)
#        
#        col1, col2 = st.columns(2)
#        with col1:
#            # Local location selector - independent of global filters
#            available_locations = self.temp_coctel_fuente_notas['lugar'].dropna().unique()
#            option_lugar = st.selectbox(
#                "Lugar", 
#                options=sorted(available_locations), 
#                key="lugar_s4"
#            )
#        with col2:
#            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s4")
#        
#        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s4")
#        
#        temp_data = self.temp_coctel_fuente_notas[
#            (self.temp_coctel_fuente_notas['fecha_registro'] >= fecha_inicio) &
#            (self.temp_coctel_fuente_notas['fecha_registro'] <= fecha_fin) &
#            (self.temp_coctel_fuente_notas['lugar'] == option_lugar)
#        ]
#        
#        if not temp_data.empty:
#            weekly_data = self.analytics.calculate_weekly_favor_contra(temp_data, option_fuente)
#            
#            if not weekly_data.empty:
#                if usar_fechas_viernes:
#                    weekly_data["eje_x"] = weekly_data["viernes"].dt.strftime("%d-%m-%Y")
#                else:
#                    weekly_data["eje_x"] = weekly_data["fecha_registro"].dt.strftime("%Y-%m") + "-S" + (
#                        (weekly_data["fecha_registro"].dt.day - 1) // 7 + 1
#                    ).astype(str)
#                
#                fig = go.Figure()
#                fig.add_trace(
#                    go.Scatter(
#                        x=weekly_data["eje_x"],
#                        y=weekly_data["a_favor"],
#                        mode="lines+markers+text" if mostrar_todos else "lines+markers",
#                        name="A favor",
#                        text=weekly_data["a_favor"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
#                        textposition="top center",
#                        line=dict(color="blue"),
#                    )
#                )
#                
#                fig.add_trace(
#                    go.Scatter(
#                        x=weekly_data["eje_x"],
#                        y=weekly_data["en_contra"],
#                        mode="lines+markers+text" if mostrar_todos else "lines+markers",
#                        name="En contra",
#                        text=weekly_data["en_contra"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
#                        textposition="top center",
#                        line=dict(color="red"),
#                    )
#                )
#                
#                fig.update_xaxes(
#                    title_text="Fecha (Viernes)" if usar_fechas_viernes else "Semana",
#                    tickangle=45
#                )
#                fig.update_yaxes(title_text="Porcentaje de noticias %")
#                
#                st.plotly_chart(fig, use_container_width=True)
#            else:
#                st.warning("No hay datos suficientes")
#        else:
#            st.warning("No hay datos para mostrar")
    
    def section_4_favor_vs_contra(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """4.- Gráfico semanal de noticias a favor y en contra"""
        from sections.functions.grafico4 import data_section_4_favor_vs_contra_sql, calcular_viernes_semana
        
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
        
        # Usar la nueva función SQL
        weekly_data = data_section_4_favor_vs_contra_sql(
            fecha_inicio.strftime('%Y-%m-%d'),
            fecha_fin.strftime('%Y-%m-%d'),
            option_lugar,
            option_fuente
        )
        
        if not weekly_data.empty:
            # Calcular viernes de cada semana
            weekly_data = calcular_viernes_semana(weekly_data)
            
            # Crear eje X según toggle
            if usar_fechas_viernes:
                weekly_data["eje_x"] = weekly_data["viernes"].dt.strftime("%d-%m-%Y")
            else:
                weekly_data["eje_x"] = weekly_data["fecha_registro"].dt.strftime("%Y-%m") + "-S" + (
                    (weekly_data["fecha_registro"].dt.day - 1) // 7 + 1
                ).astype(str)
            
            # Crear gráfico con 2 líneas
            fig = go.Figure()
            
            # Línea A FAVOR (azul)
            fig.add_trace(
                go.Scatter(
                    x=weekly_data["eje_x"],
                    y=weekly_data["pct_a_favor"],
                    mode="lines+markers+text" if mostrar_todos else "lines+markers",
                    name="A favor",
                    text=weekly_data["pct_a_favor"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                    textposition="top center",
                    line=dict(color="blue")
                )
            )
            
            # Línea EN CONTRA (roja)
            fig.add_trace(
                go.Scatter(
                    x=weekly_data["eje_x"],
                    y=weekly_data["pct_en_contra"],
                    mode="lines+markers+text" if mostrar_todos else "lines+markers",
                    name="En contra",
                    text=weekly_data["pct_en_contra"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                    textposition="top center",
                    line=dict(color="red")
                )
            )
            
            fig.update_xaxes(
                title_text="Fecha (Viernes)" if usar_fechas_viernes else "Semana",
                tickangle=45,
                tickmode="array" if usar_fechas_viernes else "linear",
                tickvals=weekly_data["eje_x"] if usar_fechas_viernes else None,
                ticktext=weekly_data["eje_x"] if usar_fechas_viernes else None,
            )
            fig.update_yaxes(title_text="Porcentaje de notas %")
            fig.update_layout(
                title=f"Tendencia semanal A Favor vs En Contra - {option_fuente} en {option_lugar}",
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos suficientes para la tendencia semanal")
    
    
    # REEMPLAZO PARA section_5_grafico_acumulativo en coctel_sections.py

    def section_5_grafico_acumulativo(self, global_filters: Dict[str, Any], mostrar_todos: bool):
        """5.- Gráfico acumulativo porcentaje de cocteles"""
        from sections.functions.grafico5 import data_section_5_acumulativo_lugares_sql, calcular_viernes_semana
        
        st.subheader("5.- Gráfico acumulativo porcentaje de cocteles en lugar y fecha específica")
        
        fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s5", global_filters)
        
        col1, col2 = st.columns(2)
        with col1:
            option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s5")
        with col2:
            option_lugares = self.filter_manager.get_section_locations("s5", global_filters, multi=True)
        
        usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s5")
        
        # Usar la nueva función SQL
        cumulative_data = data_section_5_acumulativo_lugares_sql(
            fecha_inicio.strftime('%Y-%m-%d'),
            fecha_fin.strftime('%Y-%m-%d'),
            option_lugares,
            option_fuente
        )
        
        if not cumulative_data.empty:
            # Calcular viernes de cada semana
            cumulative_data = calcular_viernes_semana(cumulative_data)
            
            # Crear eje X según toggle
            if usar_fechas_viernes:
                cumulative_data["eje_x"] = cumulative_data["viernes"].dt.strftime("%d-%m-%Y")
            else:
                cumulative_data["eje_x"] = cumulative_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                    (cumulative_data["viernes"].dt.day - 1) // 7 + 1
                ).astype(str)
            
            # Crear gráfico con múltiples líneas (una por lugar)
            fig = px.line(
                cumulative_data,
                x="eje_x",
                y="porcentaje",
                color="lugar",
                title="Porcentaje de cocteles por semana %",
                labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", 
                        "porcentaje": "Porcentaje de cocteles %",
                        "lugar": "Lugar"},
                markers=True,
                text=cumulative_data["porcentaje"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
            )
            
            fig.update_traces(textposition="top center")
            fig.update_xaxes(tickangle=45)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Mostrar tabla resumen de la última semana
            st.write(f"Porcentaje de cocteles por lugar en la última semana")
            last_week = cumulative_data.sort_values("semana").groupby("lugar").last().reset_index()
            last_week['porcentaje'] = last_week['porcentaje'].map(lambda x: f"{x:.1f}")
            last_week = last_week[["lugar", "porcentaje"]].rename(columns={"porcentaje": "pct_cocteles"})
            st.dataframe(last_week, hide_index=True)
        else:
            st.warning("No hay datos suficientes")


    #def section_5_grafico_acumulativo(self, global_filters: Dict[str, Any], mostrar_todos: bool):
    #    """5.- Gráfico acumulativo porcentaje de cocteles"""
    #    st.subheader("5.- Gráfico acumulativo porcentaje de cocteles en lugar y fecha específica")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s5", global_filters)
    #    
    #    col1, col2 = st.columns(2)
    #    with col1:
    #        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes", "Todos"), key="fuente_s5")
    #    with col2:
    #        option_lugares = self.filter_manager.get_section_locations("s5", global_filters, multi=True)
    #    
    #    usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s5")
    #    
    #    temp_data = self.temp_coctel_fuente[
    #        (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente['lugar'].isin(option_lugares))
    #    ]
    #    
    #    if not temp_data.empty:
    #        cumulative_data = self.analytics.calculate_cumulative_percentage(temp_data, option_fuente)
    #        
    #        if not cumulative_data.empty:
    #            if usar_fechas_viernes:
    #                cumulative_data["eje_x"] = cumulative_data["viernes"].dt.strftime("%d-%m-%Y")
    #            else:
    #                cumulative_data["eje_x"] = cumulative_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
    #                    (cumulative_data["viernes"].dt.day - 1) // 7 + 1
    #                ).astype(str)
    #            
    #            fig = px.line(
    #                cumulative_data,
    #                x="eje_x",
    #                y="coctel_mean",
    #                color="lugar",
    #                title="Porcentaje de cocteles por semana %",
    #                labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", "coctel_mean": "Porcentaje de cocteles %"},
    #                markers=True,
    #                text=cumulative_data["coctel_mean"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
    #            )
    #            
    #            fig.update_traces(textposition="top center")
    #            fig.update_xaxes(tickangle=45)
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #            
    #            # Mostrar tabla resumen
    #            st.write(f"Porcentaje de cocteles por lugar en la última semana")
    #            last_week = cumulative_data.sort_values("semana").groupby("lugar").last().reset_index()
    #            last_week['coctel_mean'] = last_week['coctel_mean'].map(lambda x: f"{x:.1f}")
    #            last_week = last_week[["lugar", "coctel_mean"]].rename(columns={"coctel_mean": "pct_cocteles"})
    #            st.dataframe(last_week, hide_index=True)
    #        else:
    #            st.warning("No hay datos suficientes")
    #    else:
    #        st.warning("No hay datos para mostrar")
    

    def section_top3_mejores_lugares(self, global_filters: Dict[str, Any], mostrar_todos: bool):
       """Top 3 mejores porcentajes de coctel semanal por lugar"""
       from sections.functions.grafico_top3 import data_section_top3_lugares_sql, calcular_viernes_semana
       
       st.subheader("Top 3 mejores porcentajes de coctel semanal por lugar en fuente y fecha específica")
       
       fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("stop3", global_filters)
       
       option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_stop3")
       usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_stop3")
       
       # Usar la nueva función SQL que automáticamente selecciona TOP 3
       top_data, top_lugares_list = data_section_top3_lugares_sql(
           fecha_inicio.strftime('%Y-%m-%d'),
           fecha_fin.strftime('%Y-%m-%d'),
           option_fuente,
           top_n=3
       )
       
       if not top_data.empty:
           # Calcular viernes de cada semana
           top_data = calcular_viernes_semana(top_data)
           
           # Crear eje X según toggle
           if usar_fechas_viernes:
               top_data["eje_x"] = top_data["viernes"].dt.strftime("%d-%m-%Y")
           else:
               top_data["eje_x"] = top_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                   (top_data["viernes"].dt.day - 1) // 7 + 1
               ).astype(str)
           
           # Crear gráfico con las 3 líneas (top 3 lugares)
           fig = px.line(
               top_data,
               x="eje_x",
               y="porcentaje",
               color="lugar",
               title="Top 3 lugares con mayor porcentaje de cocteles",
               labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", 
                       "porcentaje": "Porcentaje de cocteles %",
                       "lugar": "Lugar"},
               markers=True,
               text=top_data["porcentaje"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
           )
           
           fig.update_traces(textposition="top center")
           fig.update_xaxes(tickangle=45)
           
           st.plotly_chart(fig, use_container_width=True)
           
           # Mostrar tabla de top 3 lugares
           st.write(f"Top 3 lugares con mayor porcentaje de cocteles según {option_fuente}")
           top_summary = top_data.sort_values("semana").groupby("lugar").last().reset_index()
           top_summary['porcentaje'] = top_summary['porcentaje'].map(lambda x: f"{x:.1f}")
           st.dataframe(top_summary[["lugar", "porcentaje"]], hide_index=True)
       else:
           st.warning("No hay datos suficientes")
           
    #def section_top3_mejores_lugares(self, global_filters: Dict[str, Any], mostrar_todos: bool):
    #    """Top 3 mejores porcentajes de coctel semanal por lugar"""
    #    st.subheader("Top 3 mejores porcentajes de coctel semanal por lugar en fuente y fecha específica")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("stop3", global_filters)
    #    
    #    option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_stop3")
    #    usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_stop3")
    #    
    #    temp_data = self.temp_coctel_fuente[
    #        (self.temp_coctel_fuente['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente['fecha_registro'] <= fecha_fin)
    #    ]
    #    
    #    if not temp_data.empty:
    #        top_data, top_lugares_list = self.analytics.calculate_top_lugares(temp_data, option_fuente, 3)
    #        
    #        if not top_data.empty:
    #            if usar_fechas_viernes:
    #                top_data["eje_x"] = top_data["viernes"].dt.strftime("%d-%m-%Y")
    #            else:
    #                top_data["eje_x"] = top_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
    #                    (top_data["viernes"].dt.day - 1) // 7 + 1
    #                ).astype(str)
    #            
    #            fig = px.line(
    #                top_data,
    #                x="eje_x",
    #                y="coctel",
    #                color="lugar",
    #                title="Top 3 lugares con mayor porcentaje de cocteles",
    #                labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", "coctel": "Porcentaje de cocteles %"},
    #                markers=True,
    #                text=top_data["coctel"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
    #            )
    #            
    #            fig.update_traces(textposition="top center")
    #            fig.update_xaxes(tickangle=45)
    #            
    #            st.plotly_chart(fig, use_container_width=True)
    #            
    #            # Mostrar tabla de top lugares
    #            st.write(f"Top 3 lugares con mayor porcentaje de cocteles según {option_fuente}")
    #            top_summary = top_data.sort_values("semana").groupby("lugar").last().reset_index()
    #            top_summary['coctel'] = top_summary['coctel'].map(lambda x: f"{x:.1f}")
    #            st.dataframe(top_summary[["lugar", "coctel"]], hide_index=True)
    #        else:
    #            st.warning("No hay datos suficientes")
    #    else:
    #        st.warning("No hay datos para mostrar")

    def section_6_top_medios(self, global_filters: Dict[str, Any], mostrar_todos: bool):
      """6.- Top 3 mejores radios, redes, tv"""
      from sections.functions.grafico6 import data_section_6_top_medios_sql, calcular_viernes_semana
      
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
      
      # Usar la nueva función SQL que automáticamente selecciona TOP 3 medios
      top_data = data_section_6_top_medios_sql(
          fecha_inicio.strftime('%Y-%m-%d'),
          fecha_fin.strftime('%Y-%m-%d'),
          option_lugar,
          option_fuente,
          top_n=3
      )
      
      if not top_data.empty:
          # Calcular viernes de cada semana
          top_data = calcular_viernes_semana(top_data)
          
          # Crear eje X según toggle
          if usar_fechas_viernes:
              top_data["eje_x"] = top_data["viernes"].dt.strftime("%d-%m-%Y")
          else:
              top_data["eje_x"] = top_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                  (top_data["viernes"].dt.day - 1) // 7 + 1
              ).astype(str)
          
          # Crear gráfico con las 3 líneas (top 3 medios)
          fig = px.line(
              top_data,
              x="eje_x",
              y="porcentaje",
              color="nombre_medio",
              title=f"Top 3 {option_fuente.lower()} con mayor porcentaje de cocteles",
              labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", 
                      "porcentaje": "Porcentaje de cocteles %",
                      "nombre_medio": "Medio"},
              markers=True,
              text=top_data["porcentaje"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
          )
          
          fig.update_traces(textposition="top center")
          fig.update_xaxes(tickangle=45)
          
          st.plotly_chart(fig, use_container_width=True)
      else:
          st.warning("No hay datos para mostrar")
              
    #def section_6_top_medios(self, global_filters: Dict[str, Any], mostrar_todos: bool):
    #    """6.- Top 3 mejores radios, redes, tv"""
    #    st.subheader("6.- Top 3 mejores radios, redes, tv en lugar y fecha específica")
    #    
    #    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("s6", global_filters)
    #    
    #    col1, col2 = st.columns(2)
    #    with col1:
    #        option_fuente = st.selectbox("Fuente", ("Radio", "TV", "Redes"), key="fuente_s6")
    #    with col2:
    #        # Local location selector - independent of global filters
    #        # Get available locations from both dataframes
    #        available_locations_programas = self.temp_coctel_fuente_programas['lugar'].dropna().unique()
    #        available_locations_fb = self.temp_coctel_fuente_fb['lugar'].dropna().unique()
    #        all_locations = sorted(set(list(available_locations_programas) + list(available_locations_fb)))
    #        
    #        option_lugar = st.selectbox(
    #            "Lugar", 
    #            options=all_locations, 
    #            key="lugar_s6"
    #        )
    #    
    #    usar_fechas_viernes = st.toggle("Mostrar fechas (Viernes de cada semana)", key="toggle_s6")
    #    
    #    temp_programas = self.temp_coctel_fuente_programas[
    #        (self.temp_coctel_fuente_programas['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente_programas['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente_programas['lugar'] == option_lugar)
    #    ]
    #    
    #    temp_fb = self.temp_coctel_fuente_fb[
    #        (self.temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio) &
    #        (self.temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin) &
    #        (self.temp_coctel_fuente_fb['lugar'] == option_lugar)
    #    ]
    #    
    #    top_data = self.analytics.calculate_top_medios(temp_programas, temp_fb, option_fuente, 3)
    #    
    #    if not top_data.empty:
    #        if usar_fechas_viernes:
    #            top_data["eje_x"] = top_data["viernes"].dt.strftime("%d-%m-%Y")
    #        else:
    #            top_data["eje_x"] = top_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
    #                (top_data["viernes"].dt.day - 1) // 7 + 1
    #            ).astype(str)
    #        
    #        fig = px.line(
    #            top_data,
    #            x="eje_x",
    #            y="coctel",
    #            color="nombre_medio",
    #            title=f"Top 3 {option_fuente.lower()} con mayor porcentaje de cocteles",
    #            labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", "coctel": "Porcentaje de cocteles %"},
    #            markers=True,
    #            text=top_data["coctel"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
    #        )
    #        
    #        fig.update_traces(textposition="top center")
    #        fig.update_xaxes(tickangle=45)
    #        
    #        st.plotly_chart(fig, use_container_width=True)
    #    else:
    #        st.warning("No hay datos para mostrar")
    #
    def section_7_macroregion(self, global_filters: Dict[str, Any], mostrar_todos: bool):
       """7.- Crecimiento de cocteles por macroregión"""
       from sections.functions.grafico7 import data_section_7_macroregion_sql, calcular_viernes_semana
       
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
       
       # Usar la nueva función SQL
       macro_data = data_section_7_macroregion_sql(
           fecha_inicio.strftime('%Y-%m-%d'),
           fecha_fin.strftime('%Y-%m-%d'),
           option_macroregion,
           option_fuente
       )
       
       if not macro_data.empty:
           # Calcular viernes de cada semana
           macro_data = calcular_viernes_semana(macro_data)
           
           # Crear eje X según toggle
           if usar_fechas_viernes:
               macro_data["eje_x"] = macro_data["viernes"].dt.strftime("%d-%m-%Y")
           else:
               macro_data["eje_x"] = macro_data["viernes"].dt.strftime("%Y-%m") + "-S" + (
                   (macro_data["viernes"].dt.day - 1) // 7 + 1
               ).astype(str)
           
           # Crear gráfico con múltiples líneas (una por cada lugar de la macroregión)
           fig = px.line(
               macro_data,
               x="eje_x",
               y="porcentaje",
               color="lugar",
               title=f"Crecimiento de cocteles por macroregión en {option_macroregion}",
               labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes else "Semana", 
                       "porcentaje": "Porcentaje de cocteles %",
                       "lugar": "Lugar"},
               markers=True,
               text=macro_data["porcentaje"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
           )
           
           fig.update_traces(textposition="top center")
           fig.update_xaxes(tickangle=45)
           
           st.plotly_chart(fig, use_container_width=True)
           st.write("Nota: Los valores muestran el porcentaje de cocteles en cada semana")
       else:
           st.warning("No hay datos suficientes")
    # --- Agregar en la zona de imports (al inicio del archivo) ---
    
    
    # --- Agregar la función de renderizado ---
    def section_28_registros_usuarios(self):
        """
        Renderiza el Gráfico 28: Reporte Mensual de Productividad
        Muestra dos tablas:
        1. Notas CON Coctel (id_nota != NULL)
        2. Notas SIN Coctel (id_nota == NULL)
        """
        st.markdown("## 📊 Gráfico 28: Productividad Mensual de Usuarios")
        
        from sections.functions.grafico28 import obtener_data_grafico28
        
        # Obtenemos los dos DataFrames
        df_con_coctel, df_sin_coctel = obtener_data_grafico28()
        
        if df_con_coctel.empty and df_sin_coctel.empty:
            st.warning("⚠️ No se encontraron datos de productividad en los últimos 12 meses.")
            return
    
        # --- TABLA 1: CON COCTEL ---
        st.markdown("### 🍸 Notas CON Coctel (Registradas)")
        st.markdown("Cantidad de notas asociadas a un coctel, desglosadas por usuario y región.")
        
        if not df_con_coctel.empty:
            st.dataframe(
                df_con_coctel, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Nombre del usuario": st.column_config.TextColumn("Usuario", width="medium"),
                    "Región": st.column_config.TextColumn("Región", width="medium"),
                }
            )
        else:
            st.info("No hay registros con coctel en este periodo.")
            
        st.markdown("---")
    
        # --- TABLA 2: SIN COCTEL ---
        st.markdown("### 📝 Notas SIN Coctel (Acontecimientos)")
        st.markdown("Cantidad de acontecimientos sin nota asociada, desglosados por usuario y región.")
        
        if not df_sin_coctel.empty:
            st.dataframe(
                df_sin_coctel, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Nombre del usuario": st.column_config.TextColumn("Usuario", width="medium"),
                    "Región": st.column_config.TextColumn("Región", width="medium"),
                }
            )
        else:
            st.info("No hay registros sin coctel en este periodo.")
    def render_single_section(self, section_code: str, global_filters: Dict[str, Any], mostrar_todos: bool = True):
        """Renderizar una sección específica basada en su código"""
        
        # Mapeo de códigos a métodos
        section_map = {
            "sn": lambda: self.section_sn_proporcion_basica(global_filters, mostrar_todos),
            "1": lambda: self.section_1_proporcion_combinada(global_filters, mostrar_todos),
            "2": lambda: self.section_2_posicion_por_fuente(global_filters, mostrar_todos),
            "3": lambda: self.section_3_tendencia_semanal(global_filters, mostrar_todos),
            "4": lambda: self.section_4_favor_vs_contra(global_filters, mostrar_todos),
            "5": lambda: self.section_5_grafico_acumulativo(global_filters, mostrar_todos),
            "top3": lambda: self.section_top3_mejores_lugares(global_filters, mostrar_todos),
            "6": lambda: self.section_6_top_medios(global_filters, mostrar_todos),
            "7": lambda: self.section_7_macroregion(global_filters, mostrar_todos),
            "8": lambda: self.section_8_conteo_posiciones(global_filters, mostrar_todos),
            "9": lambda: self.section_9_distribucion_posiciones(global_filters, mostrar_todos),
            "10": lambda: self.section_10_eventos_coctel(global_filters, mostrar_todos),
            "11": lambda: self.section_11_cocteles_fuente_lugar(global_filters),
            "12": lambda: self.section_12_medios_generan_coctel(global_filters),
            "13": lambda: self.section_13_conteo_mensual(global_filters),
            "14": lambda: self.section_14_notas_favor_contra(global_filters, mostrar_todos),
            "15": lambda: self.section_15_proporcion_mensajes(global_filters),
            "16": lambda: self.section_16_mensajes_por_tema(global_filters),
            "17": lambda: self.section_17_proporcion_por_tema(global_filters, mostrar_todos),
            "18": lambda: self.section_18_tendencia_por_medio(global_filters),
            "19": lambda: self.section_19_notas_tiempo_posicion(global_filters),
            "20": lambda: self.section_20_actores_posiciones(global_filters),
            "21": lambda: self.section_21_porcentaje_medios(global_filters, mostrar_todos),
            "22": lambda: self.section_22_ultimos_3_meses(global_filters, mostrar_todos),
            "23": lambda: self.section_23_evolucion_mensual(global_filters, mostrar_todos),
            "24": lambda: self.section_24_mensajes_fuerza(global_filters, mostrar_todos),
            "25": lambda: self.section_25_impactos_programa(global_filters),
            "26": lambda: self.section_26_distribucion_medio(global_filters),
            "27": lambda: self.section_27_favor_contra_mensual(global_filters),
            "28": lambda: self.section_28_registros_usuarios(), # <--- AGREGADO
        }
        
        # Ejecutar la sección seleccionada
        if section_code in section_map:
            section_map[section_code]()
        else:
            st.error(f"❌ Sección '{section_code}' no encontrada")        