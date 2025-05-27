import pandas as pd
import numpy as np
import streamlit as st
import time
from utils import get_query
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd

#%% Carga de Base de Datos
@st.cache_data(ttl=3600)
def cargar_coctel_completo():
    print("⏳ [START] cargar_coctel_completo()", flush=True)
    t0 = time.time()
    # temp_coctel_completo = pd.read_parquet('app/tables/temp_coctel_completo.parquet')
    temp_coctel_completo = get_query("cocteles","coctel_completo")
    temp_coctel_completo['fecha_registro'] = pd.to_datetime(temp_coctel_completo['fecha_registro'])
    temp_coctel_completo['coctel'] = pd.to_numeric(temp_coctel_completo['coctel'], errors='coerce').fillna(0.0)
    temp_coctel_completo = temp_coctel_completo[~temp_coctel_completo["coctel"].isin([5.0, 15.0])]
    temp_coctel_completo = temp_coctel_completo[temp_coctel_completo["acontecimiento"] != "pRUEBA"]
    temp_coctel_completo['id_fuente'] = temp_coctel_completo['id_fuente'].fillna(3)
    temp_coctel_fuente = temp_coctel_completo[['id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
    'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal',
    'canal_nombre']].copy()
    temp_coctel_fuente_programas = temp_coctel_fuente.copy()
    temp_coctel_fuente_fb = temp_coctel_completo[['id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
        'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal',
        'canal_nombre', 'num_reacciones', 'num_comentarios', 'num_compartidos',
        'fecha_post', 'nombre_facebook_page']].copy()
    temp_coctel_fuente_actores = temp_coctel_completo[['id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
        'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal','canal_nombre', 'nombre']].copy()
    temp_coctel_temas = temp_coctel_completo[['id', 'fecha_registro', 'acontecimiento', 'coctel', 'id_posicion',
        'lugar', 'color', 'id_fuente', 'fuente_nombre', 'id_canal',
        'canal_nombre', 'descripcion']].copy()

    temp_coctel_fuente_programas['nombre_canal'] = temp_coctel_fuente_programas['canal_nombre']
    temp_coctel_fuente_fb['nombre_canal'] = temp_coctel_fuente_fb['canal_nombre']

    lugares_uniques = temp_coctel_fuente['lugar'].unique().tolist()

    t1 = time.time()
    print(f"✅ [END]   cargar_coctel_completo() ({t1-t0:.1f}s)", flush=True)
    return temp_coctel_completo, temp_coctel_fuente, temp_coctel_fuente_programas, temp_coctel_fuente_fb, temp_coctel_fuente_actores, temp_coctel_temas, lugares_uniques

#%%% Funcion 1
def coctel_dashboard():
    st.title("Análisis de Cocteles")
    
    temp_coctel_completo, temp_coctel_fuente, temp_coctel_fuente_programas, temp_coctel_fuente_fb, temp_coctel_fuente_actores, temp_coctel_temas, lugares_uniques = cargar_coctel_completo()

    #%% diccionarios

    id_posicion_dict = {1: 'a favor',
                        2: 'potencialmente a favor',
                        3: 'neutral',
                        4: 'potencialmente en contra',
                        5: 'en contra'}
    coctel_dict = {0: 'Sin coctel',
                1: 'Con coctel'
                }
    id_fuente_dict = {1: 'Radio',
                    2: 'TV',
                    3: 'Redes'
                    }
    color_posicion_dict = {"a favor": "blue",
                        "potencialmente a favor": "lightblue",
                        "neutral": "gray",
                        "potencialmente en contra": "#FFA500",
                        "en contra": "red"
                        }
    color_discrete_map = {'Celeste': 'lightblue',
                            'Rojo': 'Red',     
                            'Naranja': '#FFA500',
                            'Gris': 'Gray',
                            'Azul': 'Blue',
                        }
    mostrar_todos = st.checkbox("Mostrar todos los porcentajes", value=True)

    ano_actual = datetime.now().year
    anos = list(range(ano_actual - 9, ano_actual + 1))  # Últimos 10 años
    ano = datetime.now().year
    anos = list(range(ano_actual - 9, ano_actual + 1))  # Últimos 10 años
    meses = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]

    #%% 1.- PROPORCION COCTELES solo fechas y lugar
    st.subheader("sn.- Proporción de cocteles en lugar y fecha específica")
    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_c1 = st.date_input(
        "Fecha Inicio",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_c1 = st.date_input(
        "Fecha Fin",
        format="DD.MM.YYYY")
    with col3:
        option_lugar_c1 = st.selectbox(
        "Lugar",
        lugares_uniques)

    fecha_inicio_c1 = pd.to_datetime(fecha_inicio_c1,format='%Y-%m-%d')
    fecha_fin_c1 = pd.to_datetime(fecha_fin_c1,format='%Y-%m-%d')

    temp_c1 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_c1)&
                                (temp_coctel_fuente['fecha_registro']<=fecha_fin_c1)&
                                (temp_coctel_fuente['lugar']==option_lugar_c1)]

    temp_c1["Fuente"] = temp_c1["id_fuente"].map(id_fuente_dict)

    def proporcion_cocteles(df):
        df = df.rename(columns={'coctel':'Fuente','id':'Cantidad'})
        df['Proporción'] = df['Cantidad'] / df['Cantidad'].sum()
        df['Proporción'] = df['Proporción'].map('{:.0%}'.format)
        df['Fuente'] = df['Fuente'].replace({0:'Otras Fuentes',1:'Coctel Noticias'})
        return df

    if not temp_c1.empty:
        temp_c1_radio = temp_c1[temp_c1['id_fuente']==1].groupby('coctel').agg({'id':'count'}).reset_index()
        temp_c1_radio = proporcion_cocteles(temp_c1_radio)

        temp_c1_tv = temp_c1[temp_c1['id_fuente']==2].groupby('coctel').agg({'id':'count'}).reset_index()
        temp_c1_tv = proporcion_cocteles(temp_c1_tv)

        temp_c1_redes = temp_c1[temp_c1['id_fuente']==3].groupby('coctel').agg({'id':'count'}).reset_index()
        temp_c1_redes = proporcion_cocteles(temp_c1_redes)
        st.write(f"Proporción de cocteles en {option_lugar_c1} entre {fecha_inicio_c1.strftime('%d.%m.%Y')} y {fecha_fin_c1.strftime('%d.%m.%Y')}")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.write("Radio")
            st.dataframe(temp_c1_radio, hide_index=True)

        with col2:
            st.write("TV")
            st.dataframe(temp_c1_tv, hide_index=True)

        with col3:
            st.write("Redes")
            st.dataframe(temp_c1_redes, hide_index=True)

    else:
        st.warning("No hay datos para mostrar")
    #%% 1.- PROPORCION COCTELES

    st.subheader("1.- Proporción de cocteles en lugar, fuentes y fechas específicas")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_t1 = st.date_input(
        "Fecha Inicio g1",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_t1 = st.date_input(
        "Fecha Fin g1",
        format="DD.MM.YYYY")
    with col3:
        option_fuente_t1 = st.multiselect(
        "Fuente g1",
        ["Radio", "TV", "Redes"], ["Radio", "TV", "Redes"])

    option_lugar_t1 = st.multiselect("Lugar g1",
                                    lugares_uniques,
                                    lugares_uniques
                                    )

    fecha_inicio_t1 = pd.to_datetime(fecha_inicio_t1,format='%Y-%m-%d')
    fecha_fin_t1 = pd.to_datetime(fecha_fin_t1,format='%Y-%m-%d')

    temp_t1 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_t1)&
                                (temp_coctel_fuente['fecha_registro']<=fecha_fin_t1)&
                            (temp_coctel_fuente['lugar'].isin(option_lugar_t1))]

    temp_t1["Fuente"] = temp_t1["id_fuente"].map(id_fuente_dict)

    if option_fuente_t1:
        temp_t1 = temp_t1[temp_t1['Fuente'].isin(option_fuente_t1)]

    temp_t1 = temp_t1.groupby('coctel').agg({'id':'count'}).reset_index()

    st.write(f"Proporción de cocteles en {', '.join(option_lugar_t1)} entre {fecha_inicio_t1.strftime('%d.%m.%Y')} y {fecha_fin_t1.strftime('%d.%m.%Y')}")
    if not temp_t1.empty:
        temp_t1 = temp_t1.rename(columns={'coctel':'Fuente','id':'Cantidad'})
        temp_t1['Proporción'] = temp_t1['Cantidad'] / temp_t1['Cantidad'].sum()
        temp_t1['Proporción'] = temp_t1['Proporción'].map('{:.0%}'.format)
        temp_t1['Fuente'] = temp_t1['Fuente'].replace({0:'Otras Fuentes',1:'Coctel Noticias'})
        st.dataframe(temp_t1, hide_index=True)
    else:
        st.warning("No hay datos para mostrar")

    #%% 2.- Posición por fuente

    st.subheader("2.- Posición por fuente en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_t2 = st.date_input(
        "Fecha Inicio g2",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_t2 = st.date_input(
        "Fecha Fin g2",
        format="DD.MM.YYYY")
    with col3:
        option_coctel_t2 = st.selectbox(
        "Notas g2",
        ("Coctel noticias", "Otras fuentes", "Todas"),)
    with col4:
        option_lugar_t2 = st.selectbox(
        "Lugar g2",
        lugares_uniques)

    fecha_inicio_t2 = pd.to_datetime(fecha_inicio_t2,format='%Y-%m-%d')
    fecha_fin_t2 = pd.to_datetime(fecha_fin_t2,format='%Y-%m-%d')

    temp_t2 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_t2)&(temp_coctel_fuente['fecha_registro']<=fecha_fin_t2)&
                            (temp_coctel_fuente['lugar']==option_lugar_t2)]
    temp_colores = pd.DataFrame({'id_fuente': [1,1,1,1,1,2,2,2,2,2,3,3,3,3,3], 
                                'Posicion': ['(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente','(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente','(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente'],
                                'color': ['Azul','Rojo','Gris','Celeste','Naranja','Azul','Rojo','Gris','Celeste','Naranja','Azul','Rojo','Gris','Celeste','Naranja']})
    if option_coctel_t2 == 'Coctel noticias':
        temp_t2 = temp_t2[temp_t2['coctel']==1].groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()
    elif option_coctel_t2 == 'Otras fuentes':
        temp_t2 = temp_t2[temp_t2['coctel']==0].groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()
    else:
        temp_t2 = temp_t2.groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()

    st.write(f"Posición por {option_coctel_t2} en {option_lugar_t2} entre {fecha_inicio_t2} y {fecha_fin_t2}")

    if not temp_t2.empty:
        temp_t2 = pd.merge(temp_colores,temp_t2,how='left',on=['id_fuente','color'])
        temp_t2['id'] = temp_t2['id'].fillna(0)
        temp_t2 = temp_t2.rename(columns={'id_fuente':'Medio','color':'Color','id':'Cantidad'})
        temp_t2['Medio'] = temp_t2['Medio'].replace({1:'RADIO',2:'TV',3:'REDES'})
        temp_t2['Porcentaje'] = temp_t2['Cantidad'] / temp_t2.groupby('Medio')['Cantidad'].transform('sum')
        temp_t2['Porcentaje'] = temp_t2['Porcentaje'].map('{:.0%}'.format)
        st.dataframe(temp_t2, hide_index=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% 3.1.- Grafico semanal por porcentaje cocteles
    st.subheader("3.- Gráfico semanal por porcentaje de cocteles en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g1 = st.date_input("Fecha Inicio g3", format="DD.MM.YYYY")
    with col2:
        fecha_fin_g1 = st.date_input("Fecha Fin g3", format="DD.MM.YYYY")
    with col3:
        option_fuente_g1 = st.selectbox("Fuente g3", ("Radio", "TV", "Redes", "Todos"))
    with col4:
        option_lugar_g1 = st.selectbox("Lugar g3", lugares_uniques)

    usar_fechas_viernes_g3 = st.toggle("Mostrar Fechas (Viernes de cada semana)",  key="toggle_fechas_g3")

    fecha_inicio_g1 = pd.to_datetime(fecha_inicio_g1, format="%Y-%m-%d")
    fecha_fin_g1 = pd.to_datetime(fecha_fin_g1, format="%Y-%m-%d")

    temp_g1 = temp_coctel_fuente[
        (temp_coctel_fuente["fecha_registro"] >= fecha_inicio_g1)
        & (temp_coctel_fuente["fecha_registro"] <= fecha_fin_g1)
        & (temp_coctel_fuente["lugar"] == option_lugar_g1)
    ]

    if option_fuente_g1 == "Radio":
        temp_g1 = temp_g1[temp_g1["id_fuente"] == 1]
    elif option_fuente_g1 == "TV":
        temp_g1 = temp_g1[temp_g1["id_fuente"] == 2]
    elif option_fuente_g1 == "Redes":
        temp_g1 = temp_g1[temp_g1["id_fuente"] == 3]

    if not temp_g1.empty:
        temp_g1["semana"] = (
            temp_g1["fecha_registro"].dt.year.map(str)
            + "-"
            + temp_g1["fecha_registro"].dt.isocalendar().week.map(str)
        )

        temp_g1["viernes"] = temp_g1["fecha_registro"] + pd.to_timedelta(
            (4 - temp_g1["fecha_registro"].dt.weekday) % 7, unit="D"
        )

        temp_g1 = temp_g1.groupby("semana", as_index=False).agg(
            {"id": "count", "coctel": "sum", "fecha_registro": "first", "viernes": "first"}
        )

        temp_g1 = temp_g1.rename(columns={"id": "Cantidad"})
        temp_g1["porcentaje"] = (temp_g1["coctel"] / temp_g1["Cantidad"]) * 100
        temp_g1 = temp_g1.sort_values("fecha_registro")

        if usar_fechas_viernes_g3:
            temp_g1["eje_x"] = temp_g1["viernes"].dt.strftime("%Y-%m-%d")
        else:
            temp_g1["eje_x"] = temp_g1["fecha_registro"].dt.strftime("%Y-%m") + "-S" + (
                (temp_g1["fecha_registro"].dt.day - 1) // 7 + 1
            ).astype(str)

        fig1 = go.Figure()
        fig1.add_trace(
            go.Scatter(
                x=temp_g1["eje_x"],
                y=temp_g1["porcentaje"],
                mode="lines+markers+text" if mostrar_todos else "lines+markers",
                text=temp_g1["porcentaje"].map(lambda x: f"{x:.1f}")
                if mostrar_todos
                else None,
                textposition="top center",
            )
        )

        fig1.update_layout(xaxis=dict(tickformat="%d-%m-%Y" if usar_fechas_viernes_g3 else ""))
        fig1.update_xaxes(
            title_text="Fecha (Viernes)" if usar_fechas_viernes_g3 else "Semana",
            tickangle=45,  
            tickmode="array" if usar_fechas_viernes_g3 else "linear",  # Usa "auto" en vez de "linear" para evitar inconsistencias
            tickvals=temp_g1["eje_x"] if usar_fechas_viernes_g3 else None,  # Evita duplicados en fechas
            # ticktext=temp_g1["eje_x"] if usar_fechas_viernes_g3 else None,  # Se asegura que los textos coincidan con los valores
            tickformat="%d-%m-%Y" if usar_fechas_viernes_g3 else "",
            )
        fig1.update_yaxes(title_text="Porcentaje de cocteles %")
        
        st.plotly_chart(fig1)

    else:
        st.warning("No hay datos para mostrar")

    #%% 3.2.- Grafico semanal noticias a favor y en contra
    st.subheader("4.- Gráfico semanal de noticias a favor y en contra en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g2 = st.date_input("Fecha Inicio g4", format="DD.MM.YYYY")
    with col2:
        fecha_fin_g2 = st.date_input("Fecha Fin g4", format="DD.MM.YYYY")
    with col3:
        option_lugar_g2 = st.selectbox("Lugar g4", lugares_uniques)
    with col4:
        option_fuente_g2 = st.selectbox("Fuente g4", ("Radio", "TV", "Redes", "Todos"))

    usar_fechas_viernes_g4 = st.toggle("Mostrar Fechas (Viernes de cada semana)", key="toggle_g4")

    fecha_inicio_g2 = pd.to_datetime(fecha_inicio_g2, format="%Y-%m-%d")
    fecha_fin_g2 = pd.to_datetime(fecha_fin_g2, format="%Y-%m-%d")

    temp_g2 = temp_coctel_fuente[
        (temp_coctel_fuente["fecha_registro"] >= fecha_inicio_g2)
        & (temp_coctel_fuente["fecha_registro"] <= fecha_fin_g2)
        & (temp_coctel_fuente["lugar"] == option_lugar_g2)
    ]

    if not temp_g2.empty:
        temp_g2["semana"] = (
            temp_g2["fecha_registro"].dt.year.map(str)
            + "-"
            + temp_g2["fecha_registro"].dt.isocalendar().week.map(str)
        )

        temp_g2["viernes"] = temp_g2["fecha_registro"] + pd.to_timedelta(
            (4 - temp_g2["fecha_registro"].dt.weekday) % 7, unit="D"
        )

        temp_g2["a_favor"] = (temp_g2["id_posicion"].isin([1, 2])).astype(int)
        temp_g2["en_contra"] = (temp_g2["id_posicion"].isin([4, 5])).astype(int)

        if option_fuente_g2 == "Radio":
            temp_g2 = temp_g2[temp_g2["id_fuente"] == 1]
        elif option_fuente_g2 == "TV":
            temp_g2 = temp_g2[temp_g2["id_fuente"] == 2]
        elif option_fuente_g2 == "Redes":
            temp_g2 = temp_g2[temp_g2["id_fuente"] == 3]

        temp_g2 = temp_g2.groupby("semana", as_index=False).agg(
            {
                "id": "count",
                "a_favor": "sum",
                "en_contra": "sum",
                "fecha_registro": "first",
                "viernes": "first",
            }
        )

        temp_g2 = temp_g2.rename(columns={"id": "Cantidad"})
        temp_g2 = temp_g2.sort_values("fecha_registro")
        temp_g2["a_favor"] = (temp_g2["a_favor"] / temp_g2["Cantidad"]) * 100
        temp_g2["en_contra"] = (temp_g2["en_contra"] / temp_g2["Cantidad"]) * 100

        if usar_fechas_viernes_g4:
            temp_g2["eje_x"] = temp_g2["viernes"].dt.strftime("%d-%m-%Y")
        else:
            temp_g2["eje_x"] = temp_g2["fecha_registro"].dt.strftime("%Y-%m") + "-S" + (
                (temp_g2["fecha_registro"].dt.day - 1) // 7 + 1
            ).astype(str)

        fig2 = go.Figure()
        fig2.add_trace(
            go.Scatter(
                x=temp_g2["eje_x"],
                y=temp_g2["a_favor"],
                mode="lines+markers+text" if mostrar_todos else "lines+markers",
                name="A favor",
                text=temp_g2["a_favor"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                textposition="top center",
                line=dict(color="blue"),
            )
        )

        fig2.add_trace(
            go.Scatter(
                x=temp_g2["eje_x"],
                y=temp_g2["en_contra"],
                mode="lines+markers+text" if mostrar_todos else "lines+markers",
                name="En contra",
                text=temp_g2["en_contra"].map(lambda x: f"{x:.1f}%") if mostrar_todos else None,
                textposition="top center",
                line=dict(color="red"),
            )
        )

        # Ajustes en el eje X para mostrar más fechas
        fig2.update_xaxes(
            title_text="Fecha (Viernes)" if usar_fechas_viernes_g4 else "Semana",
            tickangle=45,  # Rota las fechas para evitar superposición
            tickmode="array" if usar_fechas_viernes_g4 else "linear",
            tickvals=temp_g2["eje_x"] if usar_fechas_viernes_g4 else None,
            ticktext=temp_g2["eje_x"] if usar_fechas_viernes_g4 else None,
            tickformat="%d-%m-%Y" if usar_fechas_viernes_g4 else "",
        )

        fig2.update_yaxes(title_text="Porcentaje de noticias %")

        st.plotly_chart(fig2)

    else:
        st.warning("No hay datos para mostrar")

    #%% 4.- Grafico acumulativo porcentaje cocteles
    st.subheader("5.- Gráfico acumulativo porcentaje de cocteles en lugar y fecha específica")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_g3 = st.date_input("Fecha Inicio g5", format="DD.MM.YYYY")
    with col2:
        fecha_fin_g3 = st.date_input("Fecha Fin g5", format="DD.MM.YYYY")
    with col3:
        option_fuente_g3 = st.selectbox("Fuente g5", ("Radio", "TV", "Redes", "Todos"))

    option_lugar_g3 = st.multiselect("Lugar g5", lugares_uniques, lugares_uniques)

    usar_fechas_viernes_g5 = st.toggle("Mostrar Fechas (Viernes de cada semana)", key="toggle_g5")

    fecha_inicio_g3 = pd.to_datetime(fecha_inicio_g3, format="%Y-%m-%d")
    fecha_fin_g3 = pd.to_datetime(fecha_fin_g3, format="%Y-%m-%d")

    temp_g3 = temp_coctel_fuente[
        (temp_coctel_fuente["fecha_registro"] >= fecha_inicio_g3)
        & (temp_coctel_fuente["fecha_registro"] <= fecha_fin_g3)
        & (temp_coctel_fuente["lugar"].isin(option_lugar_g3))
    ]

    if option_fuente_g3 == "Radio":
        temp_g3 = temp_g3[temp_g3["id_fuente"] == 1]
    elif option_fuente_g3 == "TV":
        temp_g3 = temp_g3[temp_g3["id_fuente"] == 2]
    elif option_fuente_g3 == "Redes":
        temp_g3 = temp_g3[temp_g3["id_fuente"] == 3]

    if not temp_g3.empty:
        temp_g3["semana"] = (
            temp_g3["fecha_registro"].dt.year.map(str)
            + "-"
            + temp_g3["fecha_registro"].dt.isocalendar().week.map(lambda x: f"{x:02}")
        )

        temp_g3["viernes"] = temp_g3["fecha_registro"] + pd.to_timedelta(
            (4 - temp_g3["fecha_registro"].dt.weekday) % 7, unit="D"
        )

        temp_g3 = temp_g3.groupby(["semana", "lugar"], as_index=False).agg(
            coctel_mean=("coctel", "mean"), viernes=("viernes", "first")
        )

        temp_g3["coctel_mean"] = temp_g3["coctel_mean"] * 100

        if usar_fechas_viernes_g5:
            temp_g3["eje_x"] = temp_g3["viernes"].dt.strftime("%d-%m-%Y")
        else:
            temp_g3["eje_x"] = temp_g3["viernes"].dt.strftime("%Y-%m") + "-S" + (
                (temp_g3["viernes"].dt.day - 1) // 7 + 1
            ).astype(str)

        #temp_g3=temp_g3[temp_g3["coctel_mean"]>0]
        fig = px.line(
            temp_g3,
            x="eje_x",
            y="coctel_mean",
            color="lugar",
            title="Porcentaje de cocteles por semana %",
            labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes_g5 else "Semana", "coctel_mean": "Porcentaje de cocteles %"},
            markers=True,
            text=temp_g3["coctel_mean"].map(lambda x: f"{x:.1f}") if mostrar_todos else None,
        )

        fig.update_traces(textposition="top center")

        # Ajustes en el eje X para mostrar más fechas y rotar etiquetas
        fig.update_xaxes(
            title_text="Fecha (Viernes)" if usar_fechas_viernes_g5 else "Semana",
            tickangle=45,  # Rota las fechas para mejor legibilidad
            tickmode="array" if usar_fechas_viernes_g5 else "linear",
            tickvals=temp_g3["eje_x"] if usar_fechas_viernes_g5 else None,
            ticktext=temp_g3["eje_x"] if usar_fechas_viernes_g5 else None,
            tickformat="%d-%m-%Y" if usar_fechas_viernes_g5 else "",
        )

        fig.update_yaxes(title_text="Porcentaje de cocteles %")

        st.plotly_chart(fig)

        st.write(f"Porcentaje de cocteles por lugar en la última semana entre {fecha_inicio_g3} y {fecha_fin_g3} según {option_fuente_g3}")

        temp_g3 = temp_g3.sort_values("semana").groupby("lugar").last().reset_index()
        temp_g3['coctel_mean'] = temp_g3['coctel_mean'].map(lambda x: f"{x:.1f}")
        temp_g3 = temp_g3[["lugar", "coctel_mean"]].rename(columns={"coctel_mean": "pct_cocteles"})

        st.dataframe(temp_g3, hide_index=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% #.- Top 3 mejores lugares segun fechas y fuente
    st.subheader("#.- Top 3 mejores porcentajes de coctel semanal por lugar en fuente y fecha específica")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_sn2 = st.date_input("Fecha Inicio sn_2", format="DD.MM.YYYY")
    with col2:
        fecha_fin_sn2 = st.date_input("Fecha Fin sn_2", format="DD.MM.YYYY")
    with col3:
        option_fuente_sn2 = st.selectbox("Fuente sn_2", ("Radio", "TV", "Redes"))

    usar_fechas_viernes_sn2 = st.toggle("Mostrar Fechas (Viernes de cada semana)", key="toggle_sn2")

    fecha_inicio_sn2 = pd.to_datetime(fecha_inicio_sn2, format="%Y-%m-%d")
    fecha_fin_sn2 = pd.to_datetime(fecha_fin_sn2, format="%Y-%m-%d")

    temp_sn2 = temp_coctel_fuente[
        (temp_coctel_fuente["fecha_registro"] >= fecha_inicio_sn2)
        & (temp_coctel_fuente["fecha_registro"] <= fecha_fin_sn2)
    ]

    if option_fuente_sn2 == "Radio":
        temp_sn2 = temp_sn2[temp_sn2["id_fuente"] == 1]
    elif option_fuente_sn2 == "TV":
        temp_sn2 = temp_sn2[temp_sn2["id_fuente"] == 2]
    elif option_fuente_sn2 == "Redes":
        temp_sn2 = temp_sn2[temp_sn2["id_fuente"] == 3]

    if not temp_sn2.empty:
        temp_sn2["semana"] = (
            temp_sn2["fecha_registro"].dt.year.map(str)
            + "-"
            + temp_sn2["fecha_registro"].dt.isocalendar().week.map(lambda x: f"{x:02}")
        )

        temp_sn2["viernes"] = temp_sn2["fecha_registro"] + pd.to_timedelta(
            (4 - temp_sn2["fecha_registro"].dt.weekday) % 7, unit="D"
        )

        temp_sn2 = temp_sn2.groupby(["lugar", "semana"], as_index=False).agg(
            coctel=("coctel", "mean"), viernes=("viernes", "first")
        )

        temp_sn2_last = temp_sn2.sort_values("semana").groupby(["lugar"]).last().reset_index()
        temp_sn2_last = temp_sn2_last.sort_values("coctel", ascending=False).head(3).reset_index(drop=True)

        temp_sn2 = temp_sn2[temp_sn2["lugar"].isin(temp_sn2_last["lugar"])]
        temp_sn2["coctel"] = temp_sn2["coctel"] * 100

        if usar_fechas_viernes_sn2:
            temp_sn2["eje_x"] = temp_sn2["viernes"].dt.strftime("%d-%m-%Y")
        else:
            temp_sn2["eje_x"] = temp_sn2["viernes"].dt.strftime("%Y-%m") + "-S" + (
                (temp_sn2["viernes"].dt.day - 1) // 7 + 1
            ).astype(str)

        fig_sn2 = px.line(
            temp_sn2,
            x="eje_x",
            y="coctel",
            color="lugar",
            title="Top 3 lugares con mayor porcentaje de cocteles",
            labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes_sn2 else "Semana", "coctel": "Porcentaje de cocteles %"},
            markers=True,
            text=temp_sn2["coctel"].map(lambda x: f"{x:.1f}") if mostrar_todos else None,
        )

        fig_sn2.update_traces(textposition="top center")

        # Ajustes en el eje X para mostrar más fechas y rotar etiquetas
        fig_sn2.update_xaxes(
            title_text="Fecha (Viernes)" if usar_fechas_viernes_sn2 else "Semana",
            tickangle=45,  # Rota las fechas para mejor legibilidad
            tickmode="array" if usar_fechas_viernes_sn2 else "linear",
            tickvals=temp_sn2["eje_x"] if usar_fechas_viernes_sn2 else None,
            ticktext=temp_sn2["eje_x"] if usar_fechas_viernes_sn2 else None,
            tickformat="%d-%m-%Y" if usar_fechas_viernes_sn2 else "",
        )

        fig_sn2.update_yaxes(title_text="Porcentaje de cocteles %")

        st.plotly_chart(fig_sn2)
        st.write(f"Top 3 lugares con mayor porcentaje de cocteles en la última semana entre {fecha_inicio_sn2} y {fecha_fin_sn2} según {option_fuente_sn2}")  

        temp_sn2_last['coctel'] = (temp_sn2_last['coctel'] * 100).map(lambda x: f"{x:.1f}")
        st.dataframe(temp_sn2_last, hide_index=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% 5.- Top 3 mejores radios, redes, tv usar dataframes de programas y redes
    st.subheader("6.- Top 3 mejores radios, redes, tv en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g6 = st.date_input("Fecha Inicio g6", format="DD.MM.YYYY")
    with col2:
        fecha_fin_g6 = st.date_input("Fecha Fin g6", format="DD.MM.YYYY")
    with col3:
        option_fuente_g6 = st.selectbox("Fuente g6", ("Radio", "TV", "Redes"))
    with col4:
        option_lugar_g6 = st.selectbox("Lugar g6", lugares_uniques)

    usar_fechas_viernes_g6 = st.toggle("Mostrar Fechas (Viernes de cada semana)", key="toggle_g6")

    fecha_inicio_g6 = pd.to_datetime(fecha_inicio_g6, format="%Y-%m-%d")
    fecha_fin_g6 = pd.to_datetime(fecha_fin_g6, format="%Y-%m-%d")

    temp_g6_medio = temp_coctel_fuente_programas[
        (temp_coctel_fuente_programas["fecha_registro"] >= fecha_inicio_g6)
        & (temp_coctel_fuente_programas["fecha_registro"] <= fecha_fin_g6)
        & (temp_coctel_fuente_programas["lugar"] == option_lugar_g6)
    ]

    temp_g6_redes = temp_coctel_fuente_fb[
        (temp_coctel_fuente_fb["fecha_registro"] >= fecha_inicio_g6)
        & (temp_coctel_fuente_fb["fecha_registro"] <= fecha_fin_g6)
        & (temp_coctel_fuente_fb["lugar"] == option_lugar_g6)
    ]

    if option_fuente_g6 == "Radio":
        temp_g6_medio = temp_g6_medio[temp_g6_medio["id_fuente"] == 1]
    elif option_fuente_g6 == "TV":
        temp_g6_medio = temp_g6_medio[temp_g6_medio["id_fuente"] == 2]
    elif option_fuente_g6 == "Redes":
        temp_g6_redes = temp_g6_redes[temp_g6_redes["id_fuente"] == 3]

    if option_fuente_g6 == "Redes" and not temp_g6_redes.empty:
        temp_g6_redes["viernes"] = temp_g6_redes["fecha_registro"] + pd.to_timedelta(
            (4 - temp_g6_redes["fecha_registro"].dt.weekday) % 7, unit="D"
        )

        temp_g6_redes_top = temp_g6_redes.groupby(["nombre_facebook_page"], as_index=False).agg({"coctel": "mean"})
        temp_g6_redes_top = temp_g6_redes_top.sort_values("coctel", ascending=False).head(3)

        top_3_redes_list = temp_g6_redes_top["nombre_facebook_page"].tolist()

        temp_g6_redes = temp_g6_redes[temp_g6_redes["nombre_facebook_page"].isin(top_3_redes_list)]
        temp_g6_redes = temp_g6_redes.groupby(["viernes", "nombre_facebook_page"], as_index=False).agg({"coctel": "mean"})

        temp_g6_redes["coctel"] = temp_g6_redes["coctel"] * 100

        if usar_fechas_viernes_g6:
            temp_g6_redes["eje_x"] = temp_g6_redes["viernes"].dt.strftime("%d-%m-%Y")
        else:
            temp_g6_redes["eje_x"] = temp_g6_redes["viernes"].dt.strftime("%Y-%m") + "-S" + (
                (temp_g6_redes["viernes"].dt.day - 1) // 7 + 1
            ).astype(str)
    
        fig_6 = px.line(
            temp_g6_redes,
            x="eje_x",
            y="coctel",
            color="nombre_facebook_page",
            title="Top 3 redes sociales con mayor porcentaje de cocteles",
            labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes_g6 else "Semana", "coctel": "Porcentaje de cocteles %"},
            markers=True,
            text=temp_g6_redes["coctel"].map(lambda x: f"{x:.1f}") if mostrar_todos else None,
        )

        fig_6.update_traces(textposition="top center")

        # Ajustes en el eje X para mostrar más fechas y rotar etiquetas
        fig_6.update_xaxes(
            title_text="Fecha (Viernes)" if usar_fechas_viernes_g6 else "Semana",
            tickangle=45,  
            tickmode="array" if usar_fechas_viernes_g6 else "linear",
            tickvals=temp_g6_redes["eje_x"] if usar_fechas_viernes_g6 else None,
            ticktext=temp_g6_redes["eje_x"] if usar_fechas_viernes_g6 else None,
            tickformat="%d-%m-%Y" if usar_fechas_viernes_g6 else "",
        )

        fig_6.update_yaxes(title_text="Porcentaje de cocteles %")

        st.plotly_chart(fig_6)

    elif option_fuente_g6 != "Redes" and not temp_g6_medio.empty:
        temp_g6_medio["viernes"] = temp_g6_medio["fecha_registro"] + pd.to_timedelta(
            (4 - temp_g6_medio["fecha_registro"].dt.weekday) % 7, unit="D"
        )

        temp_g6_medio_top = temp_g6_medio.groupby(["nombre_canal"], as_index=False).agg({"coctel": "mean"})
        temp_g6_medio_top = temp_g6_medio_top.sort_values("coctel", ascending=False).head(3)

        top_3_medio_list = temp_g6_medio_top["nombre_canal"].tolist()

        temp_g6_medio = temp_g6_medio[temp_g6_medio["nombre_canal"].isin(top_3_medio_list)]
        temp_g6_medio = temp_g6_medio.groupby(["viernes", "nombre_canal"], as_index=False).agg({"coctel": "mean"})

        temp_g6_medio["coctel"] = temp_g6_medio["coctel"] * 100

        if usar_fechas_viernes_g6:
            temp_g6_medio["eje_x"] = temp_g6_medio["viernes"].dt.strftime("%d-%m-%Y")
        else:
            temp_g6_medio["eje_x"] = temp_g6_medio["viernes"].dt.strftime("%Y-%m") + "-S" + (
                (temp_g6_medio["viernes"].dt.day - 1) // 7 + 1
            ).astype(str)

        fig_6 = px.line(
            temp_g6_medio,
            x="eje_x",
            y="coctel",
            color="nombre_canal",
            title="Top 3 medios con mayor porcentaje de cocteles",
            labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes_g6 else "Semana", "coctel": "Porcentaje de cocteles %"},
            markers=True,
            text=temp_g6_medio["coctel"].map(lambda x: f"{x:.1f}") if mostrar_todos else None,
        )

        fig_6.update_traces(textposition="top center")
        fig_6.update_xaxes(
            title_text="Fecha (Viernes)" if usar_fechas_viernes_g6 else "Semana",
            tickangle=45,  
            tickmode="array" if usar_fechas_viernes_g6 else "linear",
            tickvals=temp_g6_medio["eje_x"] if usar_fechas_viernes_g6 else None,
            ticktext=temp_g6_medio["eje_x"] if usar_fechas_viernes_g6 else None,
            tickformat="%d-%m-%Y" if usar_fechas_viernes_g6 else "",
        )
        st.plotly_chart(fig_6)

    else:
        st.warning("No hay datos para mostrar")

    #%% 6.- Crecimiento de cocteles por macroregion

    # Para radio y redes las macroregiones son: Macro región Sur 1 – Tacna, Puno y Cusco (radio y redes),
    #                                           Macro región Sur 2 – Ayacucho y Arequipa (radio y redes)
    #                                           Macro región Norte – Piura y Trujillo (radio y redes)
    #                                           Macro región Centro – Lima, Ica, Huánuco (radio y redes)
    #                                           Macro región Unacem – Lima Sur, Cañete, Tarma (radio y redes) 
    # Para tv la macroregion es: Macro región TV: Ayacucho, Piura y Arequipa
    macroregiones_radio_redes = ["Macro región Sur 1", "Macro región Sur 2", "Macro región Norte", "Macro región Centro", "Macro región UNACEM"]
    macroregiones_tv = ["Macro región TV"]

    macroregiones = {
        "Macro región Sur 1": ["Tacna", "Puno", "Cusco"],
        "Macro región Sur 2": ["Ayacucho", "Arequipa"],
        "Macro región Norte": ["Piura", "Trujillo"],
        "Macro región Centro": ["Lima", "Ica", "Huanuco"],
        "Macro región UNACEM": ["Lima Sur", "Cañete", "Tarma"],
        "Macro región TV": ["Ayacucho", "Piura", "Arequipa"]
    }

    st.subheader("7.- Crecimiento de cocteles por macroregion en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g7 = st.date_input("Fecha Inicio g7", format="DD.MM.YYYY")
    with col2:
        fecha_fin_g7 = st.date_input("Fecha Fin g7", format="DD.MM.YYYY")
    with col3:
        option_fuente_g7 = st.selectbox("Fuente g7", ("Radio", "TV", "Redes"))
    with col4:
        if option_fuente_g7 in ["Radio", "Redes"]:
            option_macroregion_g7 = st.selectbox("Macroregión g7", macroregiones_radio_redes)
        elif option_fuente_g7 == "TV":
            option_macroregion_g7 = st.selectbox("Macroregión g7", macroregiones_tv)

    usar_fechas_viernes_g7 = st.toggle("Mostrar Fechas (Viernes de cada semana)", key="toggle_g7")

    fecha_inicio_g7 = pd.to_datetime(fecha_inicio_g7, format="%Y-%m-%d")
    fecha_fin_g7 = pd.to_datetime(fecha_fin_g7, format="%Y-%m-%d")

    temp_g7 = temp_coctel_fuente[
        (temp_coctel_fuente["fecha_registro"] >= fecha_inicio_g7)
        & (temp_coctel_fuente["fecha_registro"] <= fecha_fin_g7)
    ]

    if option_fuente_g7 == "Radio":
        temp_g7 = temp_g7[temp_g7["id_fuente"] == 1]
    elif option_fuente_g7 == "TV":
        temp_g7 = temp_g7[temp_g7["id_fuente"] == 2]
    elif option_fuente_g7 == "Redes":
        temp_g7 = temp_g7[temp_g7["id_fuente"] == 3]

    if not temp_g7.empty:
        temp_g7["semana"] = (
            temp_g7["fecha_registro"].dt.year.map(str)
            + "-"
            + temp_g7["fecha_registro"].dt.isocalendar().week.map(str)
        )
        temp_g7["viernes"] = temp_g7["fecha_registro"] + pd.to_timedelta(
            (4 - temp_g7["fecha_registro"].dt.weekday) % 7, unit="D"
        )
        temp_g7 = temp_g7.groupby(["semana", "lugar"], as_index=False).agg(
            coctel_mean=("coctel", "mean"), viernes=("viernes", "first")
            ).reset_index()
        temp_g7["coctel_mean"] = temp_g7["coctel_mean"] * 100
        temp_g7 = temp_g7[temp_g7["coctel_mean"] > 0]
        temp_g7 = temp_g7.sort_values("semana")

        departamentos = macroregiones.get(option_macroregion_g7, [])
        temp_g7 = temp_g7[temp_g7["lugar"].isin(departamentos)]
        

        if usar_fechas_viernes_g7:
            temp_g7["eje_x"] = temp_g7["viernes"].dt.strftime("%d-%m-%Y")
        else:
            temp_g7["eje_x"] = temp_g7["viernes"].dt.strftime("%Y-%m") + "-S" + (
                (temp_g7["viernes"].dt.day - 1) // 7 + 1
            ).astype(str)

        fig_7 = px.line(
            temp_g7,
            x="eje_x",
            y="coctel_mean",
            color="lugar",
            title=f"Crecimiento de cocteles por macroregión en {option_macroregion_g7} entre {fecha_inicio_g7.strftime('%d-%m-%Y')} y {fecha_fin_g7.strftime('%d-%m-%Y')}",
            labels={"eje_x": "Fecha (Viernes)" if usar_fechas_viernes_g7 else "Semana", "coctel_mean": "Porcentaje de cocteles %"},
            markers=True,
            text=temp_g7["coctel_mean"].map(lambda x: f"{x:.1f}") if mostrar_todos else None,
        )
        fig_7.update_traces(textposition="top center")
        fig_7.update_layout(xaxis=dict(tickformat="%d-%m-%Y" if usar_fechas_viernes_g7 else ""))
        fig_7.update_xaxes(

            title_text="Fecha (Viernes)" if usar_fechas_viernes_g7 else "Semana",
            tickangle=45,  
            tickmode="array" if usar_fechas_viernes_g7 else "linear",  # Usa "auto" en vez de "linear" para evitar inconsistencias
            tickvals=temp_g7["eje_x"] if usar_fechas_viernes_g7 else None,  # Evita duplicados en fechas
            ticktext=temp_g7["eje_x"] if usar_fechas_viernes_g7 else None,  # Se asegura que los textos coincidan con los valores
            # type="category" if not usar_fechas_viernes_g7 else "date",  # Mantiene el formato correcto del eje X
            tickformat="%d-%m-%Y" if usar_fechas_viernes_g7 else ""
        )        


        # fig_7.update_xaxes(type="category" if not usar_fechas_viernes_g7 else "date")

        st.plotly_chart(fig_7)
        st.write("Nota: Los valores muestran el porcentaje de cocteles en cada semana tomando como referencia el viernes")

    else:
        st.warning("No hay datos para mostrar")


    #%% 7.- Grafico de barras contando posiciones

    st.subheader("8.- Gráfico de barras contando posiciones en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g7 = st.date_input("Fecha Inicio g8",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g7 = st.date_input("Fecha Fin g8",
                                    format="DD.MM.YYYY")
    with col3:
        option_lugar_g7 = st.selectbox(
            "Lugar g8",
            lugares_uniques
            )
    with col4:
        option_nota_g7 = st.selectbox(
            "Nota g8",
            ("Con coctel", "Sin coctel", "Todos"))

    fecha_inicio_g7 = pd.to_datetime(fecha_inicio_g7, format='%Y-%m-%d')
    fecha_fin_g7 = pd.to_datetime(fecha_fin_g7, format='%Y-%m-%d')

    temp_g7 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g7) &
                                (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g7) &
                                (temp_coctel_fuente['lugar'] == option_lugar_g7)]

    if option_nota_g7 == 'Con coctel':
        temp_g7 = temp_g7[temp_g7['coctel'] == 1]
        titulo_g7 = f'Conteo de posiciones con coctel en {option_lugar_g7} por tipo de medio'
    elif option_nota_g7 == 'Sin coctel':
        temp_g7 = temp_g7[temp_g7['coctel'] == 0]
        titulo_g7 = f'Conteo de posiciones sin coctel en {option_lugar_g7} por tipo de medio'
    else:
        titulo_g7 = f'Conteo de posiciones en {option_lugar_g7} por tipo de medio'

    if not temp_g7.empty:
        temp_g7['semana'] = temp_g7['fecha_registro'].dt.isocalendar().year.map(str) +'-'+temp_g7['fecha_registro'].dt.isocalendar().week.map(str)
        conteo_total = temp_g7.groupby(['id_posicion', 'id_fuente']).size().reset_index(name='count')
        conteo_total['Posición'] = conteo_total['id_posicion'].map(id_posicion_dict)
        conteo_total['Tipo de Medio'] = conteo_total['id_fuente'].map(id_fuente_dict)
        conteo_total = conteo_total.dropna()

        fig_7 = px.bar(conteo_total,
                    x='Posición',
                    y='count',
                    color='Tipo de Medio',
                    title=titulo_g7,
                    barmode='group',
                    labels={'count': 'Conteo', 'Posición': 'Posición', 'Tipo de Medio': 'Tipo de Medio'},
                    color_discrete_map={'Radio': '#3F6EC3', 'TV': '#A1A1A1', 'Redes': '#C00000'},
                    text='count'
                    )

        fig_7.update_layout(xaxis_title='Posición',
                            yaxis_title='Conteo',
                            legend_title='Tipo de Medio')

        st.plotly_chart(fig_7)
        st.write(f"Gráfico de barras contando posiciones en {option_lugar_g7} entre {fecha_inicio_g7.strftime('%Y-%m-%d')} y {fecha_fin_g7.strftime('%Y-%m-%d')}")

    else:
        st.warning("No hay datos para mostrar")

    #%% 8.- grafico de dona que representa el porcentaje de posiciones

    st.subheader("9.- Gráfico de dona que representa el porcentaje de posiciones en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g8 = st.date_input("Fecha Inicio g9",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g8 = st.date_input("Fecha Fin g9",
                                    format="DD.MM.YYYY")
    with col3:
        option_fuente_g8 = st.selectbox(
        "Fuente g9",
        ("Radio", "TV", "Redes","Todos"))
    with col4:
        option_nota_g8 = st.selectbox(
        "Nota g9",
        ("Con coctel", "Sin coctel", "Todos"))

    option_lugar_g8 = st.multiselect(
    "Lugar g9",
    lugares_uniques,lugares_uniques)

    fecha_inicio_g8 = pd.to_datetime(fecha_inicio_g8, format='%Y-%m-%d')
    fecha_fin_g8 = pd.to_datetime(fecha_fin_g8, format='%Y-%m-%d')

    temp_g8 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g8) &
                                (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g8) & 
                                (temp_coctel_fuente['lugar'].isin(option_lugar_g8))]

    if option_nota_g8 == 'Con coctel':
        temp_g8 = temp_g8[temp_g8['coctel']==1]
        titulo_g8 = 'Porcentaje de posiciones con coctel respecto del total'
    elif option_nota_g8 == 'Sin coctel':
        temp_g8 = temp_g8[temp_g8['coctel']==0]
        titulo_g8 = 'Porcentaje de posiciones sin coctel respecto del total'
    else:
        titulo_g8 = 'Porcentaje de posiciones respecto del total'

    if option_fuente_g8 == 'Radio':
        temp_g8 = temp_g8[temp_g8['id_fuente']==1]
    elif option_fuente_g8 == 'TV':
        temp_g8 = temp_g8[temp_g8['id_fuente']==2]
    elif option_fuente_g8 == 'Redes':
        temp_g8 = temp_g8[temp_g8['id_fuente']==3]

    if not temp_g8.empty:
        conteo_total_g8 = temp_g8.groupby(['id_posicion']).size().reset_index(name='count')

        conteo_total_g8['Posición'] = conteo_total_g8['id_posicion'].map(id_posicion_dict)
        conteo_total_g8 = conteo_total_g8.dropna()

        #porcentajes
        conteo_total_g8['Porcentaje'] = conteo_total_g8['count'] / conteo_total_g8['count'].sum()
        conteo_total_g8['Porcentaje'] = conteo_total_g8['Porcentaje'].map('{:.0%}'.format)

        fig_8 = px.pie(conteo_total_g8,
                    values='count',
                    names='Posición',
                    title=titulo_g8,
                    color='Posición',
                    color_discrete_map=color_posicion_dict,
                    hole=0.3
                    )
        st.plotly_chart(fig_8)
        st.write(f"Gráfico de dona que representa el porcentaje de posiciones en {', '.join(option_lugar_g8)} entre {fecha_inicio_g8.strftime('%Y-%m-%d')} y {fecha_fin_g8.strftime('%Y-%m-%d')}")

    else:
        st.warning("No hay datos para mostrar")

    #%% 9.- porcentaje de acontecimientos con coctel

    st.subheader("10.- Porcentaje de acontecimientos con coctel en lugar y fecha específica")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_g9 = st.date_input("Fecha Inicio g10",
                                        format="DD.MM.YYYY")

    with col2:
        fecha_fin_g9 = st.date_input("Fecha Fin g10",
                                    format="DD.MM.YYYY")
        
    with col3:
        option_fuente_g9 = st.selectbox(
        "Fuente g10",
        ("Radio", "TV", "Redes","Todos"))

    option_lugar_g9 = st.multiselect(
    "Lugar g10",
    lugares_uniques,lugares_uniques)

    fecha_inicio_g9 = pd.to_datetime(fecha_inicio_g9, format='%Y-%m-%d')
    fecha_fin_g9 = pd.to_datetime(fecha_fin_g9, format='%Y-%m-%d')

    temp_g9 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g9) & (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g9) & (temp_coctel_fuente['lugar'].isin(option_lugar_g9))]

    if option_fuente_g9 == 'Radio':
        temp_g9 = temp_g9[temp_g9['id_fuente']==1]
    elif option_fuente_g9 == 'TV':
        temp_g9 = temp_g9[temp_g9['id_fuente']==2]
    elif option_fuente_g9 == 'Redes':
        temp_g9 = temp_g9[temp_g9['id_fuente']==3]

    if not temp_g9.empty:
        conteo_total_g9 = temp_g9.groupby(['coctel']).size().reset_index(name='count')
        conteo_total_g9['Coctel'] = conteo_total_g9['coctel'].map(coctel_dict)
        conteo_total_g9['Porcentaje'] = conteo_total_g9['count'] / conteo_total_g9['count'].sum()
        # conteo_total_g9['Porcentaje'] = conteo_total_g9['Porcentaje'].map('{:.0%}'.format)
        st.write(f"Porcentaje de acontecimientos con coctel en {option_lugar_g9} entre {fecha_inicio_g9} y {fecha_fin_g9}")
        fig_9 = px.pie(
            conteo_total_g9,
            values='count',
            names='Coctel',
            title='Porcentaje de acontecimientos con coctel',
            hole=0.3,
            color='Coctel',
            color_discrete_map={'Sin coctel': 'orange', 'Con coctel': 'Blue'}
            # text=conteo_total_g9['Porcentaje'].map(lambda x: f"{x:.1f}") if mostrar_todos else None  # Muestra porcentajes si mostrar_todos es True
        )

        fig_9.update_traces(
            textposition='inside' if mostrar_todos else 'none',  # Posiciona el texto dentro si mostrar_todos es True
            textinfo='label+percent' if mostrar_todos else 'label'  # Muestra porcentaje solo si mostrar_todos es True
        )

        st.plotly_chart(fig_9)


    else:
        st.warning("No hay datos para mostrar")

    #%% 12.- Tabla que muestra la cantidad de cocteles por fuente y lugar

    st.subheader("11.- Cantidad de cocteles por fuente y lugar en fecha específica")

    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio_g12 = st.date_input("Fecha Inicio g11",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g12 = st.date_input("Fecha Fin g11",
                                    format="DD.MM.YYYY")

    option_lugar_g12 = st.multiselect("Lugar g11",
                                    lugares_uniques,
                                    lugares_uniques)

    fecha_inicio_g12 = pd.to_datetime(fecha_inicio_g12, format='%Y-%m-%d')
    fecha_fin_g12 = pd.to_datetime(fecha_fin_g12, format='%Y-%m-%d')

    temp_g12 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g12) & (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g12) & (temp_coctel_fuente['lugar'].isin(option_lugar_g12))]

    if not temp_g12.empty:
        conteo_total_g12 = temp_g12.groupby(['id_fuente', 'lugar', 'coctel']).size().reset_index(name='count')
        conteo_total_g12 = conteo_total_g12[conteo_total_g12['coctel'] == 1]
        conteo_total_g12['Fuente'] = conteo_total_g12['id_fuente'].map(id_fuente_dict)

        conteo_total_g12 = pd.crosstab(conteo_total_g12['lugar'],
                                    conteo_total_g12['Fuente'],
                                    values=conteo_total_g12['count'],
                                    aggfunc='sum').fillna(0).reset_index()
        conteo_total_g12 = conteo_total_g12.rename(columns={"tv": "televisión"})
        st.write(f"Cantidad de cocteles por fuente y lugar en {option_lugar_g12} entre {fecha_inicio_g12} y {fecha_fin_g12}")
        st.dataframe(conteo_total_g12, hide_index=True, width=300)

    else:
        st.warning("No hay datos para mostrar")

    #%% 13.- Reporte quincenal acerca de cuantas radios, redes y tv generaron coctel
    st.subheader("12.- Reporte semanal acerca de cuantas radios, redes y tv generaron coctel")

    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio_g13 = st.date_input("Fecha Inicio g12",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g13 = st.date_input("Fecha Fin g12",
                                    format="DD.MM.YYYY")
        
    option_lugar_g13 = st.multiselect("Lugar g12",
                                    lugares_uniques,
                                    lugares_uniques)

    fecha_inicio_g13 = pd.to_datetime(fecha_inicio_g13, format='%Y-%m-%d')
    fecha_fin_g13 = pd.to_datetime(fecha_fin_g13, format='%Y-%m-%d')

    temp_g13 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g13) &
                                (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g13) &
                                (temp_coctel_fuente['lugar'].isin(option_lugar_g13))]

    temp_g13_fb = temp_coctel_fuente_fb[(temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio_g13) &
                                        (temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin_g13) &
                                        (temp_coctel_fuente_fb['lugar'].isin(option_lugar_g13))]

    if not temp_g13.empty:
        temp_g13["semana"] = temp_g13['fecha_registro'].dt.isocalendar().year.map(str) +'-'+temp_g13['fecha_registro'].dt.isocalendar().week.map(str)

        temp_merged = pd.merge(temp_g13, temp_g13_fb[['fecha_registro', 'acontecimiento', 'coctel','id_fuente', 'lugar', 'nombre_facebook_page']], on=['fecha_registro', 'acontecimiento', 'coctel','id_fuente', 'lugar'], how='left')
        temp_merged['id_canal'] = temp_merged['id_canal'].fillna(temp_merged['nombre_facebook_page'])
        
        temp_g13_coctel = temp_merged[temp_merged['coctel'] == 1] #solo coctel

        conteo_total_g13 = temp_g13_coctel.groupby(['id_fuente', 'lugar', 'id_canal','semana']).size().reset_index(name='count')

        #mapeo de fuente
        conteo_total_g13['Fuente'] = conteo_total_g13['id_fuente'].astype(int).map(id_fuente_dict)

        conteo_canal_g13 = conteo_total_g13.groupby(['Fuente', 'lugar'])['id_canal'].nunique().reset_index(name='conteo_canal')

        conteo_canal_g13 = pd.crosstab(conteo_canal_g13['lugar'],
                                    conteo_canal_g13['Fuente'],
                                    values=conteo_canal_g13['conteo_canal'],
                                    aggfunc='sum').fillna(0).reset_index()


        st.dataframe(conteo_canal_g13, hide_index=True, width=300)

        conteo_canal_g13_melted = conteo_canal_g13.melt(id_vars=['lugar'], var_name='Fuente', value_name='Cantidad')

        y_max = conteo_canal_g13_melted['Cantidad'].max() * 1.1  # 10% por encima del máximo

        fig = px.bar(conteo_canal_g13_melted, 
                    x='lugar', 
                    y='Cantidad', 
                    color='Fuente', 
                    text='Cantidad',
                    title="Cantidad de Medios (Canales) que generan Cocteles por Lugar",
                    labels={'Cantidad': 'Número de Medios', 'lugar': 'Lugar', 'Fuente': 'Fuente'},
                    color_discrete_map={'Radio': '#3F6EC3', 'TV': '#A1A1A1', 'Redes': '#C00000'},
                    barmode='stack')

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No hay datos para mostrar")

    #%% 18.- Se realiza un conteo mensual acerca de la cantidad de coctel utilizado por región, dividido en redes, radio y tv. Además se muestra el combinado de todas las regiones
    st.subheader("13.- Conteo mensual de la cantidad de coctel utilizado por región, dividido en redes, radio y tv")

    col1, col2 = st.columns(2)
    with col1:
        year_inicio_g18 = st.selectbox("Año de inicio g13", anos, len(anos)-1)
        month_inicio_g18 = st.selectbox("Mes de inicio g13", list(range(1,13)), index=11)
    with col2:
        year_fin_g18 = st.selectbox("Año de fin g13", anos, index=len(anos)-1)
        month_fin_g18 = st.selectbox("Mes de fin g13", list(range(1,13)), index=11)

    # col1, col2 = st.columns(2)

    # with col1:
    #     year_inicio_g18 = st.selectbox("Año Inicio g13", list(range(2023, 2025)), index=0)
    #     month_inicio_g18 = st.selectbox("Mes Inicio g13", list(range(1, 13)), index=0)

    # with col2:
    #     year_fin_g18 = st.selectbox("Año Fin g13", list(range(2023, 2025)), index=1)
    #     month_fin_g18 = st.selectbox("Mes Fin g13", list(range(1, 13)), index=11)


    option_lugar_g18 = st.multiselect("Lugar g13",
                                    lugares_uniques,
                                    lugares_uniques)

    fecha_inicio_g18 = pd.to_datetime(f'{year_inicio_g18}-{month_inicio_g18}-01')
    fecha_fin_g18 = pd.to_datetime(f'{year_fin_g18}-{month_fin_g18}-01') + pd.offsets.MonthEnd(1)

    temp_g18 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g18) &
                                    (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g18) &
                                    (temp_coctel_fuente['lugar'].isin(option_lugar_g18))]

    non_numeric_coctel = temp_g18['coctel'].apply(lambda x: isinstance(x, str) or not pd.api.types.is_number(x))
    temp_g18.loc[non_numeric_coctel, 'coctel'] = 1.0

    if not temp_g18.empty:
        temp_g18 = temp_g18.copy()  # trabajar con una copia explícita
        temp_g18["mes"] = temp_g18['fecha_registro'].dt.month
        temp_g18["año"] = temp_g18['fecha_registro'].dt.year

        temp_g18 = temp_g18[["fecha_registro", "acontecimiento", "lugar", "id_fuente", "coctel", "mes", "año"]]

        temp_g18["Fuente"] = temp_g18["id_fuente"].map(id_fuente_dict)

        temp_g18 = temp_g18.dropna().drop_duplicates().reset_index(drop=True)

        temp_g18["año_mes"] = temp_g18["año"].astype(str) + "-" + temp_g18["mes"].astype(str)

        temp_g18_copy = temp_g18.copy()

        # Agrupar los datos por lugar, año_mes, y Fuente para contar el total de cocteles
        temp_g18 = temp_g18_copy.groupby(['lugar', 'año_mes', 'Fuente']).agg({'coctel': 'sum'}).reset_index()

        st.write(f"Conteo mensual de la cantidad de coctel utilizado por región, dividido en redes, radio y tv en {option_lugar_g18} entre {fecha_inicio_g18} y {fecha_fin_g18}")
        st.dataframe(temp_g18, hide_index=True, width=300)

        temp_g18 = temp_g18_copy.groupby(['año_mes', 'Fuente']).agg({'coctel': 'sum'}).reset_index()

        #graficando el conteo de cocteles por mes y fuente

        fig_18 = px.bar(temp_g18,
                        x='año_mes',
                        y='coctel',
                        color='Fuente',
                        barmode='stack',
                        title='Conteo de cocteles por mes y fuente',
                        labels={'año_mes': 'Año y Mes',
                                'coctel': 'Número de Cocteles',
                                'Fuente': 'Fuente'},
                        text='coctel', 
                        color_discrete_map={'Radio': '#3F6EC3', 'TV': '#A1A1A1', 'Redes': '#C00000'},
                        )

        st.plotly_chart(fig_18)

    else:
        st.warning("No hay datos para mostrar")

    #%% 20.- Se tiene cuadros divididos por radio y redes en el cual se muestran las notas en general que sean a favor ( a favor y mayormente a favor), neutral y en contra (en contra y mayormente en contra)
    st.subheader("14.- Porcentaje de notas que sean a favor, neutral y en contra")

    col1, col2, col3 = st.columns(3)

    with col1:
        fecha_inicio_g20 = st.date_input("Fecha Inicio g14",
                                        format="DD.MM.YYYY")

    with col2:
        fecha_fin_g20 = st.date_input("Fecha Fin g14",
                                    format="DD.MM.YYYY")
    with col3:
        option_nota_g20 = st.selectbox(
        "Notas g14",
        ("Con coctel", "Sin coctel", "Todos"))


    option_lugar_g20 = st.multiselect("Lugar g14",
                                    lugares_uniques,
                                    lugares_uniques)

    fecha_inicio_g20 = pd.to_datetime(fecha_inicio_g20, format='%Y-%m-%d')
    fecha_fin_g20 = pd.to_datetime(fecha_fin_g20, format='%Y-%m-%d')

    temp_g20 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g20) &
                                    (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g20) &
                                    (temp_coctel_fuente['lugar'].isin(option_lugar_g20))].dropna()

    if option_nota_g20 == "Con coctel":
        temp_g20 = temp_g20[temp_g20['coctel'] == 1]
        titulo_g20 = "Porcentaje de notas que sean a favor, neutral y en contra con coctel"
    elif option_nota_g20 == "Sin coctel":
        temp_g20 = temp_g20[temp_g20['coctel'] == 0]
        titulo_g20 = "Porcentaje de notas que sean a favor, neutral y en contra sin coctel" 
    else:
        titulo_g20 = "Porcentaje de notas que sean a favor, neutral y en contra"

    if not temp_g20.empty:

        temp_g20["mes"] = temp_g20['fecha_registro'].dt.month
        temp_g20["año"] = temp_g20['fecha_registro'].dt.year
        temp_g20["año_mes"] = temp_g20["año"].astype(str) + "-" + temp_g20["mes"].astype(str)

        temp_g20["a_favor"] = 0
        temp_g20["en_contra"] = 0
        temp_g20["neutral"] = 0


        temp_g20.loc[temp_g20['id_posicion'].isin([1, 2]), "a_favor"] = 1
        temp_g20.loc[temp_g20['id_posicion'].isin([4, 5]), "en_contra"] = 1
        temp_g20.loc[temp_g20['id_posicion'] == 3, "neutral"] = 1


        temp_g20 = temp_g20[["año_mes", "fecha_registro", "acontecimiento", "lugar", "a_favor", "en_contra", "neutral"]]


        conteo_notas_20 = temp_g20.groupby('año_mes').agg({'a_favor': 'sum',
                                                            'en_contra': 'sum',
                                                            'neutral': 'sum'}).reset_index()

        st.write(f"Porcentaje de notas que sean a favor, neutral y en contra en {option_lugar_g20} entre {fecha_inicio_g20.strftime('%d-%m-%Y')} y {fecha_fin_g20.strftime('%d-%m-%Y')}")

        conteo_notas_20_pct = conteo_notas_20.copy()

        conteo_notas_20_pct["total"] = conteo_notas_20_pct["a_favor"] + conteo_notas_20_pct["en_contra"] + conteo_notas_20_pct["neutral"]

        #2 decimales
        conteo_notas_20_pct["a_favor_pct"] = ((conteo_notas_20_pct['a_favor'] / conteo_notas_20_pct['total']) * 100).round(1)
        conteo_notas_20_pct["en_contra_pct"] = ((conteo_notas_20_pct['en_contra'] / conteo_notas_20_pct['total']) * 100).round(1)
        conteo_notas_20_pct["neutral_pct"] = ((conteo_notas_20_pct['neutral'] / conteo_notas_20_pct['total']) * 100).round(1)

        conteo_notas_20_pct = conteo_notas_20_pct[["año_mes", "a_favor_pct", "en_contra_pct", "neutral_pct"]]
        conteo_notas_20_pct=conteo_notas_20_pct.dropna()
        long_df = conteo_notas_20_pct.melt(id_vars=["año_mes"], var_name="Tipo de Nota", value_name="Porcentaje")
        
        fig_20 = px.bar(
            long_df,
            x="año_mes",
            y="Porcentaje",
            color="Tipo de Nota",
            barmode="stack",
            title=titulo_g20,
            labels={"año_mes": "Año y Mes", "Porcentaje": "Porcentaje"},
            text=long_df["Porcentaje"].map("{:.1f}%".format) if mostrar_todos else None,
            color_discrete_map={
                "a_favor_pct": "blue",
                "en_contra_pct": "red",
                "neutral_pct": "gray",
            },
        )

        fig_20.update_layout(barmode='stack', xaxis={'categoryorder': 'category ascending'})

        fig_20.for_each_trace(lambda t: t.update(name=t.name.replace('_pct', ' (%)')))

        conteo_notas_20_pct["a_favor_pct"] = conteo_notas_20_pct["a_favor_pct"].map("{:.1f}".format)
        conteo_notas_20_pct["en_contra_pct"] = conteo_notas_20_pct["en_contra_pct"].map("{:.1f}".format)
        conteo_notas_20_pct["neutral_pct"] = conteo_notas_20_pct["neutral_pct"].map("{:.1f}".format)

        conteo_notas_20_pct = conteo_notas_20_pct.rename(columns={
            "a_favor_pct": "A favor (%)",
            "en_contra_pct": "En contra (%)",
            "neutral_pct": "Neutral (%)"
        })

        st.dataframe(conteo_notas_20_pct, hide_index=True)

        st.plotly_chart(fig_20)
        st.write("Los porcentajes se calcularon sobre el total de notas considerando coctel y otras fuentes")

    else:
        st.warning("No hay datos para mostrar")

    #%% 22.- Se realizan gráficos de tendencia de los mensajes emitidos por radio o redes por alguna región, por ejemplo:
    st.subheader("15.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específica")

    col1, col2 = st.columns(2) 
    with col1:
        fecha_inicio_g22 = st.date_input(
        "Fecha Inicio g15",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g22 = st.date_input(
        "Fecha Fin g15",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)
    with col1:
        option_fuente_g22 = st.selectbox(
        "Fuente g15",
        ("Radio", "TV", "Redes","Todos"),)
    with col2:
        option_lugar_g22 = st.selectbox(
        "Lugar g15",
        lugares_uniques)
    with col3:
        option_nota_g22 = st.selectbox(
        "Nota g15",
        ("Con coctel", "Sin coctel", "Todos")
        )

    fecha_inicio_g22 = pd.to_datetime(fecha_inicio_g22,format='%Y-%m-%d')
    fecha_fin_g22 = pd.to_datetime(fecha_fin_g22,format='%Y-%m-%d')

    temp_g22 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_g22) & 
                                (temp_coctel_fuente['fecha_registro']<=fecha_fin_g22) &
                                (temp_coctel_fuente['lugar']==option_lugar_g22)]

    if option_nota_g22 == 'Con coctel':
        temp_g22 = temp_g22[temp_g22['coctel'] == 1]
        titulo = f"Proporción de mensajes emitidos con coctel por {option_fuente_g22} en {option_lugar_g22} entre {fecha_inicio_g22.strftime('%d-%m-%Y')} y {fecha_fin_g22.strftime('%d-%m-%Y')}"
    elif option_nota_g22 == 'Sin coctel':
        temp_g22 = temp_g22[temp_g22['coctel'] == 0]
        titulo = f"Proporción de mensajes emitidos sin coctel por {option_fuente_g22} en {option_lugar_g22} entre {fecha_inicio_g22.strftime('%d-%m-%Y')} y {fecha_fin_g22.strftime('%d-%m-%Y')}"
    else:
        titulo = f"Proporción de mensajes emitidos por {option_fuente_g22} en {option_lugar_g22} entre {fecha_inicio_g22.strftime('%d-%m-%Y')} y {fecha_fin_g22.strftime('%d-%m-%Y')}"

    temp_g22 = temp_g22[temp_g22['id_fuente'] == {'Radio': 1, 'TV': 2, 'Redes': 3}.get(option_fuente_g22, temp_g22['id_fuente'])]
    temp_g22 = temp_g22.groupby('id_posicion')["id"].count().reset_index()
    temp_g22 = temp_g22.rename(columns={'id': 'frecuencia'})
    temp_g22["id_posicion"] = temp_g22["id_posicion"].map(id_posicion_dict)
    temp_g22['porcentaje'] = temp_g22['frecuencia'] / temp_g22['frecuencia'].sum()
    temp_g22['porcentaje'] = temp_g22['porcentaje'].apply(lambda x: "{:.2%}".format(x))
    temp_g22 = temp_g22.reset_index(drop=True)

    if not temp_g22.empty:
        fig_22 = px.pie(
            temp_g22,
            values='frecuencia',
            names='id_posicion',
            title=titulo,
            width=600,
            hole=0.3,
            color='id_posicion',  
            color_discrete_map=color_posicion_dict 
        )

        st.plotly_chart(fig_22, use_container_width=True)
        st.dataframe(temp_g22, hide_index=True)
    else:
        st.warning("No hay datos para mostrar")


    #%% 23.- Se realizan gráficas de los mensajes emitidos por tema y en estos también se subdividen si son de tipo neutral, a favor, en contra, etc.
    st.subheader("16.- Recuento de Mensajes Emitidos por Tema y Tipo de Nota en Lugar y Fecha Específica")
    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio_g23 = st.date_input(
        "Fecha Inicio g16",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g23 = st.date_input(
        "Fecha Fin g16",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)
    with col1:
        option_fuente_g23 = st.selectbox(
        "Fuente g16",
        ("Radio", "TV", "Redes","Todos"))
    with col2:
        option_lugar_g23 = st.selectbox(
        "Lugar g16",
        lugares_uniques)
    with col3:
        option_nota_g23 = st.selectbox(
        "Nota g16",
        ("Con coctel", "Sin coctel", "Todos")
        )

    fecha_inicio_g23 = pd.to_datetime(fecha_inicio_g23,format='%Y-%m-%d')
    fecha_fin_g23 = pd.to_datetime(fecha_fin_g23,format='%Y-%m-%d')

    temp_g23 = temp_coctel_temas[(temp_coctel_temas['fecha_registro']>=fecha_inicio_g23) &
                                    (temp_coctel_temas['fecha_registro']<=fecha_fin_g23) &
                                    (temp_coctel_temas['lugar']==option_lugar_g23)]

    if option_nota_g23 == 'Con coctel':
        temp_g23 = temp_g23[temp_g23['coctel'] == 1]
        titulo = f"Recuento de mensajes emitidos con coctel por {option_fuente_g23} en {option_lugar_g23} entre {fecha_inicio_g23.strftime('%d-%m-%Y')} y {fecha_fin_g23.strftime('%d-%m-%Y')}"
    elif option_nota_g23 == 'Sin coctel':
        temp_g23 = temp_g23[temp_g23['coctel'] == 0]
        titulo = f"Recuento de mensajes emitidos sin coctel por {option_fuente_g23} en {option_lugar_g23} entre {fecha_inicio_g23.strftime('%d-%m-%Y')} y {fecha_fin_g23.strftime('%d-%m-%Y')}"
    else:
        titulo = f"Recuento de mensajes emitidos por {option_fuente_g23} en {option_lugar_g23} entre {fecha_inicio_g23.strftime('%d-%m-%Y')} y {fecha_fin_g23.strftime('%d-%m-%Y')}"

    if option_fuente_g23 == 'Radio':
        temp_g23 = temp_g23[temp_g23['id_fuente']==1]
    elif option_fuente_g23 == 'TV':
        temp_g23 = temp_g23[temp_g23['id_fuente']==2]
    elif option_fuente_g23 == 'Redes':
        temp_g23 = temp_g23[temp_g23['id_fuente']==3]

    if not temp_g23.empty:
        temp_g23["id_posicion"] = temp_g23["id_posicion"].map(id_posicion_dict)

        df_grouped = temp_g23.groupby(['descripcion', 'id_posicion']).size().reset_index(name='frecuencia')

        top_10_temas = df_grouped.groupby('descripcion')['frecuencia'].sum().nlargest(10).index
        df_top_10 = df_grouped[df_grouped['descripcion'].isin(top_10_temas)]

        fig_23 = px.bar(
            df_top_10,
            x='descripcion',
            y='frecuencia',
            title=titulo,
            color='id_posicion',
            text='frecuencia',
            barmode='stack',
            labels={'frecuencia': 'Frecuencia', 'descripcion': 'Tema', 'id_posicion': 'Posición'},
            category_orders={'descripcion': top_10_temas},
            color_discrete_map=color_posicion_dict,
            height=500
        )

        st.plotly_chart(fig_23, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")


    #%% 24.- Proporción de mensajes emitidos por tema en lugar y fecha específica
    st.subheader("17.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específicos")

    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio_g24 = st.date_input(
        "Fecha Inicio g17",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g24 = st.date_input(
        "Fecha Fin g17",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)    
    with col1:
        option_fuente_g24 = st.selectbox(
        "Fuente g17",
        ("Radio", "TV", "Redes","Todos"),)

    with col2:
        option_lugar_g24 = st.selectbox(
        "Lugar g17",
        lugares_uniques)

    with col3:
        option_nota_g24 = st.selectbox(
        "Nota g17",
        ("Con coctel", "Sin coctel", "Todos")
        )

    fecha_inicio_g24 = pd.to_datetime(fecha_inicio_g24,format='%Y-%m-%d')
    fecha_fin_g24 = pd.to_datetime(fecha_fin_g24,format='%Y-%m-%d')

    temp_g24 = temp_coctel_temas[(temp_coctel_temas['fecha_registro']>=fecha_inicio_g24) &
                                    (temp_coctel_temas['fecha_registro']<=fecha_fin_g24) &
                                    (temp_coctel_temas['lugar']==option_lugar_g24)]

    if option_nota_g24 == 'Con coctel':
        temp_g24 = temp_g24[temp_g24['coctel'] == 1]
        titulo = f"Proporción de mensajes emitidos con coctel por {option_fuente_g24} en {option_lugar_g24} entre {fecha_inicio_g24.strftime('%d-%m-%Y')} y {fecha_fin_g24.strftime('%d-%m-%Y')}"
    elif option_nota_g24 == 'Sin coctel':
        temp_g24 = temp_g24[temp_g24['coctel'] == 0]
        titulo = f"Proporción de mensajes emitidos sin coctel por {option_fuente_g24} en {option_lugar_g24} entre {fecha_inicio_g24.strftime('%d-%m-%Y')} y {fecha_fin_g24.strftime('%d-%m-%Y')}"
    else:
        titulo = f"Proporción de mensajes emitidos por {option_fuente_g24} en {option_lugar_g24} entre {fecha_inicio_g24.strftime('%d-%m-%Y')} y {fecha_fin_g24.strftime('%d-%m-%Y')}"

    if option_fuente_g24 == 'Radio':
        temp_g24 = temp_g24[temp_g24['id_fuente']==1]
    elif option_fuente_g24 == 'TV':
        temp_g24 = temp_g24[temp_g24['id_fuente']==2]
    elif option_fuente_g24 == 'Redes':
        temp_g24 = temp_g24[temp_g23['id_fuente']==3]

    # solo agfrupar por top 10 temas y porcentajes
    if not temp_g24.empty:
        df_grouped_24 = temp_g24.groupby(['descripcion']).size().reset_index(name='frecuencia')
        top_10_temas_24 = df_grouped_24.nlargest(10, 'frecuencia')['descripcion']
        df_top_10_24 = df_grouped_24[df_grouped_24['descripcion'].isin(top_10_temas_24)]

        df_top_10_24['porcentaje'] = df_top_10_24['frecuencia']/df_grouped_24['frecuencia'].sum()
        df_top_10_24["porcentaje"] = df_top_10_24["porcentaje"]*100
        # df_top_10_24['porcentaje'] = df_top_10_24['porcentaje'].apply(lambda x:"{:.2f}".format(x))

        fig_24 = px.bar(
            df_top_10_24,
            x="porcentaje",
            y="descripcion",
            title=titulo,
            orientation='h',
            text=df_top_10_24["porcentaje"].map(lambda x: f"{x:.1f}") if mostrar_todos else None,  # Muestra valores si mostrar_todos es True
            labels={'porcentaje': 'Porcentaje %', 'descripcion': 'Temas'}
        )

        fig_24.update_traces(
            textposition="outside" if mostrar_todos else "none"  # Posiciona el texto fuera si mostrar_todos es True
        )

        fig_24.update_layout(yaxis={'categoryorder': 'total ascending'})

        st.plotly_chart(fig_24, use_container_width=True)


    else:
        st.warning("No hay datos para mostrar")

    #%% 25.- Se busca conocer la tendencia de las notas emitidas ya sean de coctel o no o combinadas por radio o redes
    st.subheader("18.- Tendencia de las notas emitidas en lugar y fecha específica por fuente y tipo de nota")

    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio_g25 = st.date_input(
        "Fecha Inicio g18",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g25 = st.date_input(
        "Fecha Fin g18",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)

    with col1:
        option_fuente_g25 = st.selectbox(
        "Fuente g18",
        ("Radio", "TV", "Redes"),)

    with col2:
        option_lugar_g25 = st.selectbox(
        "Lugar g18",
        lugares_uniques)

    with col3:
        option_nota_g25 = st.selectbox(
        "Nota g18",
        ("Con coctel", "Sin coctel", "Todos")
        )


    fecha_inicio_g25 = pd.to_datetime(fecha_inicio_g25,format='%Y-%m-%d')
    fecha_fin_g25 = pd.to_datetime(fecha_fin_g25,format='%Y-%m-%d')

    temp_g25_medio = temp_coctel_fuente_programas[(temp_coctel_fuente_programas['fecha_registro']>=fecha_inicio_g25) &
                                                (temp_coctel_fuente_programas['fecha_registro']<=fecha_fin_g25) &
                                                (temp_coctel_fuente_programas['lugar']==option_lugar_g25)]

    temp_g25_redes = temp_coctel_fuente_fb[(temp_coctel_fuente_fb['fecha_registro']>=fecha_inicio_g25) &
                                            (temp_coctel_fuente_fb['fecha_registro']<=fecha_fin_g25) &
                                            (temp_coctel_fuente_fb['lugar']==option_lugar_g25)]

    if option_nota_g25 == 'Con coctel':
        temp_g25_medio = temp_g25_medio[temp_g25_medio['coctel'] == 1]
        temp_g25_redes = temp_g25_redes[temp_g25_redes['coctel'] == 1]
        titulo = f"Tendencia de las notas emitidas con coctel por {option_fuente_g25} en {option_lugar_g25} entre {fecha_inicio_g25.strftime('%d-%m-%Y')} y {fecha_fin_g25.strftime('%d-%m-%Y')}"
    elif option_nota_g25 == 'Sin coctel':
        temp_g25_medio = temp_g25_medio[temp_g25_medio['coctel'] == 0]
        temp_g25_redes = temp_g25_redes[temp_g25_redes['coctel'] == 0]
        titulo = f"Tendencia de las notas emitidas sin coctel por {option_fuente_g25} en {option_lugar_g25} entre {fecha_inicio_g25.strftime('%d-%m-%Y')} y {fecha_fin_g25.strftime('%d-%m-%Y')}"
    else:
        titulo = f"Tendencia de las notas emitidas por {option_fuente_g25} en {option_lugar_g25} entre {fecha_inicio_g25.strftime('%d-%m-%Y')} y {fecha_fin_g25.strftime('%d-%m-%Y')}"

    if option_fuente_g25 == 'Radio':
        temp_g25_medio = temp_g25_medio[temp_g25_medio['id_fuente']==1]
    elif option_fuente_g25 == 'TV':
        temp_g25_medio = temp_g25_medio[temp_g25_medio['id_fuente']==2]
    elif option_fuente_g25 == 'Redes':
        temp_g25_redes = temp_g25_redes[temp_g25_redes['id_fuente']==3]


    if option_fuente_g25 == "Redes" and not temp_g25_redes.empty:
        df_grouped_25_redes = temp_g25_redes.groupby(['nombre_facebook_page', 'id_posicion']).size().reset_index(name='frecuencia')
        df_grouped_25_redes = df_grouped_25_redes.sort_values(by='frecuencia', ascending=False)
        df_grouped_25_redes['id_posicion'] = df_grouped_25_redes['id_posicion'].map(id_posicion_dict)
        
        fig_25_redes = px.bar(df_grouped_25_redes,
                            x='nombre_facebook_page',
                            y='frecuencia',
                            title = titulo, 
                            color='id_posicion',
                            barmode='stack',
                            labels={'frecuencia': 'Frecuencia', 'nombre_facebook_page': 'Pagina Facebook', 'id_posicion': 'Posición'},
                            color_discrete_map=color_posicion_dict,
                            text = 'frecuencia'
                            )
        fig_25_redes.update_layout(height=600)
        st.plotly_chart(fig_25_redes, use_container_width=True)

    elif option_fuente_g25 != "Redes" and not temp_g25_medio.empty:
        df_grouped_25_medio = temp_g25_medio.groupby(['nombre_canal', 'id_posicion']).size().reset_index(name='frecuencia')
        df_grouped_25_medio = df_grouped_25_medio.sort_values(by='frecuencia', ascending=False)
        df_grouped_25_medio['id_posicion'] = df_grouped_25_medio['id_posicion'].map(id_posicion_dict)
        
        fig_25_medio = px.bar(df_grouped_25_medio,
                            x='nombre_canal',
                            y='frecuencia',
                            color='id_posicion',
                            title = titulo,
                            barmode='stack',
                            color_discrete_map=color_posicion_dict,
                            labels={'frecuencia': 'Frecuencia', 'nombre_canal': 'Canal', 'id_posicion': 'Posición'},
                            text = 'frecuencia',
                            )
        st.plotly_chart(fig_25_medio, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")


    #%% 26.- Se busca conocer las noticias emitidas en un cierto rango de tiempo cuantos son a favor, en contra, neutral, etc
    st.subheader("19.- Notas emitidas en un rango de tiempo segun posicion y coctel")

    col1, col2, col3 = st.columns(3)

    with col1:
        fecha_inicio_g26 = st.date_input(
        "Fecha Inicio g19",
        format="DD.MM.YYYY")

    with col2:
        fecha_fin_g26 = st.date_input(
        "Fecha Fin g19",
        format="DD.MM.YYYY")

    with col3:
        option_nota_g26 = st.selectbox(
            "Nota g19",
            ("Con coctel", "Sin coctel", "Todos")
            )

    fecha_inicio_g26 = pd.to_datetime(fecha_inicio_g26,format='%Y-%m-%d')
    fecha_fin_g26 = pd.to_datetime(fecha_fin_g26,format='%Y-%m-%d')

    temp_g26 = temp_coctel_temas[(temp_coctel_temas['fecha_registro']>=fecha_inicio_g26) &
                                    (temp_coctel_temas['fecha_registro']<=fecha_fin_g26)]

    if option_nota_g26 == 'Con coctel':
        temp_g26 = temp_g26[temp_g26['coctel'] == 1]
        titulo = f"Notas emitidas con coctel entre {fecha_inicio_g26.strftime('%d-%m-%Y')} y {fecha_fin_g26.strftime('%d-%m-%Y')} según posición"
    elif option_nota_g26 == 'Sin coctel':
        temp_g26 = temp_g26[temp_g26['coctel'] == 0]
        titulo = f"Notas emitidas sin coctel entre {fecha_inicio_g26.strftime('%d-%m-%Y')} y {fecha_fin_g26.strftime('%d-%m-%Y')} según posición"
    else:
        titulo = f"Notas emitidas entre {fecha_inicio_g26.strftime('%d-%m-%Y')} y {fecha_fin_g26.strftime('%d-%m-%Y')} según posición"

    if not temp_g26.empty:

        df_grouped_26 = temp_g26.groupby(['id_posicion']).size().reset_index(name='frecuencia')

        df_grouped_26['id_posicion'] = df_grouped_26['id_posicion'].map(id_posicion_dict)

        fig_26 = px.bar(df_grouped_26,
                        x='id_posicion',
                        y='frecuencia',
                        title = titulo,
                        labels={'frecuencia': 'Frecuencia', 'id_posicion': 'Posición'},
                        color='id_posicion',
                        color_discrete_map=color_posicion_dict,
                        text='frecuencia'
                        )

        st.plotly_chart(fig_26, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% 27.- grafico de barras sobre actores y posiciones
    st.subheader("20.- Recuento de posiciones emitidas por actor en lugar y fecha específica")

    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio_g27 = st.date_input(
        "Fecha Inicio g20",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g27 = st.date_input(
        "Fecha Fin g20",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)

    with col1:
        option_fuente_g27 = st.selectbox(
        "Fuente g20",
        ("Radio", "TV", "Redes","Todos"),)

    with col2:
        option_lugar_g27 = st.selectbox(
        "Lugar g20",
        lugares_uniques)

    with col3:
        option_nota_g27 = st.selectbox(
            "Nota g20",
            ("Con coctel", "Sin coctel", "Todos"))


    fecha_inicio_g27 = pd.to_datetime(fecha_inicio_g27,format='%Y-%m-%d')
    fecha_fin_g27 = pd.to_datetime(fecha_fin_g27,format='%Y-%m-%d')

    temp_g27 = temp_coctel_fuente_actores[(temp_coctel_fuente_actores['fecha_registro']>=fecha_inicio_g27) &
                                    (temp_coctel_fuente_actores['fecha_registro']<=fecha_fin_g27) &
                                    (temp_coctel_fuente_actores['lugar']==option_lugar_g27)]

    if option_fuente_g27 == 'Radio':
        temp_g27 = temp_g27[temp_g27['id_fuente']==1]
    elif option_fuente_g27 == 'TV':
        temp_g27 = temp_g27[temp_g27['id_fuente']==2]
    elif option_fuente_g27 == 'Redes':
        temp_g27 = temp_g27[temp_g27['id_fuente']==3]

    if option_nota_g27 == 'Con coctel':
        temp_g27 = temp_g27[temp_g27['coctel'] == 1]
        titulo = f"Recuento de posiciones emitidas por actor en {option_lugar_g27} entre {fecha_inicio_g27.strftime('%d-%m-%Y')} y {fecha_fin_g27.strftime('%d-%m-%Y')}. Notas con coctel"
    elif option_nota_g27 == 'Sin coctel':
        temp_g27 = temp_g27[temp_g27['coctel'] == 0]
        titulo = f"Recuento de posiciones emitidas por actor en {option_lugar_g27} entre {fecha_inicio_g27.strftime('%d-%m-%Y')} y {fecha_fin_g27.strftime('%d-%m-%Y')}. Notas sin coctel"

    else:
        titulo = f"Recuento de posiciones emitidas por actor en {option_lugar_g27} entre {fecha_inicio_g27.strftime('%d-%m-%Y')} y {fecha_fin_g27.strftime('%d-%m-%Y')}"


    if not temp_g27.empty:
        temp_g27["posicion"] = temp_g27["id_posicion"].map(id_posicion_dict)
        temp_g27 = temp_g27[temp_g27["nombre"] != "periodista"]
        
        df_grouped_27 = temp_g27.groupby(['nombre', 'posicion']).size().reset_index(name='frecuencia')

        top_10_actores = df_grouped_27.groupby('nombre')['frecuencia'].sum().nlargest(10).index
        df_top_10_27 = df_grouped_27[df_grouped_27['nombre'].isin(top_10_actores)]

        fig_27 = px.bar(df_top_10_27,
                        x='nombre',
                        y='frecuencia',
                        title = titulo,
                        color='posicion',
                        barmode='stack',
                        color_discrete_map={'a favor': 'blue', 'potencialmente a favor': 'lightblue', 'neutral': 'gray', 'potencialmente en contra': 'orange', 'en contra': 'red'},
                        labels={'frecuencia': 'Frecuencia', 'nombre': 'Actor', 'posicion': 'Posición'},
                        category_orders={'nombre': top_10_actores},
                        text='frecuencia'
                        )

        st.plotly_chart(fig_27, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% 28.- gráfico de barras sobre actores y posiciones
    st.subheader("21.- Porcentaje de cóctel de todos los medios")

    col1, col2 = st.columns(2)
    with col1:
        ano_inicio_g28 = st.selectbox("Año de inicio g21", anos, len(anos)-1)
        mes_inicio_g28 = st.selectbox("Mes de inicio g21", meses, index=11)
    with col2:
        ano_fin_g28 = st.selectbox("Año de fin g21", anos, index=len(anos)-1)
        mes_fin_g28 = st.selectbox("Mes de fin g21", meses, index=11)

    col1, col2 = st.columns(2)
    with col1:
        option_regiones_g28 = st.multiselect(
            "Lugar g21",
            temp_coctel_fuente['lugar'].unique().tolist(),
            default=temp_coctel_fuente['lugar'].unique().tolist()
        )

    fecha_inicio_g28 = pd.to_datetime(f"{ano_inicio_g28}-{meses.index(mes_inicio_g28) + 1}-01")
    fecha_fin_g28 = pd.to_datetime(f"{ano_fin_g28}-{meses.index(mes_fin_g28) + 1}-01")  

    temp_g28 = temp_coctel_fuente.copy()
    temp_g28['fecha_registro'] = pd.to_datetime(temp_g28['fecha_registro'])
    temp_g28['fecha_mes'] = temp_g28['fecha_registro'].dt.to_period('M').dt.to_timestamp()
    temp_g28 = temp_g28[
        (temp_g28['fecha_mes'] >= fecha_inicio_g28) & 
        (temp_g28['fecha_mes'] <= fecha_fin_g28) &
        (temp_g28['lugar'].isin(option_regiones_g28))
        ]

    # fuentes_disponibles = ['Radio', 'Redes', 'TV']
    # fuentes_seleccionadas = st.multiselect("Selecciona las fuentes a mostrar", fuentes_disponibles, default=fuentes_disponibles)

    if not temp_g28.empty:
        temp_g28 = temp_g28.groupby(['id_fuente', 'lugar']).agg({'coctel': 'sum'}).reset_index()
        total_por_lugar = temp_g28.groupby(['lugar'])['coctel'].transform('sum')
        temp_g28['Fuente'] = temp_g28['id_fuente'].map(id_fuente_dict)
        temp_g28['porcentaje_coctel'] = (temp_g28['coctel'] / total_por_lugar) * 100
        temp_g28 = temp_g28.dropna()
        
        # temp_g28 = temp_g28[temp_g28['Fuente'].isin(fuentes_seleccionadas)]
        
        promedios = temp_g28.groupby('Fuente')['porcentaje_coctel'].mean().to_dict()
        
        fig = px.bar(
            temp_g28,
            x="lugar",
            y="porcentaje_coctel",
            color="Fuente",
            barmode="group",
            title=f"Porcentaje de cóctel de todos los medios - {fecha_inicio_g28.strftime("%Y-%m")} hasta {fecha_fin_g28.strftime("%Y-%m")}",
            labels={"lugar": "Regiones", "porcentaje_coctel": "Porcentaje de Cóctel"},
            text=temp_g28["porcentaje_coctel"].map("{:.1f}%".format) if mostrar_todos else None,  # Muestra valores si mostrar_todos es True
            color_discrete_map={"Radio": "blue", "Redes": "red", "TV": "gray"},
        )

        fig.update_layout(
            font=dict(size=8)
        )

        fig.update_traces(
            textposition="outside" if mostrar_todos else "none"
        )

        # Agregar líneas de promedio por fuente
        for fuente, promedio in promedios.items():
            fig.add_hline(
                y=promedio,
                line_dash="dash",
                annotation_text=f"Promedio de {fuente}: {promedio:.2f}%",
                annotation_position="right",
                line_color={"Radio": "blue", "Redes": "orange", "TV": "gray"}[fuente],
            )

        st.plotly_chart(fig)

    else:
        st.warning("No hay datos para mostrar")

    #%% 29.- gráfico de barras sobre porcentaje de cóctel en los últimos 3 meses por fuente
    st.subheader("22.- Porcentaje de cóctel en los últimos 3 meses por fuente")

    col1, col2 = st.columns(2)
    with col1:
        ano_fin_g29 = st.selectbox("Año de referencia g22", anos, index=len(anos)-1)
        mes_fin_g29 = st.selectbox("Mes de referencia g22", meses, index=11)

    option_regiones_g29 = st.multiselect(
        "Lugar g22",
        temp_coctel_fuente['lugar'].unique().tolist(),
        default=temp_coctel_fuente['lugar'].unique().tolist()
    )

    fuentes_disponibles = ['Radio', 'Redes', 'TV']
    fuente_g29 = st.selectbox("Fuente g22", fuentes_disponibles)

    fecha_fin_g29 = pd.to_datetime(f"{ano_fin_g29}-{meses.index(mes_fin_g29) + 1}-01")
    fecha_inicio_g29 = fecha_fin_g29 - pd.DateOffset(months=2)  # Retrocede 2 meses para incluir 3 en total

    temp_g29 = temp_coctel_fuente.copy()
    temp_g29['fecha_registro'] = pd.to_datetime(temp_g29['fecha_registro'])
    temp_g29['fecha_mes'] = temp_g29['fecha_registro'].dt.to_period('M').dt.to_timestamp()

    temp_g29 = temp_g29[
        (temp_g29['fecha_mes'] >= fecha_inicio_g29) & 
        (temp_g29['fecha_mes'] <= fecha_fin_g29) & 
        (temp_g29['lugar'].isin(option_regiones_g29))
    ]

    temp_g29['Fuente'] = temp_g29['id_fuente'].map(id_fuente_dict)
    temp_g29 = temp_g29[temp_g29['Fuente'] == fuente_g29]

    if not temp_g29.empty:
        temp_g29 = temp_g29.groupby(['fecha_mes', 'lugar'], as_index=False).agg({'coctel': 'sum'})
        total_por_lugar = temp_g29.groupby(['fecha_mes'])['coctel'].transform('sum')
        temp_g29['porcentaje_coctel'] = (temp_g29['coctel'] / total_por_lugar) * 100
        temp_g29 = temp_g29.dropna()

        # Mapeo de meses en español
        meses_es = {
            "January": "Enero",
            "February": "Febrero",
            "March": "Marzo",
            "April": "Abril",
            "May": "Mayo",
            "June": "Junio",
            "July": "Julio",
            "August": "Agosto",
            "September": "Septiembre",
            "October": "Octubre",
            "November": "Noviembre",
            "December": "Diciembre"
        }

        # Mostrar solo el nombre del mes en la leyenda y traducirlo
        temp_g29['mes'] = temp_g29['fecha_mes'].dt.strftime('%B').map(meses_es)

        # Verificar que haya suficientes meses únicos para asignar colores
        unique_months = temp_g29['mes'].unique()

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
        else:
            st.warning("No hay datos disponibles para la selección actual.")
            st.stop()

        fig = px.bar(
            temp_g29,
            x="lugar",
            y="porcentaje_coctel",
            color="mes",
            barmode="group",
            title=f"Porcentaje de cóctel {fuente_g29} - Últimos 3 meses",
            labels={"lugar": "Región", "porcentaje_coctel": "Porcentaje de Cóctel", "mes": "Mes"},
            color_discrete_map=color_mapping,
            text=temp_g29["porcentaje_coctel"].map("{:.1f}%".format)
        )
        fig.update_layout(
            font=dict(size=15)
        )

        fig.update_traces(
            textposition="outside" if mostrar_todos else "none"
        )

        st.plotly_chart(fig)

    else:
        st.warning("No hay datos para mostrar en la selección actual.")

    #%% 30.- gráfico de barras sobre actores y posiciones
    st.subheader("23.- Gráfico Mensual Lineal sobre la evolución de Radio, Redes y TV")

    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio_g30 = st.date_input("Fecha Inicio g23", format="DD.MM.YYYY")
    with col2:
        fecha_fin_g30 = st.date_input("Fecha Fin g23", format="DD.MM.YYYY")

    col1, col2 = st.columns(2)
    with col1:
        option_lugar_g30 = st.multiselect(
            "Lugar g23",
            lugares_uniques,
            lugares_uniques
        )

    # Opción para mostrar valores en el gráfico
    mostrar_todos_g30 = st.toggle("Mostrar todos los valores", key="toggle_g30")

    fecha_inicio_g30 = pd.to_datetime(fecha_inicio_g30, format='%Y-%m-%d')
    fecha_fin_g30 = pd.to_datetime(fecha_fin_g30, format='%Y-%m-%d')

    temp_g30 = temp_coctel_fuente.copy()
    temp_g30['fecha_mes'] = temp_g30['fecha_registro'].dt.strftime('%Y-%m')
    temp_g30 = temp_g30[
        (temp_g30['fecha_registro'] >= fecha_inicio_g30) & 
        (temp_g30['fecha_registro'] <= fecha_fin_g30) & 
        (temp_g30['lugar'].isin(option_lugar_g30))
    ]

    temp_g30['Fuente'] = temp_g30['id_fuente'].map(id_fuente_dict)

    if not temp_g30.empty:
        temp_g30 = temp_g30[['coctel', 'fecha_mes', 'Fuente']]
        temp_g30 = temp_g30.groupby(['fecha_mes', 'Fuente'], as_index=False).agg({'coctel': 'sum'})

        total_g30 = temp_g30.groupby('fecha_mes', as_index=False).agg({'coctel': 'sum'})
        total_g30['Fuente'] = "Total"  # Crear la categoría "Total"

        temp_g30 = pd.concat([temp_g30, total_g30], ignore_index=True)

        fig = px.line(
            temp_g30,
            x='fecha_mes', 
            y='coctel', 
            color='Fuente',
            markers=True,  
            color_discrete_map={'Radio': 'gray', 'Redes': 'red', 'TV': 'blue', 'Total': 'green'},  # Línea Total en verde
            title="Gráfico Mensual Lineal sobre la evolución de Radio, Redes y TV",
            text=temp_g30["coctel"].map(str) if mostrar_todos_g30 else None  # Agregar valores solo si la opción está activada
        )

        fig.update_traces(textposition="top center")

        fig.update_layout(
            xaxis_title="Mes y Año",  
            yaxis_title="Impactos de Coctel", 
            xaxis=dict(tickangle=45, showgrid=False),  
            yaxis=dict(showgrid=True), 
            # plot_bgcolor="white",
            font=dict(size=12),
            margin=dict(l=50, r=50, t=50, b=50)
        )

        st.plotly_chart(fig)

    else:
        st.warning("No hay datos para mostrar")

    #%% 31.- gráfico de barras sobre actores y posiciones
    st.subheader("24.- Porcentaje de cocteles por mensajes fuerza")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g31 = st.date_input("Fecha Inicio 24",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g31 = st.date_input("Fecha Fin g24",
                                    format="DD.MM.YYYY")
    with col3:
        option_fuente_g31 = st.selectbox(
        "Fuente g24",
        ("Radio", "TV", "Redes","Todos"))
    with col4:
        option_nota_g31 = st.selectbox(
        "Nota g24",
        ("Con coctel", "Sin coctel", "Todos"))

    fecha_inicio_g31 = pd.to_datetime(fecha_inicio_g31,format='%Y-%m-%d')
    fecha_fin_g31 = pd.to_datetime(fecha_fin_g31,format='%Y-%m-%d')

    temp_g31 = temp_coctel_completo[['fecha_registro','coctel','mensaje_fuerza','id_fuente']].copy()
    temp_g31['Fuente'] = temp_g31['id_fuente'].map(id_fuente_dict)
    if option_fuente_g31 != 'Todos':
        temp_g31 = temp_g31[(temp_g31['fecha_registro']>=fecha_inicio_g31) &
                            (temp_g31['fecha_registro']<=fecha_fin_g31) &
                            (temp_g31['Fuente']==option_fuente_g31)]
    else:
        temp_g31 = temp_g31[(temp_g31['fecha_registro']>=fecha_inicio_g31) &
                            (temp_g31['fecha_registro']<=fecha_fin_g31)]        
        
    if option_nota_g31 == "Con coctel":
        temp_g31 = temp_g31[temp_g31['coctel'] == 1]
        titulo_g31 = "Porcentaje de notas por mensaje de fuerza con coctel"
    elif option_nota_g31 == "Sin coctel":
        temp_g31 = temp_g31[temp_g31['coctel'] == 0]
        titulo_g31 = "Porcentaje de notas por mensaje de fuerza sin coctel" 
    else:
        titulo_g31 = "Porcentaje de notas por mensaje de fuerza"
    
    temp_g31 = temp_g31[['coctel','id_fuente','mensaje_fuerza']]
    temp_g31=temp_g31.groupby(['mensaje_fuerza']).agg({'coctel':'count'}).reset_index()
    temp_g31['porcentaje'] = (temp_g31['coctel'] / temp_g31['coctel'].sum()) * 100
    temp_g31 = temp_g31.dropna()

    if not temp_g31.empty:
        fig = px.bar(
            temp_g31,
            x='coctel',
            y='mensaje_fuerza',
            orientation='h',
            text=temp_g31.apply(lambda row: f"{row['coctel']} ({row['porcentaje']:.1f}%)", axis=1) if mostrar_todos else None,
            labels={'coctel': '', 'mensaje_fuerza': ''},
            title=titulo_g31,
            color_discrete_sequence=['red']
        )
        fig.update_layout(
            font=dict(size=8)
        )
        fig.update_traces(
            textposition="outside" if mostrar_todos else "none"
        )
        st.plotly_chart(fig)

    else:
        st.warning("No hay datos para mostrar")