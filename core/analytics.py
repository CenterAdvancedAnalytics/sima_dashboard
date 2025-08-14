#analytics.py
import pandas as pd
from typing import Dict, Any, List, Tuple
import sys
import os

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.constants import ID_POSICION_DICT, COCTEL_DICT, ID_FUENTE_DICT, MACROREGIONES

class AnalyticsEngine:
    """Motor de análisis de datos completo con todas las funciones migradas"""
    
    # =====================================================
    # SECCIONES DE PROPORCIONES BÁSICAS
    # =====================================================
    
    @staticmethod
    def calculate_coctel_proportion(data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Calcular proporción de cocteles por fuente (Sección SN) - using S25 logic"""
        result = {}
        
        # Apply S25 logic: dedup first like S25 does
        data_deduped = data.drop_duplicates()
        
        print("=== SN DEBUG ===")
        print(f"total combined data rows: {len(data)}")
        print(f"after dedup: {len(data_deduped)}")
        print(f"unique id_fuente values: {sorted(data_deduped['id_fuente'].unique())}")
        print(f"total coctel=1 in deduped data: {data_deduped['coctel'].sum()}")
        
        print("ID_FUENTE_DICT mapping:")
        for k, v in ID_FUENTE_DICT.items():
            print(f"  {k} -> {v}")
        
        print("breakdown by id_fuente:")
        for fuente_id, fuente_name in ID_FUENTE_DICT.items():
            temp_data_subset = data_deduped[data_deduped['id_fuente'] == fuente_id]
            if not temp_data_subset.empty:
                coctel_ones = len(temp_data_subset[temp_data_subset['coctel'] == 1])
                coctel_zeros = len(temp_data_subset[temp_data_subset['coctel'] == 0])
                print(f"  {fuente_name} (id={fuente_id}): {len(temp_data_subset)} total, {coctel_ones} coctel=1, {coctel_zeros} coctel=0")
            else:
                print(f"  {fuente_name} (id={fuente_id}): no data")
        
        print("=== END SN DEBUG ===\n")
        
        data_deduped["Fuente"] = data_deduped["id_fuente"].map(ID_FUENTE_DICT)
        
        for fuente_id, fuente_name in ID_FUENTE_DICT.items():
            temp_data = data_deduped[data_deduped['id_fuente'] == fuente_id]
            if not temp_data.empty:
                # Use S25 logic: sum coctel values and count total records after dedup
                coctel_count = temp_data['coctel'].sum()  # matches S25's ("coctel", "sum")
                total_count = len(temp_data)  # matches S25's ("id", "count") after dedup
                otras_count = total_count - coctel_count
                
                # Create the SN format result
                result_data = []
                if otras_count > 0:
                    otras_prop = (otras_count / total_count * 100) if total_count > 0 else 0
                    result_data.append({
                        'Fuente': 'Otras Fuentes',
                        'Cantidad': otras_count,
                        'Proporción': f"{otras_prop:.0f}%"
                    })
                
                if coctel_count > 0:
                    coctel_prop = (coctel_count / total_count * 100) if total_count > 0 else 0
                    result_data.append({
                        'Fuente': 'Coctel Noticias', 
                        'Cantidad': coctel_count,
                        'Proporción': f"{coctel_prop:.0f}%"
                    })
                
                result[fuente_name] = pd.DataFrame(result_data)
            else:
                result[fuente_name] = pd.DataFrame()
        
        return result

    @staticmethod
    def calculate_coctel_proportion_combined(data: pd.DataFrame, sources: List[str], locations: List[str]) -> pd.DataFrame:
        """Calcular proporción combinada de cocteles (Sección 1)"""
        data["Fuente"] = data["id_fuente"].map(ID_FUENTE_DICT)
        
        if sources:
            data = data[data['Fuente'].isin(sources)]
            
        grouped = data.groupby('coctel').agg({'id': 'count'}).reset_index()
        if not grouped.empty:
            grouped = grouped.rename(columns={'coctel': 'Fuente', 'id': 'Cantidad'})
            grouped['Proporción'] = grouped['Cantidad'] / grouped['Cantidad'].sum()
            grouped['Proporción'] = grouped['Proporción'].map('{:.0%}'.format)
            grouped['Fuente'] = grouped['Fuente'].replace({0: 'Otras Fuentes', 1: 'Coctel Noticias'})
        return grouped

    @staticmethod
    def calculate_position_by_source(data: pd.DataFrame, coctel_type: str) -> pd.DataFrame:
        """Calcular posición por fuente (Sección 2)"""
        temp_colores = pd.DataFrame({
            'id_fuente': [1,1,1,1,1,2,2,2,2,2,3,3,3,3,3], 
            'Posicion': ['(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente',
                        '(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente',
                        '(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente'],
            'color': ['Azul','Rojo','Gris','Celeste','Naranja','Azul','Rojo','Gris','Celeste','Naranja',
                     'Azul','Rojo','Gris','Celeste','Naranja']
        })
        
        if coctel_type == 'Coctel noticias':
            temp_data = data[data['coctel']==1].groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()
        elif coctel_type == 'Otras fuentes':
            temp_data = data[data['coctel']==0].groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()
        else:
            temp_data = data.groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()
            
        if not temp_data.empty:
            temp_data = pd.merge(temp_colores, temp_data, how='left', on=['id_fuente','color'])
            temp_data['id'] = temp_data['id'].fillna(0)
            temp_data = temp_data.rename(columns={'id_fuente':'Medio','color':'Color','id':'Cantidad'})
            temp_data['Medio'] = temp_data['Medio'].replace({1:'RADIO',2:'TV',3:'REDES'})
            temp_data['Porcentaje'] = temp_data['Cantidad'] / temp_data.groupby('Medio')['Cantidad'].transform('sum')
            temp_data['Porcentaje'] = temp_data['Porcentaje'].fillna(0.0)
            temp_data['Porcentaje'] = temp_data['Porcentaje'].map('{:.0%}'.format)
        
        return temp_data

    # =====================================================
    # SECCIONES DE TENDENCIAS TEMPORALES
    # =====================================================

    @staticmethod
    def calculate_weekly_percentage(data: pd.DataFrame, option_fuente: str) -> pd.DataFrame:
        """Calcular porcentaje semanal de cocteles (Sección 3)"""
        if option_fuente == "Radio":
            data = data[data["id_fuente"] == 1]
        elif option_fuente == "TV":
            data = data[data["id_fuente"] == 2]
        elif option_fuente == "Redes":
            data = data[data["id_fuente"] == 3]
        
        if data.empty:
            return pd.DataFrame()
            
        data["semana"] = (
            data["fecha_registro"].dt.year.map(str) + "-" +
            data["fecha_registro"].dt.isocalendar().week.map(str)
        )
        
        data["viernes"] = data["fecha_registro"] + pd.to_timedelta(
            (4 - data["fecha_registro"].dt.weekday) % 7, unit="D"
        )
        
        grouped = data.groupby("semana", as_index=False).agg({
            "id": "count", 
            "coctel": "sum", 
            "fecha_registro": "first", 
            "viernes": "first"
        })
        
        grouped = grouped.rename(columns={"id": "Cantidad"})
        grouped["porcentaje"] = (grouped["coctel"] / grouped["Cantidad"]) * 100
        grouped = grouped.sort_values("fecha_registro")
        
        return grouped

    @staticmethod
    def calculate_weekly_favor_contra(data: pd.DataFrame, option_fuente: str) -> pd.DataFrame:
        """Calcular tendencia semanal a favor vs en contra (Sección 4)"""
        data["semana"] = (
            data["fecha_registro"].dt.year.map(str) + "-" +
            data["fecha_registro"].dt.isocalendar().week.map(str)
        )
        
        data["viernes"] = data["fecha_registro"] + pd.to_timedelta(
            (4 - data["fecha_registro"].dt.weekday) % 7, unit="D"
        )
        
        data["a_favor"] = (data["id_posicion"].isin([1, 2])).astype(int)
        data["en_contra"] = (data["id_posicion"].isin([4, 5])).astype(int)

        if option_fuente == "Radio":
            data = data[data["id_fuente"] == 1]
        elif option_fuente == "TV":
            data = data[data["id_fuente"] == 2]
        elif option_fuente == "Redes":
            data = data[data["id_fuente"] == 3]

        grouped = data.groupby("semana", as_index=False).agg({
            "id": "count",
            "a_favor": "sum",
            "en_contra": "sum",
            "fecha_registro": "first",
            "viernes": "first",
        })

        grouped = grouped.rename(columns={"id": "Cantidad"})
        grouped = grouped.sort_values("fecha_registro")
        grouped["a_favor"] = (grouped["a_favor"] / grouped["Cantidad"]) * 100
        grouped["en_contra"] = (grouped["en_contra"] / grouped["Cantidad"]) * 100
        
        return grouped

    @staticmethod
    def calculate_cumulative_percentage(data: pd.DataFrame, option_fuente: str) -> pd.DataFrame:
        """Calcular porcentaje acumulativo por lugar (Sección 5)"""
        if option_fuente == "Radio":
            data = data[data["id_fuente"] == 1]
        elif option_fuente == "TV":
            data = data[data["id_fuente"] == 2]
        elif option_fuente == "Redes":
            data = data[data["id_fuente"] == 3]

        if data.empty:
            return pd.DataFrame()

        data["semana"] = (
            data["fecha_registro"].dt.year.map(str) + "-" +
            data["fecha_registro"].dt.isocalendar().week.map(lambda x: f"{x:02}")
        )

        data["viernes"] = data["fecha_registro"] + pd.to_timedelta(
            (4 - data["fecha_registro"].dt.weekday) % 7, unit="D"
        )

        grouped = data.groupby(["semana", "lugar"], as_index=False).agg(
            coctel_mean=("coctel", "mean"), viernes=("viernes", "first")
        )

        grouped["coctel_mean"] = grouped["coctel_mean"] * 100
        
        return grouped

    @staticmethod
    def calculate_top_lugares(data: pd.DataFrame, option_fuente: str, top_n: int = 3) -> Tuple[pd.DataFrame, List[str]]:
        """Calcular top lugares por porcentaje de coctel (Sección Top 3)"""
        if option_fuente == "Radio":
            data = data[data["id_fuente"] == 1]
        elif option_fuente == "TV":
            data = data[data["id_fuente"] == 2]
        elif option_fuente == "Redes":
            data = data[data["id_fuente"] == 3]

        if data.empty:
            return pd.DataFrame(), []

        data["semana"] = (
            data["fecha_registro"].dt.year.map(str) + "-" +
            data["fecha_registro"].dt.isocalendar().week.map(lambda x: f"{x:02}")
        )

        data["viernes"] = data["fecha_registro"] + pd.to_timedelta(
            (4 - data["fecha_registro"].dt.weekday) % 7, unit="D"
        )

        grouped = data.groupby(["lugar", "semana"], as_index=False).agg(
            coctel=("coctel", "mean"), viernes=("viernes", "first")
        )

        last_week = grouped.sort_values("semana").groupby(["lugar"]).last().reset_index()
        top_lugares = last_week.sort_values("coctel", ascending=False).head(top_n).reset_index(drop=True)

        filtered_data = grouped[grouped["lugar"].isin(top_lugares["lugar"])]
        filtered_data["coctel"] = filtered_data["coctel"] * 100

        return filtered_data, top_lugares["lugar"].tolist()

    @staticmethod
    def calculate_top_medios(data_programas: pd.DataFrame, data_fb: pd.DataFrame, 
                           option_fuente: str, top_n: int = 3) -> pd.DataFrame:
        """Calcular top medios/canales por coctel (Sección 6)"""
        if option_fuente == "Redes":
            if data_fb.empty:
                return pd.DataFrame()
                
            data_fb["viernes"] = data_fb["fecha_registro"] + pd.to_timedelta(
                (4 - data_fb["fecha_registro"].dt.weekday) % 7, unit="D"
            )

            top_redes = data_fb.groupby(["nombre_facebook_page"], as_index=False).agg({"coctel": "mean"})
            top_redes = top_redes.sort_values("coctel", ascending=False).head(top_n)

            top_list = top_redes["nombre_facebook_page"].tolist()
            filtered_data = data_fb[data_fb["nombre_facebook_page"].isin(top_list)]
            result = filtered_data.groupby(["viernes", "nombre_facebook_page"], as_index=False).agg({"coctel": "mean"})
            result["coctel"] = result["coctel"] * 100
            result["nombre_medio"] = result["nombre_facebook_page"]
            
        else:  # Radio o TV
            if data_programas.empty:
                return pd.DataFrame()
                
            if option_fuente == "Radio":
                data_programas = data_programas[data_programas["id_fuente"] == 1]
            elif option_fuente == "TV":
                data_programas = data_programas[data_programas["id_fuente"] == 2]

            data_programas["viernes"] = data_programas["fecha_registro"] + pd.to_timedelta(
                (4 - data_programas["fecha_registro"].dt.weekday) % 7, unit="D"
            )

            top_medios = data_programas.groupby(["nombre_canal"], as_index=False).agg({"coctel": "mean"})
            top_medios = top_medios.sort_values("coctel", ascending=False).head(top_n)

            top_list = top_medios["nombre_canal"].tolist()
            filtered_data = data_programas[data_programas["nombre_canal"].isin(top_list)]
            result = filtered_data.groupby(["viernes", "nombre_canal"], as_index=False).agg({"coctel": "mean"})
            result["coctel"] = result["coctel"] * 100
            result["nombre_medio"] = result["nombre_canal"]

        return result

    @staticmethod
    def calculate_macroregion_growth(data: pd.DataFrame, option_fuente: str, macroregion: str) -> pd.DataFrame:
        """Calcular crecimiento por macroregión (Sección 7)"""
        if option_fuente == "Radio":
            data = data[data["id_fuente"] == 1]
        elif option_fuente == "TV":
            data = data[data["id_fuente"] == 2]
        elif option_fuente == "Redes":
            data = data[data["id_fuente"] == 3]

        if data.empty:
            return pd.DataFrame()

        data["semana"] = (
            data["fecha_registro"].dt.year.map(str) + "-" +
            data["fecha_registro"].dt.isocalendar().week.map(str)
        )
        data["viernes"] = data["fecha_registro"] + pd.to_timedelta(
            (4 - data["fecha_registro"].dt.weekday) % 7, unit="D"
        )
        
        grouped = data.groupby(["semana", "lugar"], as_index=False).agg(
            coctel_mean=("coctel", "mean"), viernes=("viernes", "first")
        ).reset_index()
        
        grouped["coctel_mean"] = grouped["coctel_mean"] * 100
        grouped = grouped[grouped["coctel_mean"] > 0]
        grouped = grouped.sort_values("semana")

        departamentos = MACROREGIONES.get(macroregion, [])
        filtered_data = grouped[grouped["lugar"].isin(departamentos)]
        
        return filtered_data

    # =====================================================
    # SECCIONES DE ANÁLISIS DE POSICIONES
    # =====================================================

    @staticmethod
    def calculate_position_count(data: pd.DataFrame, source: str, coctel_type: str) -> pd.DataFrame:
        """Contar posiciones por fuente y tipo de coctel (Sección 8)"""
        if coctel_type == 'Con coctel':
            data = data[data['coctel'] == 1]
        elif coctel_type == 'Sin coctel':
            data = data[data['coctel'] == 0]

        if source == 'Radio':
            data = data[data['id_fuente'] == 1]
        elif source == 'TV':
            data = data[data['id_fuente'] == 2]
        elif source == 'Redes':
            data = data[data['id_fuente'] == 3]

        if data.empty:
            return pd.DataFrame()

        data['semana'] = data['fecha_registro'].dt.isocalendar().year.map(str) + '-' + data['fecha_registro'].dt.isocalendar().week.map(str)
        conteo_total = data.groupby(['id_posicion', 'id_fuente']).size().reset_index(name='count')
        conteo_total['Posición'] = conteo_total['id_posicion'].map(ID_POSICION_DICT)
        conteo_total['Tipo de Medio'] = conteo_total['id_fuente'].map(ID_FUENTE_DICT)
        conteo_total = conteo_total.dropna()

        return conteo_total

    @staticmethod
    def calculate_position_distribution(data: pd.DataFrame, source: str, coctel_type: str) -> pd.DataFrame:
        """Calcular distribución de posiciones como porcentaje (Sección 9)"""
        if coctel_type == 'Con coctel':
            data = data[data['coctel'] == 1]
        elif coctel_type == 'Sin coctel':
            data = data[data['coctel'] == 0]

        if source == 'Radio':
            data = data[data['id_fuente'] == 1]
        elif source == 'TV':
            data = data[data['id_fuente'] == 2]
        elif source == 'Redes':
            data = data[data['id_fuente'] == 3]

        if data.empty:
            return pd.DataFrame()

        conteo_total = data.groupby(['id_posicion']).size().reset_index(name='count')
        conteo_total['Posición'] = conteo_total['id_posicion'].map(ID_POSICION_DICT)
        conteo_total = conteo_total.dropna()

        conteo_total['Porcentaje'] = conteo_total['count'] / conteo_total['count'].sum()
        conteo_total['Porcentaje'] = conteo_total['Porcentaje'].map('{:.0%}'.format)

        return conteo_total

    # =====================================================
    # SECCIONES DE ACONTECIMIENTOS
    # =====================================================

    @staticmethod
    def calculate_coctel_events_distribution(data: pd.DataFrame, source: str) -> pd.DataFrame:
        """Calcular distribución de eventos con/sin coctel (Sección 10)"""
        if source == 'Radio':
            data = data[data['id_fuente'] == 1]
        elif source == 'TV':
            data = data[data['id_fuente'] == 2]
        elif source == 'Redes':
            data = data[data['id_fuente'] == 3]

        if data.empty:
            return pd.DataFrame()

        conteo_total = data.groupby(['coctel']).size().reset_index(name='count')
        conteo_total['Coctel'] = conteo_total['coctel'].map(COCTEL_DICT)
        conteo_total['Porcentaje'] = conteo_total['count'] / conteo_total['count'].sum()
        
        return conteo_total

    @staticmethod
    def calculate_coctel_by_source_location(data: pd.DataFrame) -> pd.DataFrame:
        """Calcular cantidad de cocteles por fuente y lugar (Sección 11)"""
        conteo_total = data.groupby(['id_fuente', 'lugar', 'coctel']).size().reset_index(name='count')
        conteo_total = conteo_total[conteo_total['coctel'] == 1]
        conteo_total['Fuente'] = conteo_total['id_fuente'].map(ID_FUENTE_DICT)

        result = pd.crosstab(conteo_total['lugar'],
                            conteo_total['Fuente'],
                            values=conteo_total['count'],
                            aggfunc='sum').fillna(0).reset_index()
        
        return result

    @staticmethod
    def calculate_media_generating_coctel(data: pd.DataFrame, data_fb: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calcular cantidad de medios que generan coctel (Sección 12)"""
        data["semana"] = data['fecha_registro'].dt.isocalendar().year.map(str) + '-' + data['fecha_registro'].dt.isocalendar().week.map(str)

        merged = pd.merge(data, data_fb[['fecha_registro', 'acontecimiento', 'coctel','id_fuente', 'lugar', 'nombre_facebook_page']], 
                         on=['fecha_registro', 'acontecimiento', 'coctel','id_fuente', 'lugar'], how='left')
        merged['id_canal'] = merged['id_canal'].fillna(merged['nombre_facebook_page'])
        
        coctel_data = merged[merged['coctel'] == 1]

        conteo_total = coctel_data.groupby(['id_fuente', 'lugar', 'id_canal','semana']).size().reset_index(name='count')
        conteo_total['Fuente'] = conteo_total['id_fuente'].astype(int).map(ID_FUENTE_DICT)

        conteo_canal = conteo_total.groupby(['Fuente', 'lugar'])['id_canal'].nunique().reset_index(name='conteo_canal')

        crosstab = pd.crosstab(conteo_canal['lugar'],
                              conteo_canal['Fuente'],
                              values=conteo_canal['conteo_canal'],
                              aggfunc='sum').fillna(0).reset_index()

        melted = crosstab.melt(id_vars=['lugar'], var_name='Fuente', value_name='Cantidad')

        return crosstab, melted

    @staticmethod
    def calculate_monthly_coctel_count(data: pd.DataFrame, data_fb: pd.DataFrame = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calcular conteo mensual de cocteles (Sección 13) - VERSION HÍBRIDA"""
        
        if data_fb is not None and not data_fb.empty:
            
            data_base = data.copy()
            
            data_fb_extra = data_fb.copy()
            
            columnas_comunes = ['fecha_registro', 'lugar', 'id_fuente', 'coctel']
            if 'acontecimiento' in data.columns and 'acontecimiento' in data_fb.columns:
                columnas_comunes.append('acontecimiento')
                
            data_final = pd.concat([
                data_base[columnas_comunes],
                data_fb_extra[columnas_comunes]
            ], ignore_index=True)
            
            data = data_final
        
        data = data.copy()
        non_numeric = data['coctel'].apply(lambda x: isinstance(x, str) or not pd.api.types.is_number(x))
        data.loc[non_numeric, 'coctel'] = 1.0

        data["mes"] = data['fecha_registro'].dt.month
        data["año"] = data['fecha_registro'].dt.year
        data["Fuente"] = data["id_fuente"].map(ID_FUENTE_DICT)
        data = data.dropna().drop_duplicates().reset_index(drop=True)
        data["año_mes"] = data["año"].astype(str) + "-" + data["mes"].astype(str)

        by_location = data.groupby(['lugar', 'año_mes', 'Fuente']).agg({'coctel': 'sum'}).reset_index()
        by_month = data.groupby(['año_mes', 'Fuente']).agg({'coctel': 'sum'}).reset_index()

        return by_location, by_month

    # =====================================================
    # SECCIONES DE ANÁLISIS DE CONTENIDO
    # =====================================================

    @staticmethod
    def calculate_favor_contra_notes(data: pd.DataFrame, coctel_type: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Calcular porcentaje de notas a favor vs en contra (Sección 14)"""

        if coctel_type == "Con coctel":
            data = data[data['coctel'] == 1]
        elif coctel_type == "Sin coctel":
            data = data[data['coctel'] == 0]

        data = data.dropna()
        data["mes"] = data['fecha_registro'].dt.month
        data["año"] = data['fecha_registro'].dt.year
        data["año_mes"] = data["año"].astype(str) + "-" + data["mes"].astype(str).str.zfill(2)

        data["a_favor"] = 0
        data["en_contra"] = 0
        data["neutral"] = 0

        data.loc[data['id_posicion'].isin([1, 2]), "a_favor"] = 1
        data.loc[data['id_posicion'].isin([4, 5]), "en_contra"] = 1
        data.loc[data['id_posicion'] == 3, "neutral"] = 1

        conteo_abs = (
            data
            .groupby('año_mes')
            .agg({'a_favor': 'sum', 'en_contra': 'sum', 'neutral': 'sum'})
            .reset_index()
        )

        conteo_pct = conteo_abs.copy()
        conteo_pct["total"] = (
            conteo_pct["a_favor"] + conteo_pct["en_contra"] + conteo_pct["neutral"]
        )

        conteo_pct["a_favor_pct"] = (conteo_pct['a_favor'] / conteo_pct['total'] * 100).round(1)
        conteo_pct["en_contra_pct"] = (conteo_pct['en_contra'] / conteo_pct['total'] * 100).round(1)
        conteo_pct["neutral_pct"] = (conteo_pct['neutral'] / conteo_pct['total'] * 100).round(1)

        conteo_pct = conteo_pct[["año_mes", "a_favor_pct", "neutral_pct", "en_contra_pct"]].dropna()

        long_df = conteo_pct.melt(
            id_vars=["año_mes"],
            var_name="Tipo de Nota",
            value_name="Porcentaje"
        )

        return conteo_pct, long_df, conteo_abs


    @staticmethod
    def calculate_message_proportion_by_position(data: pd.DataFrame, source: str, coctel_type: str) -> pd.DataFrame:
        """Calcular proporción de mensajes por posición (Sección 15)"""
        if coctel_type == 'Con coctel':
            data = data[data['coctel'] == 1]
        elif coctel_type == 'Sin coctel':
            data = data[data['coctel'] == 0]

        if source != 'Todos':
            source_map = {'Radio': 1, 'TV': 2, 'Redes': 3}
            data = data[data['id_fuente'] == source_map.get(source, data['id_fuente'])]
            
        grouped = data.groupby('id_posicion')["id"].count().reset_index()
        grouped = grouped.rename(columns={'id': 'frecuencia'})
        grouped["id_posicion"] = grouped["id_posicion"].map(ID_POSICION_DICT)
        grouped['porcentaje'] = grouped['frecuencia'] / grouped['frecuencia'].sum()
        grouped['porcentaje'] = grouped['porcentaje'].apply(lambda x: "{:.2%}".format(x))
        grouped = grouped.reset_index(drop=True)

        return grouped

    @staticmethod
    def calculate_messages_by_topic(data: pd.DataFrame, source: str, coctel_type: str, top_n: int = 10) -> pd.DataFrame:
        """Calcular mensajes por tema con posiciones (Sección 16)"""
        if coctel_type == 'Con coctel':
            data = data[data['coctel'] == 1]
        elif coctel_type == 'Sin coctel':
            data = data[data['coctel'] == 0]

        if source == 'Radio':
            data = data[data['id_fuente'] == 1]
        elif source == 'TV':
            data = data[data['id_fuente'] == 2]
        elif source == 'Redes':
            data = data[data['id_fuente'] == 3]

        if data.empty:
            return pd.DataFrame()

        data["id_posicion"] = data["id_posicion"].map(ID_POSICION_DICT)
        df_grouped = data.groupby(['descripcion', 'id_posicion']).size().reset_index(name='frecuencia')

        top_temas = df_grouped.groupby('descripcion')['frecuencia'].sum().nlargest(top_n).index
        df_top = df_grouped[df_grouped['descripcion'].isin(top_temas)]

        return df_top

    @staticmethod
    def calculate_topic_proportion(data: pd.DataFrame, source: str, coctel_type: str, top_n: int = 10) -> pd.DataFrame:
        """Calcular proporción de mensajes por tema (Sección 17)"""
        if coctel_type == 'Con coctel':
            data = data[data['coctel'] == 1]
        elif coctel_type == 'Sin coctel':
            data = data[data['coctel'] == 0]

        if source == 'Radio':
            data = data[data['id_fuente'] == 1]
        elif source == 'TV':
            data = data[data['id_fuente'] == 2]
        elif source == 'Redes':
            data = data[data['id_fuente'] == 3]

        if data.empty:
            return pd.DataFrame()

        df_grouped = data.groupby(['descripcion']).size().reset_index(name='frecuencia')
        top_temas = df_grouped.nlargest(top_n, 'frecuencia')['descripcion']
        df_top = df_grouped[df_grouped['descripcion'].isin(top_temas)]

        df_top['porcentaje'] = df_top['frecuencia'] / df_grouped['frecuencia'].sum()
        df_top["porcentaje"] = df_top["porcentaje"] * 100

        return df_top

    @staticmethod
    def calculate_notes_trend_by_medium(data_programas: pd.DataFrame, data_fb: pd.DataFrame, 
                                      source: str, coctel_type: str) -> pd.DataFrame:
        """Calcular tendencia de notas por medio (Sección 18)"""
        if coctel_type == 'Con coctel':
            data_programas = data_programas[data_programas['coctel'] == 1]
            data_fb = data_fb[data_fb['coctel'] == 1]
        elif coctel_type == 'Sin coctel':
            data_programas = data_programas[data_programas['coctel'] == 0]
            data_fb = data_fb[data_fb['coctel'] == 0]

        if source == 'Radio':
            data_programas = data_programas[data_programas['id_fuente'] == 1]
        elif source == 'TV':
            data_programas = data_programas[data_programas['id_fuente'] == 2]
        elif source == 'Redes':
            data_fb = data_fb[data_fb['id_fuente'] == 3]

        if source == "Redes" and not data_fb.empty:
            df_grouped = data_fb.groupby(['nombre_facebook_page', 'id_posicion']).size().reset_index(name='frecuencia')
            df_grouped = df_grouped.sort_values(by='frecuencia', ascending=False)
            df_grouped['id_posicion'] = df_grouped['id_posicion'].map(ID_POSICION_DICT)
            df_grouped['medio_nombre'] = df_grouped['nombre_facebook_page']
            
        elif source != "Redes" and not data_programas.empty:
            df_grouped = data_programas.groupby(['nombre_canal', 'id_posicion']).size().reset_index(name='frecuencia')
            df_grouped = df_grouped.sort_values(by='frecuencia', ascending=False)
            df_grouped['id_posicion'] = df_grouped['id_posicion'].map(ID_POSICION_DICT)
            df_grouped['medio_nombre'] = df_grouped['nombre_canal']
        else:
            df_grouped = pd.DataFrame()

        return df_grouped

    @staticmethod
    def calculate_notes_by_time_position(data: pd.DataFrame, coctel_type: str) -> pd.DataFrame:
        """Calcular notas por posición en rango de tiempo (Sección 19)"""
        if coctel_type == 'Con coctel':
            data = data[data['coctel'] == 1]
        elif coctel_type == 'Sin coctel':
            data = data[data['coctel'] == 0]

        if data.empty:
            return pd.DataFrame()

        df_grouped = data.groupby(['id_posicion']).size().reset_index(name='frecuencia')
        df_grouped['id_posicion'] = df_grouped['id_posicion'].map(ID_POSICION_DICT)

        return df_grouped

    @staticmethod
    def calculate_actor_positions(data: pd.DataFrame, source: str, coctel_type: str, top_n: int = 10) -> pd.DataFrame:
        """Calcular posiciones por actor (Sección 20)"""
        if source == 'Radio':
            data = data[data['id_fuente'] == 1]
        elif source == 'TV':
            data = data[data['id_fuente'] == 2]
        elif source == 'Redes':
            data = data[data['id_fuente'] == 3]

        if coctel_type == 'Con coctel':
            data = data[data['coctel'] == 1]
        elif coctel_type == 'Sin coctel':
            data = data[data['coctel'] == 0]

        if data.empty:
            return pd.DataFrame()

        data["posicion"] = data["id_posicion"].map(ID_POSICION_DICT)
        data = data[data["nombre"] != "periodista"]
        
        df_grouped = data.groupby(['nombre', 'posicion']).size().reset_index(name='frecuencia')

        top_actores = df_grouped.groupby('nombre')['frecuencia'].sum().nlargest(top_n).index
        df_top = df_grouped[df_grouped['nombre'].isin(top_actores)]

        return df_top

    # =====================================================
    # SECCIONES DE ANÁLISIS COMPARATIVO
    # =====================================================

    @staticmethod
    def calculate_coctel_percentage_by_media(data: pd.DataFrame, year_month_start: str, year_month_end: str) -> pd.DataFrame:
        """Calcular porcentaje de cóctel por medios (Sección 21)"""
        
        data = data.copy()
        data['fecha_mes'] = data['fecha_registro'].dt.to_period('M').dt.to_timestamp()
        
        start_date = pd.to_datetime(year_month_start)
        end_date = pd.to_datetime(year_month_end)
        
        data = data[
            (data['fecha_mes'] >= start_date) & 
            (data['fecha_mes'] <= end_date)
        ]

        if data.empty:
            return pd.DataFrame()

        grouped = data.groupby(['id_fuente', 'lugar']).agg({'coctel': 'sum'}).reset_index()
        
        total_por_lugar = grouped.groupby(['lugar'])['coctel'].transform('sum')
        
        grouped['Fuente'] = grouped['id_fuente'].map(ID_FUENTE_DICT)
        grouped['porcentaje_coctel'] = (grouped['coctel'] / total_por_lugar) * 100
        
        grouped = grouped.dropna()
        
        return grouped

    @staticmethod
    def calculate_last_3_months_coctel(data: pd.DataFrame, end_date: str, source: str) -> pd.DataFrame:
        """Calcular porcentaje de cóctel en últimos 3 meses (Sección 22)"""
        data = data.copy()
        data['fecha_mes'] = data['fecha_registro'].dt.to_period('M').dt.to_timestamp()

        end_date_dt = pd.to_datetime(end_date)
        start_date = end_date_dt - pd.DateOffset(months=2)

        data = data[
            (data['fecha_mes'] >= start_date) & 
            (data['fecha_mes'] <= end_date_dt)
        ]

        data['Fuente'] = data['id_fuente'].map(ID_FUENTE_DICT)
        data = data[data['Fuente'] == source]

        if data.empty:
            return pd.DataFrame()

        grouped = data.groupby(['fecha_mes', 'lugar'], as_index=False).agg({'coctel': 'sum'})
        total_por_mes = grouped.groupby(['fecha_mes'])['coctel'].transform('sum')
        grouped['porcentaje_coctel'] = (grouped['coctel'] / total_por_mes) * 100
        grouped = grouped.dropna()

        # Mapeo de meses
        meses_es = {
            "January": "Enero", "February": "Febrero", "March": "Marzo", "April": "Abril",
            "May": "Mayo", "June": "Junio", "July": "Julio", "August": "Agosto",
            "September": "Septiembre", "October": "Octubre", "November": "Noviembre", "December": "Diciembre"
        }

        grouped['mes'] = grouped['fecha_mes'].dt.strftime('%B').map(meses_es)

        return grouped

    @staticmethod
    def calculate_monthly_evolution(data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calcular evolución mensual de medios (Sección 23)"""
        data = data.copy()
        data['fecha_mes'] = data['fecha_registro'].dt.strftime('%Y-%m')
        data['Fuente'] = data['id_fuente'].map(ID_FUENTE_DICT)

        by_source = data[['coctel', 'fecha_mes', 'Fuente']].groupby(['fecha_mes', 'Fuente'], as_index=False).agg({'coctel': 'sum'})

        total_monthly = data.groupby('fecha_mes', as_index=False).agg({'coctel': 'sum'})
        total_monthly['Fuente'] = "Total"

        combined = pd.concat([by_source, total_monthly], ignore_index=True)

        return by_source, combined

    @staticmethod
    def calculate_coctel_by_message_force(data: pd.DataFrame, source: str, coctel_type: str) -> pd.DataFrame:
        """Calcular cocteles por mensaje fuerza (Sección 24)"""
        data = data[['fecha_registro','coctel','mensaje_fuerza','id_fuente']].copy()
        data['Fuente'] = data['id_fuente'].map(ID_FUENTE_DICT)
        
        if source != 'Todos':
            data = data[data['Fuente'] == source]
        
        if coctel_type == "Con coctel":
            data = data[data['coctel'] == 1]
        elif coctel_type == "Sin coctel":
            data = data[data['coctel'] == 0]
        
        grouped = data.groupby(['mensaje_fuerza']).agg({'coctel':'count'}).reset_index()
        grouped['porcentaje'] = (grouped['coctel'] / grouped['coctel'].sum()) * 100
        grouped = grouped.dropna()

        return grouped

    # =====================================================
    # SECCIONES DE REPORTES
    # =====================================================

    @staticmethod
    def calculate_program_impacts(data: pd.DataFrame, source: str) -> pd.DataFrame:
        """Calcular impactos por programa (Sección 32)"""
        if source in ("Radio", "TV"):
            columns = ["nombre_canal", 'programa_nombre']
            source_id = 1 if source == "Radio" else 2
        else:  # Redes
            columns = ["nombre_facebook_page"]
            source_id = 3

        filtered_data = data[data["id_fuente"] == source_id].copy()

        if filtered_data.empty:
            return pd.DataFrame()

        if source in ("Radio", "TV"):
            result = (
                filtered_data
                .groupby(columns, as_index=False)
                .agg(**{"Total de impactos": ("id", "count")})
                .sort_values(columns)
            )
        else:
            result = (
                filtered_data
                .groupby(columns, as_index=False)
                .agg(**{"Total de impactos": ("id", "count")})
                .sort_values(columns)
            )

        return result

    @staticmethod
    def calculate_coctel_distribution_by_media(data: pd.DataFrame) -> pd.DataFrame:
        """Calcular distribución de cócteles por medio (Sección 33)"""
        filtered_data = data[data["coctel"] == 1]

        if filtered_data.empty:
            return pd.DataFrame()

        conteo = (
            filtered_data
            .groupby("id_fuente", as_index=False)
            .agg({"id": "count"})
            .rename(columns={"id": "count"})
        )
        
        conteo["Fuente"] = conteo["id_fuente"].map(ID_FUENTE_DICT)

        return conteo
    
    @staticmethod
    def calculate_program_impacts_complete(temp_data: pd.DataFrame, medio: str) -> tuple:
        """
        Calcula tanto los impactos con cóctel como el total de impactos por programa
        
        Args:
            temp_data: DataFrame filtrado con los datos
            medio: Tipo de medio ("Radio", "TV", "Redes")
        
        Returns:
            tuple: (result_coctel, result_total) - DataFrames con impactos con cóctel y total
        """
        if medio in ("Radio", "TV"):
            prog_cols = ["nombre_canal", "programa_nombre"]
        else:
            prog_cols = ["nombre_facebook_page"]
        
        try:
            # 1) Impactos con cóctel - NO drop_duplicates for coctel count to match SN logic
            result_coctel = (
                temp_data[temp_data['coctel'] == 1]  # filter to coctel=1 first
                .groupby(prog_cols, as_index=False)
                .agg(**{"Impactos con cóctel": ("id", "count")})  # count records, not sum coctel
                .sort_values(prog_cols)
            )
            
            # 2) Total de impactos - keep drop_duplicates for total count
            result_total = (
                temp_data  # no more .drop_duplicates()
                .groupby(prog_cols, as_index=False)
                .agg(**{"Total de impactos": ("id", "count")})
                .sort_values(prog_cols)
            )
            
            return result_coctel, result_total
            
        except Exception as e:
            print(f"Error en calculate_program_impacts_complete: {e}")
            return pd.DataFrame(), pd.DataFrame()
        
    @staticmethod
    def calculate_favor_vs_contra_monthly(data: pd.DataFrame, source: str) -> pd.DataFrame:
        """Calcular notas a favor vs en contra por mes (Sección 34)"""
        if source != "Todos":
            source_map = {"Radio": 1, "TV": 2, "Redes": 3}
            data = data[data["id_fuente"] == source_map[source]]

        if data.empty:
            return pd.DataFrame()

        df = data.copy()
        df["mes"] = df["fecha_registro"].dt.to_period("M").dt.to_timestamp()
        df["a_favor"] = df["id_posicion"].isin([1, 2]).astype(int)
        df["en_contra"] = df["id_posicion"].isin([4, 5]).astype(int)

        resumen = (
            df
            .groupby("mes", as_index=False)
            .agg(
                a_favor=("a_favor", "sum"),
                en_contra=("en_contra", "sum"),
                total=("id", "count")
            )
        )
        
        resumen["A favor (%)"] = resumen["a_favor"] / resumen["total"] * 100
        resumen["En contra (%)"] = resumen["en_contra"] / resumen["total"] * 100

        long_df = resumen.melt(
            id_vars=["mes"],
            value_vars=["A favor (%)", "En contra (%)"],
            var_name="Tipo",
            value_name="Porcentaje"
        )
        
        long_df["mes_str"] = long_df["mes"].dt.strftime("%b-%y")

        return long_df