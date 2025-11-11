# sections/functions/grafico8.py

import pandas as pd
import psycopg2
import os
from typing import Optional, List, Any

def ejecutar_query(query: str, params: Optional[List[Any]] = None) -> Optional[pd.DataFrame]:
    """
    Ejecuta una query SQL y retorna los resultados como un DataFrame de pandas.
    """
    
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'tu_base_de_datos'),
        'user': os.getenv('DB_USER', 'tu_usuario'),
        'password': os.getenv('DB_PASSWORD', 'tu_contraseña'),
        'port': os.getenv('DB_PORT', '5432')
    }
    
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        results = cursor.fetchall()
        
        if not results:
            return pd.DataFrame()
        
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=column_names)
        
        return df
        
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def obtener_id_lugar(nombre_lugar):
    """
    Obtiene el ID de un lugar por su nombre
    """
    query = """
    SELECT id, nombre 
    FROM lugares 
    WHERE nombre = %s
    LIMIT 1;
    """
    
    try:
        resultado = ejecutar_query(query, params=[nombre_lugar])
        
        if resultado is None or resultado.empty:
            print(f"No se encontró el lugar: {nombre_lugar}")
            return None
            
        return int(resultado.iloc[0]['id'])
        
    except Exception as e:
        print(f"Error al buscar el lugar '{nombre_lugar}': {e}")
        return None

def conteo_posiciones_radio_tv(fecha_inicio: str, fecha_fin: str, lugar: str) -> pd.DataFrame:
    """
    Obtiene conteo de posiciones para Radio y TV con programas
    """
    
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        return pd.DataFrame()
    
    query = """
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            a.id_lugar,
            a.fecha_registro,
            CASE 
                WHEN a.id_nota IS NOT NULL THEN 'CON_COCTEL'
                ELSE 'SIN_COCTEL'
            END as tipo_coctel,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN (1, 2)  -- Solo Radio (1) y TV (2)
    ),
    acontecimientos_deduplicados AS (
        SELECT DISTINCT
            acontecimiento_id,
            id_posicion,
            id_lugar,
            fecha_registro,
            tipo_coctel,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_posicion, id_lugar, fecha_registro, tipo_coctel, id_fuente, programa_nombre
    )

    SELECT 
        ad.id_posicion as posicion,
        CASE 
            WHEN ad.id_fuente = 1 THEN 'Radio'
            WHEN ad.id_fuente = 2 THEN 'TV'
            ELSE 'Otros'
        END as fuente,
        ad.tipo_coctel,
        COUNT(*) as conteo_acontecimientos
    FROM acontecimientos_deduplicados ad
    GROUP BY 
        ad.id_posicion, 
        ad.id_fuente, 
        ad.tipo_coctel
    ORDER BY 
        ad.id_fuente, 
        ad.id_posicion, 
        ad.tipo_coctel;
    """
    
    try:
        resultado = ejecutar_query(query, params=[id_lugar, fecha_inicio, fecha_fin])
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_posiciones_radio_tv: {e}")
        return pd.DataFrame()

def conteo_posiciones_redes(fecha_inicio: str, fecha_fin: str, lugar: str) -> pd.DataFrame:
    """
    Obtiene conteo de posiciones para Redes Sociales con facebook posts
    """
    
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        a.id_posicion as posicion,
        'Redes' as fuente,
        CASE 
            WHEN a.id_nota IS NOT NULL THEN 'CON_COCTEL'
            ELSE 'SIN_COCTEL'
        END as tipo_coctel,
        COUNT(*) as conteo_acontecimientos
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE a.id_lugar = %s
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    GROUP BY 
        a.id_posicion, 
        CASE 
            WHEN a.id_nota IS NOT NULL THEN 'CON_COCTEL'
            ELSE 'SIN_COCTEL'
        END
    ORDER BY 
        a.id_posicion, 
        tipo_coctel;
    """
    
    try:
        resultado = ejecutar_query(query, params=[id_lugar, fecha_inicio, fecha_fin])
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_posiciones_redes: {e}")
        return pd.DataFrame()

def data_section_8_conteo_posiciones_sql(fecha_inicio: str, fecha_fin: str, lugar: str, option_fuente: str = "Todos", option_nota: str = "Todos") -> pd.DataFrame:
    """
    Función principal que combina los conteos de Radio/TV y Redes según los filtros
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: Nombre del lugar
        option_fuente: 'Radio', 'TV', 'Redes', o 'Todos'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: posicion, fuente, tipo_coctel, conteo_acontecimientos
    """
    
    resultado_final = pd.DataFrame()
    
    try:
        # Obtener datos según la fuente seleccionada
        if option_fuente in ["Radio", "TV", "Todos"]:
            resultado_radio_tv = conteo_posiciones_radio_tv(fecha_inicio, fecha_fin, lugar)
            if not resultado_radio_tv.empty:
                # Filtrar por fuente específica si no es "Todos"
                if option_fuente == "Radio":
                    resultado_radio_tv = resultado_radio_tv[resultado_radio_tv['fuente'] == 'Radio']
                elif option_fuente == "TV":
                    resultado_radio_tv = resultado_radio_tv[resultado_radio_tv['fuente'] == 'TV']
                
                resultado_final = pd.concat([resultado_final, resultado_radio_tv], ignore_index=True)
        
        if option_fuente in ["Redes", "Todos"]:
            resultado_redes = conteo_posiciones_redes(fecha_inicio, fecha_fin, lugar)
            if not resultado_redes.empty:
                resultado_final = pd.concat([resultado_final, resultado_redes], ignore_index=True)
        if not resultado_final.empty and option_nota == "Todos":
             resultado_final = resultado_final.groupby(['posicion', 'fuente']).agg({
                 'conteo_acontecimientos': 'sum'
             }).reset_index()
        # Filtrar por tipo de coctel si no es "Todos"
        if not resultado_final.empty and option_nota != "Todos":
            if option_nota == "Con coctel":
                resultado_final = resultado_final[resultado_final['tipo_coctel'] == 'CON_COCTEL']
            elif option_nota == "Sin coctel":
                resultado_final = resultado_final[resultado_final['tipo_coctel'] == 'SIN_COCTEL']
        
        # Renombrar columnas para que coincidan con el formato esperado por plotly
        if not resultado_final.empty:
            resultado_final = resultado_final.rename(columns={
                'conteo_acontecimientos': 'count',
                'fuente': 'Tipo de Medio',
                'posicion': 'Posición'
            })
        
        return resultado_final
        
    except Exception as e:
        print(f"Error en data_section_8_conteo_posiciones_sql: {e}")
        return pd.DataFrame()

def convertir_posicion_a_nombre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte los IDs de posición a nombres descriptivos.
    Función independiente que puede ser llamada cuando se necesite convertir posiciones.
    
    Args:
        df: DataFrame con columna 'Posición' que contiene IDs numéricos
    
    Returns:
        DataFrame con columna 'Posición' convertida a nombres descriptivos
    
    Ejemplo de uso:
        df_resultado = data_section_8_conteo_posiciones_sql('2024-01-01', '2024-12-31', 'Lima', 'Todos', 'Todos')
        df_con_nombres = convertir_posicion_a_nombre(df_resultado)
    """
    # Mapeo de posiciones a nombres de sentimiento
    posicion_dict = {
        1: 'A favor',
        2: 'Potencialmente a favor', 
        3: 'Neutral',
        4: 'Potencialmente en contra',
        5: 'En contra'
    }
    
    if not df.empty and 'Posición' in df.columns:
        df['Posición'] = df['Posición'].map(posicion_dict).fillna(df['Posición'])
    
    return df