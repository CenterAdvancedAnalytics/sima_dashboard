# sections/functions/grafico2.py

import pandas as pd
import psycopg2
import os
from typing import Optional, List, Any
from pathlib import Path
def ejecutar_query(query: str, params: Optional[List[Any]] = None) -> Optional[pd.DataFrame]:
    """
    Ejecuta una query SQL y retorna los resultados como un DataFrame de pandas.
    """
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
     from dotenv import load_dotenv
     load_dotenv(env_path)
     print(f"✅ .env cargado desde: {env_path}")
     
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

def obtener_id_lugar(nombre_lugar: str) -> Optional[int]:
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

def posiciones_radio_con_sin_coctel(fecha_inicio: str, fecha_fin: str, lugar: str) -> pd.DataFrame:
    """
    Obtiene las posiciones de publicaciones CON y SIN coctel para RADIO
    
    Returns:
        DataFrame con columnas: id_posicion, tipo_coctel, cantidad
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
                WHEN a.id_nota IS NOT NULL THEN 'Con coctel'
                ELSE 'Sin coctel'
            END as tipo_coctel,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar = %s
            AND p.id_fuente = 1  -- Solo Radio
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    )
    
    SELECT 
        ad.id_posicion,
        ad.tipo_coctel,
        COUNT(*) as cantidad
    FROM acontecimientos_programas ad
    GROUP BY ad.id_posicion, ad.tipo_coctel
    ORDER BY ad.id_posicion, ad.tipo_coctel;
    """
    
    try:
        resultado = ejecutar_query(query, params=[id_lugar, fecha_inicio, fecha_fin])
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en posiciones_radio_con_sin_coctel: {e}")
        return pd.DataFrame()

def posiciones_tv_con_sin_coctel(fecha_inicio: str, fecha_fin: str, lugar: str) -> pd.DataFrame:
    """
    Obtiene las posiciones de publicaciones CON y SIN coctel para TV
    
    Returns:
        DataFrame con columnas: id_posicion, tipo_coctel, cantidad
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
                WHEN a.id_nota IS NOT NULL THEN 'Con coctel'
                ELSE 'Sin coctel'
            END as tipo_coctel,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar = %s
            AND p.id_fuente = 2  -- Solo TV
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    )
    
    SELECT 
        ad.id_posicion,
        ad.tipo_coctel,
        COUNT(*) as cantidad
    FROM acontecimientos_programas ad
    GROUP BY ad.id_posicion, ad.tipo_coctel
    ORDER BY ad.id_posicion, ad.tipo_coctel;
    """
    
    try:
        resultado = ejecutar_query(query, params=[id_lugar, fecha_inicio, fecha_fin])
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en posiciones_tv_con_sin_coctel: {e}")
        return pd.DataFrame()

def data_section_2_posiciones_coctel_sql(fecha_inicio: str, fecha_fin: str, lugar: str) -> tuple:
    """
    Función principal que obtiene las posiciones con/sin coctel para Radio, TV y Redes
    
    Returns:
        Tupla con (df_radio, df_tv, df_redes)
    """
    
    try:
        df_radio = posiciones_radio_con_sin_coctel(fecha_inicio, fecha_fin, lugar)
        df_tv = posiciones_tv_con_sin_coctel(fecha_inicio, fecha_fin, lugar)
        df_redes = posiciones_redes_con_sin_coctel(fecha_inicio, fecha_fin, lugar)
        
        print(f"📻 Radio: {len(df_radio)} registros")
        print(f"📺 TV: {len(df_tv)} registros")
        print(f"📱 Redes: {len(df_redes)} registros")
        
        return df_radio, df_tv, df_redes
        
    except Exception as e:
        print(f"Error en data_section_2_posiciones_coctel_sql: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def convertir_posicion_a_nombre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte los IDs de posición a nombres legibles
    
    Args:
        df: DataFrame con columna 'id_posicion'
    
    Returns:
        DataFrame con columna 'Posición' con nombres en lugar de IDs
    """
    
    id_posicion_dict = {
        1: 'A favor',
        2: 'Potencialmente',
        3: 'Neutral',
        4: 'Potencialmente',
        5: 'En contra'
    }
    
    if 'id_posicion' in df.columns:
        df['Posición'] = df['id_posicion'].map(id_posicion_dict)
        df = df.dropna(subset=['Posición'])
    
    return df

def preparar_datos_para_grafico(df_radio: pd.DataFrame, df_tv: pd.DataFrame, df_redes: pd.DataFrame) -> pd.DataFrame:
    """
    Combina y prepara los datos de Radio, TV y Redes para un gráfico agrupado
    """
    
    resultado = pd.DataFrame()
    
    if not df_radio.empty:
        df_radio_proc = convertir_posicion_a_nombre(df_radio.copy())
        df_radio_proc['Medio'] = 'Radio'
        resultado = pd.concat([resultado, df_radio_proc], ignore_index=True)
    
    if not df_tv.empty:
        df_tv_proc = convertir_posicion_a_nombre(df_tv.copy())
        df_tv_proc['Medio'] = 'TV'
        resultado = pd.concat([resultado, df_tv_proc], ignore_index=True)
    
    if not df_redes.empty:
        df_redes_proc = convertir_posicion_a_nombre(df_redes.copy())
        df_redes_proc['Medio'] = 'Redes'
        resultado = pd.concat([resultado, df_redes_proc], ignore_index=True)
    
    if not resultado.empty:
        resultado = resultado.rename(columns={
            'tipo_coctel': 'Tipo_Coctel',
            'cantidad': 'Cantidad'
        })
        
        resultado = resultado[['Posición', 'Tipo_Coctel', 'Medio', 'Cantidad']]
    
    return resultado


def posiciones_redes_con_sin_coctel(fecha_inicio: str, fecha_fin: str, lugar: str) -> pd.DataFrame:
    """
    Obtiene las posiciones de publicaciones CON y SIN coctel para REDES (Facebook)
    
    Returns:
        DataFrame con columnas: id_posicion, tipo_coctel, cantidad
    """
    
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        return pd.DataFrame()
    
    query = """
    WITH acontecimientos_redes AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            a.id_lugar,
            a.fecha_registro,
            CASE 
                WHEN a.id_nota IS NOT NULL THEN 'Con coctel'
                ELSE 'Sin coctel'
            END as tipo_coctel
        FROM acontecimientos a
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    )
    
    SELECT 
        ar.id_posicion,
        ar.tipo_coctel,
        COUNT(*) as cantidad
    FROM acontecimientos_redes ar
    GROUP BY ar.id_posicion, ar.tipo_coctel
    ORDER BY ar.id_posicion, ar.tipo_coctel;
    """
    
    try:
        resultado = ejecutar_query(query, params=[id_lugar, fecha_inicio, fecha_fin])
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en posiciones_redes_con_sin_coctel: {e}")
        return pd.DataFrame()
# Ejemplo de uso:
# Ejemplo de uso:
if __name__ == "__main__":
    # Test de las funciones
    df_radio, df_tv, df_redes = data_section_2_posiciones_coctel_sql('2024-01-01', '2026-12-31', 'Puno')
    
    print("\n=== RADIO ===")
    print(df_radio)
    
    print("\n=== TV ===")
    print(df_tv)
    
    print("\n=== REDES ===")
    print(df_redes)
    
    print("\n=== DATOS PREPARADOS PARA GRÁFICO ===")
    df_combinado = preparar_datos_para_grafico(df_radio, df_tv, df_redes)
    print(df_combinado)