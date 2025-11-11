# sections/functions/grafico25.py

import pandas as pd
import psycopg2
import os
from typing import Optional, List, Any, Tuple

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


def obtener_id_lugar(nombre_lugar: str) -> Optional[int]:
    """
    Obtiene el ID de un lugar por su nombre
    """
    query = """
    SELECT id 
    FROM lugares 
    WHERE nombre = %s;
    """
    
    try:
        resultado = ejecutar_query(query, params=[nombre_lugar])
        if resultado is None or resultado.empty:
            print(f"No se encontró el lugar: {nombre_lugar}")
            return None
        return int(resultado.iloc[0]['id'])
    except Exception as e:
        print(f"Error al buscar lugar: {e}")
        return None


def impactos_radio_tv(
    fecha_inicio: str,
    fecha_fin: str,
    id_lugar: int,
    fuente: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcula impactos con cóctel y total de impactos para Radio o TV.
    Incluye deduplicación por nombre de programa.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        id_lugar: ID del lugar
        fuente: 'Radio' o 'TV'
    
    Returns:
        Tuple de (impactos_con_coctel_df, total_impactos_df)
        Cada DataFrame tiene columnas: nombre_canal, programa_nombre, impactos
    """
    
    # Mapeo de fuente
    id_fuente = 1 if fuente == 'Radio' else 2
    
    # Query para impactos CON cóctel
    query_coctel = """
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            c.nombre as nombre_canal,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN canales c ON p.id_canal = c.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente = %s
            AND a.id_nota IS NOT NULL  -- Solo con cóctel
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por nombre de programa
        SELECT DISTINCT
            acontecimiento_id,
            nombre_canal,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, nombre_canal, programa_nombre
    )
    SELECT 
        nombre_canal,
        programa_nombre,
        COUNT(*) as impactos_con_coctel
    FROM acontecimientos_deduplicados
    GROUP BY nombre_canal, programa_nombre
    ORDER BY nombre_canal, programa_nombre;
    """
    
    # Query para TOTAL de impactos (con y sin cóctel)
    query_total = """
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            c.nombre as nombre_canal,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN canales c ON p.id_canal = c.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente = %s
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por nombre de programa
        SELECT DISTINCT
            acontecimiento_id,
            nombre_canal,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, nombre_canal, programa_nombre
    )
    SELECT 
        nombre_canal,
        programa_nombre,
        COUNT(*) as total_impactos
    FROM acontecimientos_deduplicados
    GROUP BY nombre_canal, programa_nombre
    ORDER BY nombre_canal, programa_nombre;
    """
    
    params = [id_lugar, fecha_inicio, fecha_fin, id_fuente]
    
    try:
        resultado_coctel = ejecutar_query(query_coctel, params=params)
        resultado_total = ejecutar_query(query_total, params=params)
        
        if resultado_coctel is None:
            resultado_coctel = pd.DataFrame()
        if resultado_total is None:
            resultado_total = pd.DataFrame()
            
        return resultado_coctel, resultado_total
        
    except Exception as e:
        print(f"Error en impactos_radio_tv: {e}")
        return pd.DataFrame(), pd.DataFrame()


def impactos_redes(
    fecha_inicio: str,
    fecha_fin: str,
    id_lugar: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcula impactos con cóctel y total de impactos para Redes.
    Incluye deduplicación por nombre de facebook_page.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        id_lugar: ID del lugar
    
    Returns:
        Tuple de (impactos_con_coctel_df, total_impactos_df)
        Cada DataFrame tiene columnas: nombre_facebook_page, impactos
    """
    
    # Query para impactos CON cóctel
    query_coctel = """
    WITH acontecimientos_facebook AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            fp.nombre as nombre_facebook_page
        FROM acontecimientos a
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        INNER JOIN facebook_posts fpost ON afp.id_facebook_post = fpost.id
        INNER JOIN facebook_pages fp ON fpost.id_facebook_page = fp.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND a.id_nota IS NOT NULL  -- Solo con cóctel
            AND fp.nombre IS NOT NULL
            AND fp.nombre != ''
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por nombre de facebook_page
        SELECT DISTINCT
            acontecimiento_id,
            nombre_facebook_page
        FROM acontecimientos_facebook
        GROUP BY acontecimiento_id, nombre_facebook_page
    )
    SELECT 
        nombre_facebook_page,
        COUNT(*) as impactos_con_coctel
    FROM acontecimientos_deduplicados
    GROUP BY nombre_facebook_page
    ORDER BY nombre_facebook_page;
    """
    
    # Query para TOTAL de impactos
    query_total = """
    WITH acontecimientos_facebook AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            fp.nombre as nombre_facebook_page
        FROM acontecimientos a
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        INNER JOIN facebook_posts fpost ON afp.id_facebook_post = fpost.id
        INNER JOIN facebook_pages fp ON fpost.id_facebook_page = fp.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND fp.nombre IS NOT NULL
            AND fp.nombre != ''
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por nombre de facebook_page
        SELECT DISTINCT
            acontecimiento_id,
            nombre_facebook_page
        FROM acontecimientos_facebook
        GROUP BY acontecimiento_id, nombre_facebook_page
    )
    SELECT 
        nombre_facebook_page,
        COUNT(*) as total_impactos
    FROM acontecimientos_deduplicados
    GROUP BY nombre_facebook_page
    ORDER BY nombre_facebook_page;
    """
    
    params = [id_lugar, fecha_inicio, fecha_fin]
    
    try:
        resultado_coctel = ejecutar_query(query_coctel, params=params)
        resultado_total = ejecutar_query(query_total, params=params)
        
        if resultado_coctel is None:
            resultado_coctel = pd.DataFrame()
        if resultado_total is None:
            resultado_total = pd.DataFrame()
            
        return resultado_coctel, resultado_total
        
    except Exception as e:
        print(f"Error en impactos_redes: {e}")
        return pd.DataFrame(), pd.DataFrame()
    
    
def data_section_25_impactos_programa_sql(
    fecha_inicio: str,
    fecha_fin: str,
    lugar: str,
    medio: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Función principal para Gráfico 25: Impactos por programa
    
    Retorna dos DataFrames:
    1. Impactos con cóctel por programa/página
    2. Total de impactos por programa/página
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: Nombre del lugar
        medio: 'Radio', 'TV', o 'Redes'
    
    Returns:
        Tuple de (impactos_con_coctel_df, total_impactos_df)
    """
    
    print(f"DEBUG grafico25: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugar={lugar}, medio={medio}")
    
    # Obtener ID del lugar
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        print(f"❌ No se encontró el lugar: {lugar}")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"✅ ID lugar encontrado: {id_lugar}")
    
    # Obtener datos según el medio
    if medio == 'Radio':
        print(f"🔍 Consultando Radio...")
        resultado_coctel, resultado_total = impactos_radio_tv(fecha_inicio, fecha_fin, id_lugar, 'Radio')
    elif medio == 'TV':
        print(f"🔍 Consultando TV...")
        resultado_coctel, resultado_total = impactos_radio_tv(fecha_inicio, fecha_fin, id_lugar, 'TV')
    elif medio == 'Redes':
        print(f"🔍 Consultando Redes...")
        resultado_coctel, resultado_total = impactos_redes(fecha_inicio, fecha_fin, id_lugar)
    else:
        print(f"❌ Medio inválido: {medio}")
        return pd.DataFrame(), pd.DataFrame()
    
    if resultado_coctel.empty and resultado_total.empty:
        print(f"⚠️ No se encontraron datos para {medio}")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"✅ {medio} - Impactos con cóctel: {len(resultado_coctel)} programas")
    print(f"✅ {medio} - Total impactos: {len(resultado_total)} programas")
    
    return resultado_coctel, resultado_total