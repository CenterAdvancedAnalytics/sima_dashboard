# sections/functions/grafico6.py

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


def data_section_6_top_medios_sql(fecha_inicio: str, fecha_fin: str, lugar: str, fuente: str, top_n: int = 3) -> pd.DataFrame:
    """
    Calcula el TOP N de medios (canales o páginas) con mayor porcentaje de coctel.
    
    Para Radio/TV:
    - Agrupa por nombre_canal (no por programa)
    - Cada acontecimiento_programa cuenta (con rebotes)
    
    Para Redes:
    - Agrupa por nombre_facebook_page
    - Cada acontecimiento_facebook_post cuenta (con rebotes)
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: Nombre del lugar
        fuente: 'Radio', 'TV', o 'Redes'
        top_n: Número de medios top a retornar (default: 3)
    
    Returns:
        DataFrame con columnas: viernes, nombre_medio, porcentaje
    """
    
    print(f"DEBUG grafico6: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugar={lugar}, fuente={fuente}, top_n={top_n}")
    
    # Obtener ID del lugar
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        return pd.DataFrame()
    
    # Query para Radio y TV (agrupa por nombre_canal)
    query_radio_tv = """
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_lugar,
            a.fecha_registro,
            a.id_nota,
            p.id_fuente,
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
    -- Calcular promedio general por canal para identificar TOP N
    promedios_por_canal AS (
        SELECT 
            nombre_canal,
            COUNT(*) as total_acontecimientos,
            COUNT(CASE WHEN id_nota IS NOT NULL THEN 1 END) as total_con_coctel,
            CASE 
                WHEN COUNT(*) > 0 THEN (COUNT(CASE WHEN id_nota IS NOT NULL THEN 1 END)::float / COUNT(*)::float)
                ELSE 0
            END as promedio_coctel
        FROM acontecimientos_programas
        GROUP BY nombre_canal
        ORDER BY promedio_coctel DESC
        LIMIT %s
    ),
    -- Calcular datos semanales solo para los TOP N canales
    datos_semanales AS (
        SELECT 
            date_trunc('week', (ap.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as semana,
            ap.nombre_canal,
            COUNT(*) as total_acontecimientos,
            COUNT(CASE WHEN ap.id_nota IS NOT NULL THEN 1 END) as total_con_coctel
        FROM acontecimientos_programas ap
        INNER JOIN promedios_por_canal pc ON ap.nombre_canal = pc.nombre_canal
        GROUP BY semana, ap.nombre_canal
    )
    SELECT 
        semana,
        nombre_canal as nombre_medio,
        total_acontecimientos,
        total_con_coctel,
        CASE 
            WHEN total_acontecimientos > 0 THEN (total_con_coctel::float / total_acontecimientos::float) * 100
            ELSE 0
        END as porcentaje
    FROM datos_semanales
    ORDER BY semana, nombre_medio;
    """
    
    # Query para Redes (agrupa por nombre_facebook_page desde facebook_pages)
    query_redes = """
    WITH acontecimientos_redes AS (
        SELECT 
            a.id as acontecimiento_id,
            a.id_lugar,
            a.fecha_registro,
            a.id_nota,
            afp.id_facebook_post,
            fbp.nombre as nombre_facebook_page
        FROM acontecimientos a
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        INNER JOIN facebook_posts fp ON afp.id_facebook_post = fp.id
        INNER JOIN facebook_pages fbp ON fp.id_facebook_page = fbp.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    ),
    -- Calcular promedio general por página para identificar TOP N
    promedios_por_pagina AS (
        SELECT 
            nombre_facebook_page,
            COUNT(*) as total_acontecimientos,
            COUNT(CASE WHEN id_nota IS NOT NULL THEN 1 END) as total_con_coctel,
            CASE 
                WHEN COUNT(*) > 0 THEN (COUNT(CASE WHEN id_nota IS NOT NULL THEN 1 END)::float / COUNT(*)::float)
                ELSE 0
            END as promedio_coctel
        FROM acontecimientos_redes
        GROUP BY nombre_facebook_page
        ORDER BY promedio_coctel DESC
        LIMIT %s
    ),
    -- Calcular datos semanales solo para las TOP N páginas
    datos_semanales AS (
        SELECT 
            date_trunc('week', (ar.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as semana,
            ar.nombre_facebook_page,
            COUNT(*) as total_acontecimientos,
            COUNT(CASE WHEN ar.id_nota IS NOT NULL THEN 1 END) as total_con_coctel
        FROM acontecimientos_redes ar
        INNER JOIN promedios_por_pagina pp ON ar.nombre_facebook_page = pp.nombre_facebook_page
        GROUP BY semana, ar.nombre_facebook_page
    )
    SELECT 
        semana,
        nombre_facebook_page as nombre_medio,
        total_acontecimientos,
        total_con_coctel,
        CASE 
            WHEN total_acontecimientos > 0 THEN (total_con_coctel::float / total_acontecimientos::float) * 100
            ELSE 0
        END as porcentaje
    FROM datos_semanales
    ORDER BY semana, nombre_medio;
    """
    
    try:
        if fuente == "Radio":
            params = [id_lugar, fecha_inicio, fecha_fin, 1, top_n]  # id_fuente = 1
            resultado = ejecutar_query(query_radio_tv, params=params)
            
        elif fuente == "TV":
            params = [id_lugar, fecha_inicio, fecha_fin, 2, top_n]  # id_fuente = 2
            resultado = ejecutar_query(query_radio_tv, params=params)
            
        elif fuente == "Redes":
            params = [id_lugar, fecha_inicio, fecha_fin, top_n]
            resultado = ejecutar_query(query_redes, params=params)
            
        else:
            print(f"ERROR: Fuente no válida: {fuente}")
            return pd.DataFrame()
        
        if resultado is None or resultado.empty:
            print(f"No se encontraron datos para {lugar} - {fuente}")
            return pd.DataFrame()
        
        print(f"Resultado query grafico6: {len(resultado)} registros encontrados")
        return resultado
        
    except Exception as e:
        print(f"Error en data_section_6_top_medios_sql: {e}")
        return pd.DataFrame()


def calcular_viernes_semana(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el viernes de cada semana desde la columna 'semana'.
    
    Args:
        df: DataFrame con columna 'semana' (timestamp del inicio de semana)
    
    Returns:
        DataFrame con columna adicional 'viernes'
    """
    if df.empty:
        return df
    
    # Convertir semana a datetime si no lo es
    df['semana'] = pd.to_datetime(df['semana'])
    
    # Calcular el viernes de cada semana
    # weekday(): lunes=0, martes=1, ..., viernes=4
    df['viernes'] = df['semana'] + pd.to_timedelta(4, unit='D')
    
    return df