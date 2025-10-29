# sections/functions/grafico26.py

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


def distribucion_cocteles_radio_tv(
    fecha_inicio: str,
    fecha_fin: str
) -> pd.DataFrame:
    """
    Cuenta cócteles de Radio y TV (todas las ubicaciones).
    CUENTA REBOTES: Si acontecimiento está en 2 programas = cuenta 2 veces
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame con columnas: fuente, count
    """
    
    query = """
    WITH acontecimientos_programas AS (
        -- Paso 1: Obtener todas las combinaciones acontecimiento-programa
        SELECT DISTINCT
            a.id as acontecimiento_id,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND a.id_nota IS NOT NULL  -- Solo con cóctel
            AND p.id_fuente IN (1, 2)  -- Radio y TV
    ),
    acontecimientos_deduplicados AS (
        -- Paso 2: Eliminar duplicados EXACTOS
        -- Mantiene rebotes: mismo acontecimiento en diferentes programas cuenta múltiples veces
        SELECT DISTINCT
            acontecimiento_id,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_fuente, programa_nombre
    )
    SELECT 
        CASE 
            WHEN id_fuente = 1 THEN 'Radio'
            WHEN id_fuente = 2 THEN 'TV'
        END as fuente,
        COUNT(*) as count
    FROM acontecimientos_deduplicados
    GROUP BY id_fuente
    ORDER BY fuente;
    """
    
    params = [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        if resultado is None:
            return pd.DataFrame()
        return resultado
    except Exception as e:
        print(f"Error en distribucion_cocteles_radio_tv: {e}")
        return pd.DataFrame()


def distribucion_cocteles_redes(
    fecha_inicio: str,
    fecha_fin: str
) -> pd.DataFrame:
    """
    Cuenta cócteles de Redes (todas las ubicaciones).
    CUENTA REBOTES: Si acontecimiento está en 2 facebook_posts = cuenta 2 veces
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame con columnas: fuente, count
    """
    
    query = """
    SELECT 
        'Redes' as fuente,
        COUNT(*) as count
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        AND a.id_nota IS NOT NULL;  -- Solo con cóctel
    """
    
    params = [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        if resultado is None:
            return pd.DataFrame()
        return resultado
    except Exception as e:
        print(f"Error en distribucion_cocteles_redes: {e}")
        return pd.DataFrame()


def data_section_26_distribucion_medio_sql(
    fecha_inicio: str,
    fecha_fin: str
) -> pd.DataFrame:
    """
    Función principal para Gráfico 26: Distribución de cócteles por medio
    
    Cuenta el total de cócteles de Radio, TV y Redes en TODAS las ubicaciones.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame con columnas: fuente, count
        Ejemplo:
        fuente  | count
        --------|------
        Radio   | 450
        Redes   | 320
        TV      | 230
    """
    
    print(f"DEBUG grafico26: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}")
    print(f"📊 TODAS las ubicaciones | 📻📺📱 Radio + TV + Redes")
    
    # Obtener datos de Radio/TV
    print(f"🔍 Consultando Radio + TV...")
    resultado_radio_tv = distribucion_cocteles_radio_tv(fecha_inicio, fecha_fin)
    
    # Obtener datos de Redes
    print(f"🔍 Consultando Redes...")
    resultado_redes = distribucion_cocteles_redes(fecha_inicio, fecha_fin)
    
    # Combinar resultados
    resultado_final = pd.DataFrame()
    
    if not resultado_radio_tv.empty:
        resultado_final = pd.concat([resultado_final, resultado_radio_tv], ignore_index=True)
        print(f"📻📺 Radio/TV: {len(resultado_radio_tv)} fuentes encontradas")
    
    if not resultado_redes.empty:
        resultado_final = pd.concat([resultado_final, resultado_redes], ignore_index=True)
        print(f"📱 Redes: {len(resultado_redes)} fuentes encontradas")
    
    if resultado_final.empty:
        print(f"⚠️ No se encontraron cócteles en el rango de fechas")
        return pd.DataFrame()
    
    # Ordenar por fuente para consistencia
    resultado_final = resultado_final.sort_values('fuente').reset_index(drop=True)
    
    # Calcular totales
    total_cocteles = resultado_final['count'].sum()
    print(f"✅ Total: {len(resultado_final)} fuentes con {total_cocteles} cócteles")
    
    return resultado_final