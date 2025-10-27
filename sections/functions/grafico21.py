# sections/functions/grafico21.py

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


def data_section_21_porcentaje_medios_sql(
    fecha_inicio: str, 
    fecha_fin: str, 
    lugares: List[str]
) -> pd.DataFrame:
    """
    Calcula el porcentaje de cóctel por medio (Radio, TV, Redes) y lugar.
    
    El porcentaje se calcula como:
    (coctel de una fuente en un lugar / total de coctel en ese lugar) * 100
    
    IMPORTANTE: Radio y TV tienen id_fuente en la tabla fuentes
                Redes NO tiene id_fuente, se maneja por separado con acontecimiento_facebook_post
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugares: Lista de nombres de lugares
    
    Returns:
        DataFrame con columnas: lugar, fuente, total_coctel, porcentaje_coctel
    """
    
    print(f"DEBUG grafico21: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugares={lugares}")
    
    if not lugares:
        print("⚠️ No se especificaron lugares")
        return pd.DataFrame()
    
    # Crear la lista de parámetros para la cláusula IN
    lugares_placeholders = ','.join(['%s'] * len(lugares))
    
    # QUERY PARA RADIO Y TV (con deduplicación por programa)
    query_radio_tv = f"""
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            l.nombre as lugar,
            p.id_fuente,
            f.nombre as fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN lugares l ON a.id_lugar = l.id
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN fuentes f ON p.id_fuente = f.id
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND l.nombre IN ({lugares_placeholders})
            AND a.id_nota IS NOT NULL  -- Solo cocteles
            AND p.id_fuente IN (1, 2)  -- Solo Radio (1) y TV (2)
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por nombre de programa para evitar doble conteo
        SELECT DISTINCT
            acontecimiento_id,
            lugar,
            fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, lugar, fuente, programa_nombre
    )
    SELECT 
        lugar,
        fuente,
        COUNT(*) as total_coctel
    FROM acontecimientos_deduplicados
    GROUP BY lugar, fuente
    ORDER BY lugar, fuente;
    """
    
    # QUERY PARA REDES (sin deduplicación)
    query_redes = f"""
    SELECT 
        l.nombre as lugar,
        'REDES' as fuente,
        COUNT(*) as total_coctel
    FROM acontecimientos a
    INNER JOIN lugares l ON a.id_lugar = l.id
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        AND l.nombre IN ({lugares_placeholders})
        AND a.id_nota IS NOT NULL  -- Solo cocteles
    GROUP BY l.nombre
    ORDER BY l.nombre;
    """
    
    # Parámetros: fecha_inicio, fecha_fin, y todos los lugares
    params = [fecha_inicio, fecha_fin] + lugares
    
    try:
        # Ejecutar query para Radio/TV
        print("🔍 Consultando Radio/TV...")
        resultado_radio_tv = ejecutar_query(query_radio_tv, params=params)
        print(f"📻📺 Radio/TV: {len(resultado_radio_tv) if resultado_radio_tv is not None else 0} filas")
        
        # Ejecutar query para Redes
        print("🔍 Consultando Redes...")
        resultado_redes = ejecutar_query(query_redes, params=params)
        print(f"📱 Redes: {len(resultado_redes) if resultado_redes is not None else 0} filas")
        
        # Combinar resultados
        resultado_combinado = pd.DataFrame()
        
        if resultado_radio_tv is not None and not resultado_radio_tv.empty:
            resultado_combinado = pd.concat([resultado_combinado, resultado_radio_tv], ignore_index=True)
        
        if resultado_redes is not None and not resultado_redes.empty:
            resultado_combinado = pd.concat([resultado_combinado, resultado_redes], ignore_index=True)
        
        if resultado_combinado.empty:
            print("⚠️ No se encontraron datos en ninguna fuente")
            return pd.DataFrame()
        
        # Calcular totales por lugar
        totales_lugar = resultado_combinado.groupby('lugar')['total_coctel'].sum().reset_index()
        totales_lugar.columns = ['lugar', 'total_lugar']
        
        # Merge para calcular porcentajes
        resultado_final = resultado_combinado.merge(totales_lugar, on='lugar')
        resultado_final['porcentaje_coctel'] = round(
            (resultado_final['total_coctel'] / resultado_final['total_lugar']) * 100, 
            2
        )
        
        # Seleccionar solo las columnas necesarias
        resultado_final = resultado_final[['lugar', 'fuente', 'total_coctel', 'porcentaje_coctel']]
        resultado_final = resultado_final.sort_values(['lugar', 'fuente'])
        
        print(f"✅ Se encontraron {len(resultado_final)} combinaciones fuente-lugar")
        return resultado_final
        
    except Exception as e:
        print(f"❌ Error en data_section_21_porcentaje_medios_sql: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def calcular_promedios_por_fuente(df: pd.DataFrame) -> dict:
    """
    Calcula el promedio de porcentaje_coctel por fuente.
    
    Args:
        df: DataFrame con columnas 'fuente' y 'porcentaje_coctel'
    
    Returns:
        Diccionario con {'fuente': promedio}
    """
    if df.empty or 'fuente' not in df.columns or 'porcentaje_coctel' not in df.columns:
        return {}
    
    promedios = df.groupby('fuente')['porcentaje_coctel'].mean().to_dict()
    return promedios