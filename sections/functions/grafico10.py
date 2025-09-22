# sections/functions/grafico10.py

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

def obtener_ids_lugares(nombres_lugares: List[str]) -> List[int]:
    """
    Convierte lista de nombres de lugares a lista de IDs
    """
    if not nombres_lugares:
        return []
    
    # Crear placeholders para la query IN
    placeholders = ','.join(['%s'] * len(nombres_lugares))
    query = f"""
    SELECT id, nombre 
    FROM lugares 
    WHERE nombre IN ({placeholders})
    ORDER BY nombre;
    """
    
    try:
        resultado = ejecutar_query(query, params=nombres_lugares)
        
        if resultado is None or resultado.empty:
            print(f"No se encontraron lugares: {nombres_lugares}")
            return []
            
        ids_encontrados = resultado['id'].tolist()
        print(f"Lugares encontrados: {len(ids_encontrados)} de {len(nombres_lugares)}")
        return ids_encontrados
        
    except Exception as e:
        print(f"Error al buscar lugares {nombres_lugares}: {e}")
        return []

def conteo_eventos_radio_tv(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int]) -> pd.DataFrame:
    """
    Conteo de eventos con/sin cóctel para Radio y TV con múltiples lugares agregados
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    # Crear placeholders para la query IN
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
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
        WHERE a.id_lugar IN ({placeholders})
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN (1, 2)  -- Solo Radio (1) y TV (2)
    ),
    acontecimientos_deduplicados AS (
        SELECT DISTINCT
            acontecimiento_id,
            id_lugar,
            fecha_registro,
            tipo_coctel,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_lugar, fecha_registro, tipo_coctel, id_fuente, programa_nombre
    )
    SELECT 
        CASE 
            WHEN ad.id_fuente = 1 THEN 'Radio'
            WHEN ad.id_fuente = 2 THEN 'TV'
        END as fuente,
        ad.tipo_coctel,
        COUNT(*) as conteo_acontecimientos
    FROM acontecimientos_deduplicados ad
    GROUP BY 
        ad.id_fuente, 
        ad.tipo_coctel
    ORDER BY 
        ad.id_fuente, 
        ad.tipo_coctel;
    """
    
    # Parámetros: ids_lugares + fecha_inicio + fecha_fin
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_eventos_radio_tv: {e}")
        return pd.DataFrame()

def conteo_eventos_redes(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int]) -> pd.DataFrame:
    """
    Conteo de eventos con/sin cóctel para Redes Sociales con múltiples lugares agregados
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    # Crear placeholders para la query IN
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    SELECT 
        'Redes' as fuente,
        CASE 
            WHEN a.id_nota IS NOT NULL THEN 'CON_COCTEL'
            ELSE 'SIN_COCTEL'
        END as tipo_coctel,
        COUNT(*) as conteo_acontecimientos
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE a.id_lugar IN ({placeholders})
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    GROUP BY 
        CASE 
            WHEN a.id_nota IS NOT NULL THEN 'CON_COCTEL'
            ELSE 'SIN_COCTEL'
        END
    ORDER BY 
        tipo_coctel;
    """
    
    # Parámetros: ids_lugares + fecha_inicio + fecha_fin
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_eventos_redes: {e}")
        return pd.DataFrame()

def data_section_10_eventos_coctel_sql(fecha_inicio: str, fecha_fin: str, lugares: List[str]) -> pd.DataFrame:
    """
    Función principal que combina los conteos de Radio/TV y Redes para eventos con cóctel
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugares: Lista de nombres de lugares ['Lima', 'Arequipa']
    
    Returns:
        DataFrame con columnas: fuente, tipo_coctel, conteo_acontecimientos
    """
    
    print(f"DEBUG: Parámetros recibidos:")
    print(f"  fecha_inicio: {fecha_inicio}")
    print(f"  fecha_fin: {fecha_fin}")
    print(f"  lugares: {lugares}")
    
    # Convertir nombres de lugares a IDs
    ids_lugares = obtener_ids_lugares(lugares)
    print(f"DEBUG: IDs lugares obtenidos: {ids_lugares}")
    if not ids_lugares:
        print(f"ERROR: No se encontraron IDs para los lugares: {lugares}")
        return pd.DataFrame()
    
    resultado_final = pd.DataFrame()
    
    try:
        # Obtener datos de Radio/TV
        print(f"DEBUG: Consultando Radio/TV...")
        resultado_radio_tv = conteo_eventos_radio_tv(fecha_inicio, fecha_fin, ids_lugares)
        print(f"DEBUG: Resultado Radio/TV: {len(resultado_radio_tv)} filas")
        if not resultado_radio_tv.empty:
            resultado_final = pd.concat([resultado_final, resultado_radio_tv], ignore_index=True)
        
        # Obtener datos de Redes
        print(f"DEBUG: Consultando Redes...")
        resultado_redes = conteo_eventos_redes(fecha_inicio, fecha_fin, ids_lugares)
        print(f"DEBUG: Resultado Redes: {len(resultado_redes)} filas")
        if not resultado_redes.empty:
            resultado_final = pd.concat([resultado_final, resultado_redes], ignore_index=True)
        
        print(f"DEBUG: Resultado final combinado: {len(resultado_final)} filas")
        print(f"DEBUG: Datos finales:\n{resultado_final}")
        
        return resultado_final
        
    except Exception as e:
        print(f"ERROR en data_section_10_eventos_coctel_sql: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def convertir_a_formato_grafico(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte el resultado a formato adecuado para gráficos de pie/dona
    """
    if df.empty:
        return pd.DataFrame()
    
    # Mapear tipo_coctel a nombres más amigables
    df_resultado = df.copy()
    df_resultado['Coctel'] = df_resultado['tipo_coctel'].map({
        'CON_COCTEL': 'Con coctel',
        'SIN_COCTEL': 'Sin coctel'
    })
    
    # Renombrar columnas
    df_resultado = df_resultado.rename(columns={
        'conteo_acontecimientos': 'count'
    })
    
    return df_resultado[['fuente', 'Coctel', 'count']]

# Ejemplo de uso:
# resultado = data_section_10_eventos_coctel_sql('2025-09-17', '2025-09-17', ['Ayacucho'])
# df_grafico = convertir_a_formato_grafico(resultado)