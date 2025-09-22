# sections/functions/grafico11.py

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

def conteo_eventos_radio_tv_integrado(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int]) -> pd.DataFrame:
    """
    Conteo integrado de eventos con/sin cóctel para Radio y TV con múltiples lugares.
    Devuelve resultado con formato: Lugar, Radio, TV, Radio_Con_Coctel, Radio_Sin_Coctel, TV_Con_Coctel, TV_Sin_Coctel
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    # Crear placeholders para la query IN
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    WITH acontecimientos_programas AS (
        -- Obtener todos los acontecimientos con sus programas, deduplicando por nombre de programa
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_lugar,
            a.fecha_registro,
            CASE 
                WHEN a.id_nota IS NOT NULL THEN 'CON_COCTEL'
                ELSE 'SIN_COCTEL'
            END as tipo_coctel,
            p.id_fuente,
            p.nombre as programa_nombre,
            l.nombre as lugar_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN lugares l ON a.id_lugar = l.id
        WHERE a.id_lugar IN ({placeholders})
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN (1, 2)  -- Solo Radio (1) y TV (2)
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por nombre de programa como especificaste
        SELECT DISTINCT
            acontecimiento_id,
            id_lugar,
            fecha_registro,
            tipo_coctel,
            id_fuente,
            programa_nombre,
            lugar_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_lugar, fecha_registro, tipo_coctel, id_fuente, programa_nombre, lugar_nombre
    ),
    conteos_por_fuente AS (
        -- Contar acontecimientos por fuente y tipo de coctel
        SELECT 
            lugar_nombre,
            CASE 
                WHEN id_fuente = 1 THEN 'RADIO'
                WHEN id_fuente = 2 THEN 'TV'
            END as fuente,
            tipo_coctel,
            COUNT(*) as conteo
        FROM acontecimientos_deduplicados
        GROUP BY lugar_nombre, id_fuente, tipo_coctel
    ),
    pivot_data AS (
        -- Crear estructura pivoteada para el formato solicitado
        SELECT 
            lugar_nombre,
            SUM(CASE WHEN fuente = 'RADIO' AND tipo_coctel = 'CON_COCTEL' THEN conteo ELSE 0 END) as radio_con_coctel,
            SUM(CASE WHEN fuente = 'RADIO' AND tipo_coctel = 'SIN_COCTEL' THEN conteo ELSE 0 END) as radio_sin_coctel,
            SUM(CASE WHEN fuente = 'TV' AND tipo_coctel = 'CON_COCTEL' THEN conteo ELSE 0 END) as tv_con_coctel,
            SUM(CASE WHEN fuente = 'TV' AND tipo_coctel = 'SIN_COCTEL' THEN conteo ELSE 0 END) as tv_sin_coctel
        FROM conteos_por_fuente
        GROUP BY lugar_nombre
    )
    SELECT 
        lugar_nombre as Lugar,
        (radio_con_coctel + radio_sin_coctel) as Radio,
        (tv_con_coctel + tv_sin_coctel) as TV,
        -- Desglose detallado
        radio_con_coctel as Radio_Con_Coctel,
        radio_sin_coctel as Radio_Sin_Coctel,
        tv_con_coctel as TV_Con_Coctel,
        tv_sin_coctel as TV_Sin_Coctel
    FROM pivot_data
    ORDER BY lugar_nombre;
    """
    
    # Parámetros: ids_lugares + fecha_inicio + fecha_fin
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_eventos_radio_tv_integrado: {e}")
        return pd.DataFrame()

def conteo_eventos_redes_integrado(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int]) -> pd.DataFrame:
    """
    Conteo integrado de eventos con/sin cóctel para Redes Sociales con múltiples lugares.
    Devuelve resultado con formato: Lugar, Redes, Redes_Con_Coctel, Redes_Sin_Coctel
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    # Crear placeholders para la query IN
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    WITH acontecimientos_redes AS (
        -- Obtener todos los acontecimientos con facebook posts
        SELECT 
            a.id as acontecimiento_id,
            a.id_lugar,
            a.fecha_registro,
            CASE 
                WHEN a.id_nota IS NOT NULL THEN 'CON_COCTEL'
                ELSE 'SIN_COCTEL'
            END as tipo_coctel,
            l.nombre as lugar_nombre,
            afp.id as facebook_post_id  -- Para contar cada post individualmente
        FROM acontecimientos a
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        INNER JOIN lugares l ON a.id_lugar = l.id
        WHERE a.id_lugar IN ({placeholders})
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    ),
    conteos_por_lugar AS (
        -- Contar facebook posts por lugar y tipo de coctel
        SELECT 
            lugar_nombre,
            tipo_coctel,
            COUNT(*) as conteo  -- Cuenta cada facebook post
        FROM acontecimientos_redes
        GROUP BY lugar_nombre, tipo_coctel
    ),
    pivot_data AS (
        -- Crear estructura pivoteada para el formato solicitado
        SELECT 
            lugar_nombre,
            SUM(CASE WHEN tipo_coctel = 'CON_COCTEL' THEN conteo ELSE 0 END) as redes_con_coctel,
            SUM(CASE WHEN tipo_coctel = 'SIN_COCTEL' THEN conteo ELSE 0 END) as redes_sin_coctel
        FROM conteos_por_lugar
        GROUP BY lugar_nombre
    )
    SELECT 
        lugar_nombre as Lugar,
        (redes_con_coctel + redes_sin_coctel) as Redes,
        -- Desglose detallado
        redes_con_coctel as Redes_Con_Coctel,
        redes_sin_coctel as Redes_Sin_Coctel
    FROM pivot_data
    ORDER BY lugar_nombre;
    """
    
    # Parámetros: ids_lugares + fecha_inicio + fecha_fin
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_eventos_redes_integrado: {e}")
        return pd.DataFrame()

def data_section_11_conteo_integrado_sql(fecha_inicio: str, fecha_fin: str, lugares: List[str]) -> pd.DataFrame:
    """
    Función principal que combina los conteos integrados de Radio/TV y Redes para eventos con cóctel
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugares: Lista de nombres de lugares ['Lima', 'Arequipa']
    
    Returns:
        DataFrame con columnas: Lugar, Radio, TV, Redes, Radio_Con_Coctel, Radio_Sin_Coctel, 
        TV_Con_Coctel, TV_Sin_Coctel, Redes_Con_Coctel, Redes_Sin_Coctel
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
    
    try:
        # Obtener datos de Radio/TV
        print(f"DEBUG: Consultando Radio/TV...")
        resultado_radio_tv = conteo_eventos_radio_tv_integrado(fecha_inicio, fecha_fin, ids_lugares)
        print(f"DEBUG: Resultado Radio/TV: {len(resultado_radio_tv)} filas")
        
        # Obtener datos de Redes
        print(f"DEBUG: Consultando Redes...")
        resultado_redes = conteo_eventos_redes_integrado(fecha_inicio, fecha_fin, ids_lugares)
        print(f"DEBUG: Resultado Redes: {len(resultado_redes)} filas")
        
        # Combinar los resultados
        if not resultado_radio_tv.empty and not resultado_redes.empty:
            # Hacer merge por Lugar
            resultado_final = resultado_radio_tv.merge(
                resultado_redes, 
                on='Lugar', 
                how='outer'
            )
        elif not resultado_radio_tv.empty:
            # Solo Radio/TV
            resultado_final = resultado_radio_tv.copy()
            resultado_final['Redes'] = 0
            resultado_final['Redes_Con_Coctel'] = 0
            resultado_final['Redes_Sin_Coctel'] = 0
        elif not resultado_redes.empty:
            # Solo Redes
            resultado_final = resultado_redes.copy()
            resultado_final['Radio'] = 0
            resultado_final['TV'] = 0
            resultado_final['Radio_Con_Coctel'] = 0
            resultado_final['Radio_Sin_Coctel'] = 0
            resultado_final['TV_Con_Coctel'] = 0
            resultado_final['TV_Sin_Coctel'] = 0
        else:
            # Sin datos
            resultado_final = pd.DataFrame()
        
        # Llenar NaN con 0
        if not resultado_final.empty:
            resultado_final = resultado_final.fillna(0)
            # Asegurar que las columnas numéricas sean enteros
            numeric_cols = ['Radio', 'TV', 'Redes', 'Radio_Con_Coctel', 'Radio_Sin_Coctel', 
                          'TV_Con_Coctel', 'TV_Sin_Coctel', 'Redes_Con_Coctel', 'Redes_Sin_Coctel']
            for col in numeric_cols:
                if col in resultado_final.columns:
                    resultado_final[col] = resultado_final[col].astype(int)
        
        print(f"DEBUG: Resultado final combinado: {len(resultado_final)} filas")
        print(f"DEBUG: Columnas: {resultado_final.columns.tolist() if not resultado_final.empty else 'N/A'}")
        
        return resultado_final
        
    except Exception as e:
        print(f"ERROR en data_section_11_conteo_integrado_sql: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def convertir_a_formato_resumen(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte el resultado integrado a un formato de resumen por fuente
    """
    if df.empty:
        return pd.DataFrame()
    
    resultados = []
    
    for _, row in df.iterrows():
        lugar = row['Lugar']
        
        # Radio
        if row.get('Radio', 0) > 0:
            resultados.append({
                'Lugar': lugar,
                'Fuente': 'Radio',
                'Con_Coctel': int(row.get('Radio_Con_Coctel', 0)),
                'Sin_Coctel': int(row.get('Radio_Sin_Coctel', 0)),
                'Total': int(row.get('Radio', 0))
            })
        
        # TV
        if row.get('TV', 0) > 0:
            resultados.append({
                'Lugar': lugar,
                'Fuente': 'TV',
                'Con_Coctel': int(row.get('TV_Con_Coctel', 0)),
                'Sin_Coctel': int(row.get('TV_Sin_Coctel', 0)),
                'Total': int(row.get('TV', 0))
            })
        
        # Redes
        if row.get('Redes', 0) > 0:
            resultados.append({
                'Lugar': lugar,
                'Fuente': 'Redes',
                'Con_Coctel': int(row.get('Redes_Con_Coctel', 0)),
                'Sin_Coctel': int(row.get('Redes_Sin_Coctel', 0)),
                'Total': int(row.get('Redes', 0))
            })
    
    return pd.DataFrame(resultados)

# Ejemplo de uso:
# resultado = data_section_11_conteo_integrado_sql('2024-01-01', '2024-12-31', ['Lima', 'Arequipa'])
# resumen = convertir_a_formato_resumen(resultado)