# grafico13.py
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

def conteo_acontecimientos_radio_tv_por_lugar_mes(id_lugar: int, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """
    Conteo de acontecimientos con coctel para Radio y TV por lugar y mes
    Deduplica por nombre de programa dentro de cada acontecimiento
    
    Args:
        id_lugar: ID del lugar
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame con columnas: lugar, año_mes, fuente, coctel
    """
    
    query = """
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            l.nombre as lugar_nombre,
            DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')) as año_mes,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN lugares l ON a.id_lugar = l.id
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar = %s
            AND a.id_nota IS NOT NULL  -- Solo con coctel
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN (1, 2)  -- Solo Radio (1) y TV (2)
    ),
    acontecimientos_deduplicados AS (
        SELECT DISTINCT
            acontecimiento_id,
            lugar_nombre,
            año_mes,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, lugar_nombre, año_mes, id_fuente, programa_nombre
    )
    SELECT 
        ad.lugar_nombre as lugar,
        TO_CHAR(ad.año_mes, 'YYYY-MM') as año_mes,
        CASE 
            WHEN ad.id_fuente = 1 THEN 'RADIO'
            WHEN ad.id_fuente = 2 THEN 'TV'
        END as fuente,
        COUNT(*) as coctel
    FROM acontecimientos_deduplicados ad
    GROUP BY 
        ad.lugar_nombre,
        ad.año_mes, 
        ad.id_fuente
    ORDER BY 
        ad.lugar_nombre,
        ad.año_mes, 
        ad.id_fuente;
    """
    
    try:
        resultado = ejecutar_query(query, params=[id_lugar, fecha_inicio, fecha_fin])
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_acontecimientos_radio_tv_por_lugar_mes: {e}")
        return pd.DataFrame()


def conteo_acontecimientos_redes_por_lugar_mes(id_lugar: int, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """
    Conteo de acontecimientos con coctel para Redes por lugar y mes
    Cuenta todos los acontecimientos que tienen facebook posts
    
    Args:
        id_lugar: ID del lugar
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame con columnas: lugar, año_mes, fuente, coctel
    """
    
    query = """
    SELECT 
        l.nombre as lugar,
        TO_CHAR(DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')), 'YYYY-MM') as año_mes,
        'REDES' as fuente,
        COUNT(*) as coctel
    FROM acontecimientos a
    INNER JOIN lugares l ON a.id_lugar = l.id
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE a.id_lugar = %s
        AND a.id_nota IS NOT NULL  -- Solo con coctel
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    GROUP BY 
        l.nombre,
        DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima'))
    ORDER BY 
        l.nombre,
        año_mes;
    """
    
    try:
        resultado = ejecutar_query(query, params=[id_lugar, fecha_inicio, fecha_fin])
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_acontecimientos_redes_por_lugar_mes: {e}")
        return pd.DataFrame()


def data_section_13_acontecimientos_por_lugar_mes(fecha_inicio: str, fecha_fin: str, lugar: str) -> pd.DataFrame:
    """
    Función principal que combina los conteos de Radio/TV y Redes para acontecimientos con coctel
    por lugar y mes
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD' (primer día del mes)
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD' (último día del mes)
        lugar: Nombre del lugar
    
    Returns:
        DataFrame con columnas: lugar, año_mes, fuente, coctel
    """
    
    print(f"DEBUG: Parámetros recibidos:")
    print(f"  fecha_inicio: {fecha_inicio}")
    print(f"  fecha_fin: {fecha_fin}")
    print(f"  lugar: {lugar}")
    
    # Convertir nombre de lugar a ID
    id_lugar = obtener_id_lugar(lugar)
    print(f"DEBUG: ID lugar obtenido: {id_lugar}")
    
    if id_lugar is None:
        print(f"ERROR: No se encontró ID para el lugar: {lugar}")
        return pd.DataFrame()
    
    resultado_final = pd.DataFrame()
    
    try:
        # Obtener datos de Radio/TV
        print(f"DEBUG: Consultando Radio/TV...")
        resultado_radio_tv = conteo_acontecimientos_radio_tv_por_lugar_mes(id_lugar, fecha_inicio, fecha_fin)
        print(f"DEBUG: Resultado Radio/TV: {len(resultado_radio_tv)} filas")
        if not resultado_radio_tv.empty:
            resultado_final = pd.concat([resultado_final, resultado_radio_tv], ignore_index=True)
        
        # Obtener datos de Redes
        print(f"DEBUG: Consultando Redes...")
        resultado_redes = conteo_acontecimientos_redes_por_lugar_mes(id_lugar, fecha_inicio, fecha_fin)
        print(f"DEBUG: Resultado Redes: {len(resultado_redes)} filas")
        if not resultado_redes.empty:
            resultado_final = pd.concat([resultado_final, resultado_redes], ignore_index=True)
        
        print(f"DEBUG: Resultado final combinado: {len(resultado_final)} filas")
        print(f"DEBUG: Datos finales:\n{resultado_final}")
        
        return resultado_final
        
    except Exception as e:
        print(f"ERROR en data_section_13_acontecimientos_por_lugar_mes: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


# Ejemplo de uso:
# resultado = data_section_13_acontecimientos_por_lugar_mes('2024-01-01', '2024-03-31', 'Lima')