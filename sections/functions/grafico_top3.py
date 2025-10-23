# sections/functions/grafico_top3.py

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


def data_section_top3_lugares_sql(fecha_inicio: str, fecha_fin: str, fuente: str, top_n: int = 3) -> Tuple[pd.DataFrame, List[str]]:
    """
    Calcula el porcentaje semanal de cocteles para TODOS los lugares,
    identifica los TOP N lugares con mayor porcentaje en la última semana,
    y retorna solo los datos de esos lugares.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        fuente: 'Radio', 'TV', o 'Redes' (NO acepta "Todos")
        top_n: Número de lugares top a retornar (default: 3)
    
    Returns:
        Tuple[DataFrame, List[str]]: (datos_filtrados, lista_top_lugares)
    """
    
    print(f"DEBUG grafico_top3: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, fuente={fuente}, top_n={top_n}")
    
    # Query para Radio y TV
    query_radio_tv = """
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_lugar,
            l.nombre as lugar_nombre,
            a.fecha_registro,
            a.id_nota,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN lugares l ON a.id_lugar = l.id
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente = %s
    ),
    conteo_por_semana_lugar AS (
        SELECT 
            date_trunc('week', (fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as semana,
            lugar_nombre,
            MIN((fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as fecha_registro,
            COUNT(*) as total_acontecimientos,
            COUNT(CASE WHEN id_nota IS NOT NULL THEN 1 END) as total_con_coctel
        FROM acontecimientos_programas
        GROUP BY semana, lugar_nombre
    )
    SELECT 
        semana,
        fecha_registro,
        lugar_nombre as lugar,
        total_acontecimientos,
        total_con_coctel,
        CASE 
            WHEN total_acontecimientos > 0 THEN (total_con_coctel::float / total_acontecimientos::float) * 100
            ELSE 0
        END as porcentaje
    FROM conteo_por_semana_lugar
    ORDER BY semana, lugar_nombre;
    """
    
    # Query para Redes
    query_redes = """
    WITH acontecimientos_redes AS (
        SELECT 
            a.id as acontecimiento_id,
            a.id_lugar,
            l.nombre as lugar_nombre,
            a.fecha_registro,
            a.id_nota,
            afp.id_facebook_post
        FROM acontecimientos a
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        INNER JOIN lugares l ON a.id_lugar = l.id
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    ),
    conteo_por_semana_lugar AS (
        SELECT 
            date_trunc('week', (fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as semana,
            lugar_nombre,
            MIN((fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as fecha_registro,
            COUNT(*) as total_acontecimientos,
            COUNT(CASE WHEN id_nota IS NOT NULL THEN 1 END) as total_con_coctel
        FROM acontecimientos_redes
        GROUP BY semana, lugar_nombre
    )
    SELECT 
        semana,
        fecha_registro,
        lugar_nombre as lugar,
        total_acontecimientos,
        total_con_coctel,
        CASE 
            WHEN total_acontecimientos > 0 THEN (total_con_coctel::float / total_acontecimientos::float) * 100
            ELSE 0
        END as porcentaje
    FROM conteo_por_semana_lugar
    ORDER BY semana, lugar_nombre;
    """
    
    try:
        if fuente == "Radio":
            params = [fecha_inicio, fecha_fin, 1]  # id_fuente = 1
            resultado = ejecutar_query(query_radio_tv, params=params)
            
        elif fuente == "TV":
            params = [fecha_inicio, fecha_fin, 2]  # id_fuente = 2
            resultado = ejecutar_query(query_radio_tv, params=params)
            
        elif fuente == "Redes":
            params = [fecha_inicio, fecha_fin]
            resultado = ejecutar_query(query_redes, params=params)
            
        else:
            print(f"ERROR: Fuente no válida para TOP3: {fuente}")
            return pd.DataFrame(), []
        
        if resultado is None or resultado.empty:
            print(f"No se encontraron datos para {fuente}")
            return pd.DataFrame(), []
        
        # Identificar TOP N lugares basándose en la última semana
        ultima_semana = resultado['semana'].max()
        datos_ultima_semana = resultado[resultado['semana'] == ultima_semana]
        
        # Ordenar por porcentaje descendente y tomar top N
        top_lugares = datos_ultima_semana.nlargest(top_n, 'porcentaje')['lugar'].tolist()
        
        print(f"TOP {top_n} lugares identificados: {top_lugares}")
        
        # Filtrar solo los datos de los top lugares
        resultado_filtrado = resultado[resultado['lugar'].isin(top_lugares)]
        
        print(f"Resultado query grafico_top3: {len(resultado_filtrado)} registros para {len(top_lugares)} lugares")
        
        return resultado_filtrado, top_lugares
        
    except Exception as e:
        print(f"Error en data_section_top3_lugares_sql: {e}")
        return pd.DataFrame(), []


def calcular_viernes_semana(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el viernes de cada semana para usar como eje X en el gráfico.
    
    Args:
        df: DataFrame con columna 'fecha_registro'
    
    Returns:
        DataFrame con columna adicional 'viernes'
    """
    if df.empty:
        return df
    
    # Convertir fecha_registro a datetime si no lo es
    df['fecha_registro'] = pd.to_datetime(df['fecha_registro'])
    
    # Calcular el viernes de cada semana
    # weekday(): lunes=0, martes=1, ..., viernes=4, sábado=5, domingo=6
    df['viernes'] = df['fecha_registro'] + pd.to_timedelta(
        (4 - df['fecha_registro'].dt.weekday) % 7, unit='D'
    )
    
    return df