# sections/functions/grafico5.py

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


def data_section_5_acumulativo_lugares_sql(fecha_inicio: str, fecha_fin: str, lugares: List[str], fuente: str) -> pd.DataFrame:
    """
    Calcula el porcentaje semanal de cocteles para MÚLTIPLES lugares.
    Es igual al gráfico 3 pero agrupa por semana Y lugar.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugares: Lista de nombres de lugares ['Lima', 'Arequipa', ...]
        fuente: 'Radio', 'TV', 'Redes', o 'Todos'
    
    Returns:
        DataFrame con columnas: semana, fecha_registro, lugar, total_acontecimientos, 
                                total_con_coctel, porcentaje
    """
    
    print(f"DEBUG grafico5: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugares={lugares}, fuente={fuente}")
    
    # Obtener IDs de lugares
    ids_lugares = obtener_ids_lugares(lugares)
    if not ids_lugares:
        return pd.DataFrame()
    
    # Crear placeholders para IN clause
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    # Query para Radio y TV (cuando un acontecimiento tiene 2 programas, cuenta x2)
    query_radio_tv = f"""
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
        WHERE a.id_lugar IN ({placeholders})
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN ({{fuente_filter}})
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
    
    # Query para Redes (cuando existe acontecimiento_facebook_post cuenta por cada facebook_post)
    query_redes = f"""
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
        WHERE a.id_lugar IN ({placeholders})
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
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
            # Solo Radio (id_fuente = 1)
            query_final = query_radio_tv.format(fuente_filter="1")
            params = ids_lugares + [fecha_inicio, fecha_fin]
            resultado = ejecutar_query(query_final, params=params)
            
        elif fuente == "TV":
            # Solo TV (id_fuente = 2)
            query_final = query_radio_tv.format(fuente_filter="2")
            params = ids_lugares + [fecha_inicio, fecha_fin]
            resultado = ejecutar_query(query_final, params=params)
            
        elif fuente == "Redes":
            # Solo Redes sociales
            params = ids_lugares + [fecha_inicio, fecha_fin]
            resultado = ejecutar_query(query_redes, params=params)
            
        elif fuente == "Todos":
            # Combinar Radio, TV y Redes
            
            # 1. Radio + TV
            query_final = query_radio_tv.format(fuente_filter="1, 2")
            params = ids_lugares + [fecha_inicio, fecha_fin]
            resultado_radio_tv = ejecutar_query(query_final, params=params)
            
            # 2. Redes
            params = ids_lugares + [fecha_inicio, fecha_fin]
            resultado_redes = ejecutar_query(query_redes, params=params)
            
            # 3. Combinar resultados sumando por semana y lugar
            if resultado_radio_tv is not None and not resultado_radio_tv.empty:
                if resultado_redes is not None and not resultado_redes.empty:
                    # Ambos tienen datos, combinar
                    resultado = pd.concat([resultado_radio_tv, resultado_redes], ignore_index=True)
                    resultado = resultado.groupby(['semana', 'lugar'], as_index=False).agg({
                        'fecha_registro': 'min',
                        'total_acontecimientos': 'sum',
                        'total_con_coctel': 'sum'
                    })
                    # Recalcular porcentaje
                    resultado['porcentaje'] = (resultado['total_con_coctel'] / resultado['total_acontecimientos']) * 100
                    resultado = resultado.sort_values(['semana', 'lugar'])
                else:
                    # Solo radio_tv tiene datos
                    resultado = resultado_radio_tv
            else:
                if resultado_redes is not None and not resultado_redes.empty:
                    # Solo redes tiene datos
                    resultado = resultado_redes
                else:
                    # Ninguno tiene datos
                    resultado = pd.DataFrame()
        else:
            print(f"ERROR: Fuente no válida: {fuente}")
            return pd.DataFrame()
        
        if resultado is None or resultado.empty:
            print(f"No se encontraron datos para {lugares} - {fuente}")
            return pd.DataFrame()
        
        print(f"Resultado query grafico5: {len(resultado)} registros encontrados")
        return resultado
        
    except Exception as e:
        print(f"Error en data_section_5_acumulativo_lugares_sql: {e}")
        return pd.DataFrame()


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