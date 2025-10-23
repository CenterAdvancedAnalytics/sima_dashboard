# sections/functions/grafico4.py

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


def data_section_4_favor_vs_contra_sql(fecha_inicio: str, fecha_fin: str, lugar: str, fuente: str) -> pd.DataFrame:
    """
    Calcula el porcentaje semanal de noticias A FAVOR vs EN CONTRA.
    
    - A FAVOR: id_posicion IN (1, 2)
    - EN CONTRA: id_posicion IN (4, 5)
    - NEUTRAL (3): No se muestra pero SÍ cuenta en el denominador
    - SIN filtro de coctel
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: Nombre del lugar
        fuente: 'Radio', 'TV', 'Redes', o 'Todos'
    
    Returns:
        DataFrame con columnas: semana, fecha_registro, total_acontecimientos, 
                                total_a_favor, total_en_contra, pct_a_favor, pct_en_contra
    """
    
    print(f"DEBUG grafico4: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugar={lugar}, fuente={fuente}")
    
    # Obtener ID del lugar
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        return pd.DataFrame()
    
    # Query para Radio y TV (cuando un acontecimiento tiene 2 programas, cuenta x2)
    query_radio_tv = """
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_lugar,
            a.fecha_registro,
            a.id_posicion,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN ({fuente_filter})
    ),
    conteo_por_semana AS (
        SELECT 
            date_trunc('week', (fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as semana,
            MIN((fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as fecha_registro,
            COUNT(*) as total_acontecimientos,
            COUNT(CASE WHEN id_posicion IN (1, 2) THEN 1 END) as total_a_favor,
            COUNT(CASE WHEN id_posicion IN (4, 5) THEN 1 END) as total_en_contra
        FROM acontecimientos_programas
        GROUP BY semana
    )
    SELECT 
        semana,
        fecha_registro,
        total_acontecimientos,
        total_a_favor,
        total_en_contra,
        CASE 
            WHEN total_acontecimientos > 0 THEN (total_a_favor::float / total_acontecimientos::float) * 100
            ELSE 0
        END as pct_a_favor,
        CASE 
            WHEN total_acontecimientos > 0 THEN (total_en_contra::float / total_acontecimientos::float) * 100
            ELSE 0
        END as pct_en_contra
    FROM conteo_por_semana
    ORDER BY semana;
    """
    
    # Query para Redes (cuando existe acontecimiento_facebook_post cuenta por cada facebook_post)
    query_redes = """
    WITH acontecimientos_redes AS (
        SELECT 
            a.id as acontecimiento_id,
            a.id_lugar,
            a.fecha_registro,
            a.id_posicion,
            afp.id_facebook_post
        FROM acontecimientos a
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    ),
    conteo_por_semana AS (
        SELECT 
            date_trunc('week', (fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as semana,
            MIN((fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date) as fecha_registro,
            COUNT(*) as total_acontecimientos,
            COUNT(CASE WHEN id_posicion IN (1, 2) THEN 1 END) as total_a_favor,
            COUNT(CASE WHEN id_posicion IN (4, 5) THEN 1 END) as total_en_contra
        FROM acontecimientos_redes
        GROUP BY semana
    )
    SELECT 
        semana,
        fecha_registro,
        total_acontecimientos,
        total_a_favor,
        total_en_contra,
        CASE 
            WHEN total_acontecimientos > 0 THEN (total_a_favor::float / total_acontecimientos::float) * 100
            ELSE 0
        END as pct_a_favor,
        CASE 
            WHEN total_acontecimientos > 0 THEN (total_en_contra::float / total_acontecimientos::float) * 100
            ELSE 0
        END as pct_en_contra
    FROM conteo_por_semana
    ORDER BY semana;
    """
    
    try:
        if fuente == "Radio":
            # Solo Radio (id_fuente = 1)
            query_final = query_radio_tv.format(fuente_filter="1")
            params = [id_lugar, fecha_inicio, fecha_fin]
            resultado = ejecutar_query(query_final, params=params)
            
        elif fuente == "TV":
            # Solo TV (id_fuente = 2)
            query_final = query_radio_tv.format(fuente_filter="2")
            params = [id_lugar, fecha_inicio, fecha_fin]
            resultado = ejecutar_query(query_final, params=params)
            
        elif fuente == "Redes":
            # Solo Redes sociales
            params = [id_lugar, fecha_inicio, fecha_fin]
            resultado = ejecutar_query(query_redes, params=params)
            
        elif fuente == "Todos":
            # Combinar Radio, TV y Redes
            
            # 1. Radio + TV
            query_final = query_radio_tv.format(fuente_filter="1, 2")
            params = [id_lugar, fecha_inicio, fecha_fin]
            resultado_radio_tv = ejecutar_query(query_final, params=params)
            
            # 2. Redes
            params = [id_lugar, fecha_inicio, fecha_fin]
            resultado_redes = ejecutar_query(query_redes, params=params)
            
            # 3. Combinar resultados sumando por semana
            if resultado_radio_tv is not None and not resultado_radio_tv.empty:
                if resultado_redes is not None and not resultado_redes.empty:
                    # Ambos tienen datos, combinar
                    resultado = pd.concat([resultado_radio_tv, resultado_redes], ignore_index=True)
                    resultado = resultado.groupby('semana', as_index=False).agg({
                        'fecha_registro': 'min',
                        'total_acontecimientos': 'sum',
                        'total_a_favor': 'sum',
                        'total_en_contra': 'sum'
                    })
                    # Recalcular porcentajes
                    resultado['pct_a_favor'] = (resultado['total_a_favor'] / resultado['total_acontecimientos']) * 100
                    resultado['pct_en_contra'] = (resultado['total_en_contra'] / resultado['total_acontecimientos']) * 100
                    resultado = resultado.sort_values('semana')
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
            print(f"No se encontraron datos para {lugar} - {fuente}")
            return pd.DataFrame()
        
        print(f"Resultado query grafico4: {len(resultado)} semanas encontradas")
        return resultado
        
    except Exception as e:
        print(f"Error en data_section_4_favor_vs_contra_sql: {e}")
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