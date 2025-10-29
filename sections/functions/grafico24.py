# sections/functions/grafico24.py

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


def conteo_mensajes_fuerza_radio_tv(
    fecha_inicio: str,
    fecha_fin: str,
    fuente: str,
    coctel_type: str
) -> pd.DataFrame:
    """
    Conteo de acontecimientos por mensaje_fuerza para Radio o TV.
    NO se deduplica por programa (conteo directo de acontecimientos).
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        fuente: 'Radio' o 'TV'
        coctel_type: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: mensaje_fuerza, total_acontecimientos
    """
    
    # Mapeo de fuente
    id_fuente = 1 if fuente == 'Radio' else 2
    
    # Filtro de cóctel
    filtro_coctel = ""
    if coctel_type == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif coctel_type == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    # Si es "Todos", no agregamos filtro
    
    query = f"""
    SELECT 
        mf.mensaje as mensaje_fuerza,
        COUNT(DISTINCT a.id) as total_acontecimientos
    FROM acontecimientos a
    LEFT JOIN notas n ON a.id_nota = n.id
    LEFT JOIN mensaje_fuerza mf ON n.id_mensaje_fuerza = mf.id
    INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
    INNER JOIN programas p ON ap.id_programa = p.id
    WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        AND p.id_fuente = %s
        {filtro_coctel}
    GROUP BY mf.mensaje
    HAVING mf.mensaje IS NOT NULL
    ORDER BY total_acontecimientos DESC;
    """
    
    params = [fecha_inicio, fecha_fin, id_fuente]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_mensajes_fuerza_radio_tv: {e}")
        return pd.DataFrame()


def conteo_mensajes_fuerza_redes(
    fecha_inicio: str,
    fecha_fin: str,
    coctel_type: str
) -> pd.DataFrame:
    """
    Conteo de acontecimientos por mensaje_fuerza para Redes.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        coctel_type: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: mensaje_fuerza, total_acontecimientos
    """
    
    # Filtro de cóctel
    filtro_coctel = ""
    if coctel_type == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif coctel_type == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    
    query = f"""
    SELECT 
        mf.mensaje as mensaje_fuerza,
        COUNT(DISTINCT a.id) as total_acontecimientos
    FROM acontecimientos a
    LEFT JOIN notas n ON a.id_nota = n.id
    LEFT JOIN mensaje_fuerza mf ON n.id_mensaje_fuerza = mf.id
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        {filtro_coctel}
    GROUP BY mf.mensaje
    HAVING mf.mensaje IS NOT NULL
    ORDER BY total_acontecimientos DESC;
    """
    
    params = [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_mensajes_fuerza_redes: {e}")
        return pd.DataFrame()


def conteo_mensajes_fuerza_todos(
    fecha_inicio: str,
    fecha_fin: str,
    coctel_type: str
) -> pd.DataFrame:
    """
    Conteo de acontecimientos por mensaje_fuerza para TODAS las fuentes.
    Combina Radio, TV y Redes sin duplicar.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        coctel_type: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: mensaje_fuerza, total_acontecimientos
    """
    
    # Filtro de cóctel
    filtro_coctel = ""
    if coctel_type == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif coctel_type == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    
    query = f"""
    WITH todos_acontecimientos AS (
        -- Radio y TV
        SELECT DISTINCT
            a.id as acontecimiento_id,
            mf.mensaje as mensaje_fuerza
        FROM acontecimientos a
        LEFT JOIN notas n ON a.id_nota = n.id
        LEFT JOIN mensaje_fuerza mf ON n.id_mensaje_fuerza = mf.id
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN (1, 2)  -- Radio y TV
            {filtro_coctel}
        
        UNION
        
        -- Redes
        SELECT DISTINCT
            a.id as acontecimiento_id,
            mf.mensaje as mensaje_fuerza
        FROM acontecimientos a
        LEFT JOIN notas n ON a.id_nota = n.id
        LEFT JOIN mensaje_fuerza mf ON n.id_mensaje_fuerza = mf.id
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            {filtro_coctel}
    )
    SELECT 
        mensaje_fuerza,
        COUNT(acontecimiento_id) as total_acontecimientos
    FROM todos_acontecimientos
    WHERE mensaje_fuerza IS NOT NULL
    GROUP BY mensaje_fuerza
    ORDER BY total_acontecimientos DESC;
    """
    
    params = [fecha_inicio, fecha_fin, fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_mensajes_fuerza_todos: {e}")
        return pd.DataFrame()


def data_section_24_mensajes_fuerza_sql(
    fecha_inicio: str,
    fecha_fin: str,
    fuente: str,
    coctel_type: str
) -> pd.DataFrame:
    """
    Función principal para Gráfico 24: Porcentaje de cocteles por mensajes fuerza
    
    Cuenta cuántos acontecimientos tiene cada mensaje_fuerza y calcula el porcentaje.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        fuente: 'Radio', 'TV', 'Redes', o 'Todos'
        coctel_type: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: mensaje_fuerza, coctel (total), porcentaje
    """
    
    print(f"DEBUG grafico24: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, fuente={fuente}, coctel_type={coctel_type}")
    
    # Obtener datos según la fuente
    if fuente == 'Radio':
        print(f"🔍 Consultando Radio...")
        resultado = conteo_mensajes_fuerza_radio_tv(fecha_inicio, fecha_fin, 'Radio', coctel_type)
    elif fuente == 'TV':
        print(f"🔍 Consultando TV...")
        resultado = conteo_mensajes_fuerza_radio_tv(fecha_inicio, fecha_fin, 'TV', coctel_type)
    elif fuente == 'Redes':
        print(f"🔍 Consultando Redes...")
        resultado = conteo_mensajes_fuerza_redes(fecha_inicio, fecha_fin, coctel_type)
    elif fuente == 'Todos':
        print(f"🔍 Consultando Todos...")
        resultado = conteo_mensajes_fuerza_todos(fecha_inicio, fecha_fin, coctel_type)
    else:
        print(f"❌ Fuente inválida: {fuente}")
        return pd.DataFrame()
    
    if resultado is None or resultado.empty:
        print(f"⚠️ No se encontraron datos para {fuente}")
        return pd.DataFrame()
    
    print(f"✅ {fuente}: {len(resultado)} mensajes fuerza encontrados")
    
    # Renombrar columna para coincidir con el código original
    resultado = resultado.rename(columns={'total_acontecimientos': 'coctel'})
    
    # Calcular porcentaje
    total = resultado['coctel'].sum()
    resultado['porcentaje'] = (resultado['coctel'] / total) * 100
    
    # Ordenar por cantidad descendente
    resultado = resultado.sort_values('coctel', ascending=False)
    
    print(f"✅ Total acontecimientos: {total}")
    
    return resultado