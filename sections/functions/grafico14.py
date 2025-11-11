# sections/functions/grafico14.py

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
        print(f"✅ Lugares encontrados: {len(ids_encontrados)} de {len(nombres_lugares)}")
        return ids_encontrados
        
    except Exception as e:
        print(f"Error al buscar lugares {nombres_lugares}: {e}")
        return []


def conteo_favor_contra_radio_tv(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int], 
                                  fuentes: List[str], option_nota: str) -> pd.DataFrame:
    """
    Conteo de notas a favor, en contra y neutral para Radio y TV
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        ids_lugares: Lista de IDs de lugares (vacío para todas las regiones)
        fuentes: Lista de fuentes ['RADIO', 'TV']
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: año_mes, a_favor, en_contra, neutral
    """
    
    # Mapeo de fuentes a IDs
    fuente_map = {'RADIO': 1, 'TV': 2}
    ids_fuentes = [fuente_map[f] for f in fuentes if f in fuente_map]
    
    if not ids_fuentes:
        return pd.DataFrame()
    
    # Filtro de lugares
    filtro_lugar = ""
    if ids_lugares:
        placeholders_lugares = ','.join(['%s'] * len(ids_lugares))
        filtro_lugar = f"AND a.id_lugar IN ({placeholders_lugares})"
    
    # Filtro de cóctel
    filtro_coctel = ""
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    
    # Crear placeholders para fuentes
    placeholders_fuentes = ','.join(['%s'] * len(ids_fuentes))
    
    query = f"""
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')) as mes,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            {filtro_lugar}
            {filtro_coctel}
            AND p.id_fuente IN ({placeholders_fuentes})
    ),
    acontecimientos_deduplicados AS (
        SELECT DISTINCT
            acontecimiento_id,
            id_posicion,
            mes,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_posicion, mes, id_fuente, programa_nombre
    )
    SELECT 
        TO_CHAR(mes, 'YYYY-MM') as año_mes,
        SUM(CASE WHEN id_posicion IN (1, 2) THEN 1 ELSE 0 END) as a_favor,
        SUM(CASE WHEN id_posicion IN (4, 5) THEN 1 ELSE 0 END) as en_contra,
        SUM(CASE WHEN id_posicion = 3 THEN 1 ELSE 0 END) as neutral
    FROM acontecimientos_deduplicados
    GROUP BY mes
    ORDER BY año_mes;
    """
    
    # Construir parámetros
    params = [fecha_inicio, fecha_fin]
    if ids_lugares:
        params.extend(ids_lugares)
    params.extend(ids_fuentes)
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_favor_contra_radio_tv: {e}")
        return pd.DataFrame()


def conteo_favor_contra_redes(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int], 
                               option_nota: str) -> pd.DataFrame:
    """
    Conteo de notas a favor, en contra y neutral para Redes Sociales
    
    IMPORTANTE: Cuenta cada acontecimiento_facebook_post individualmente.
    Si un acontecimiento tiene múltiples facebook_posts, se cuenta múltiples veces
    (esto es consistente con el comportamiento de Radio/TV con múltiples programas)
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        ids_lugares: Lista de IDs de lugares (vacío para todas las regiones)
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: año_mes, a_favor, en_contra, neutral
    """
    
    # Filtro de lugares
    filtro_lugar = ""
    if ids_lugares:
        placeholders_lugares = ','.join(['%s'] * len(ids_lugares))
        filtro_lugar = f"AND a.id_lugar IN ({placeholders_lugares})"
    
    # Filtro de cóctel
    filtro_coctel = ""
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    
    query = f"""
    SELECT 
        TO_CHAR(DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')), 'YYYY-MM') as año_mes,
        SUM(CASE WHEN a.id_posicion IN (1, 2) THEN 1 ELSE 0 END) as a_favor,
        SUM(CASE WHEN a.id_posicion IN (4, 5) THEN 1 ELSE 0 END) as en_contra,
        SUM(CASE WHEN a.id_posicion = 3 THEN 1 ELSE 0 END) as neutral
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        {filtro_lugar}
        {filtro_coctel}
    GROUP BY DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima'))
    ORDER BY año_mes;
    """
    
    # Construir parámetros
    params = [fecha_inicio, fecha_fin]
    if ids_lugares:
        params.extend(ids_lugares)
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_favor_contra_redes: {e}")
        return pd.DataFrame()


def data_section_14_favor_contra_neutral_sql(fecha_inicio: str, fecha_fin: str, 
                                             lugares: List[str], fuentes: List[str], 
                                             option_nota: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Función principal para calcular porcentaje de notas a favor, neutral y en contra
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugares: Lista de nombres de lugares (vacío para todas las regiones)
        fuentes: Lista de fuentes ['RADIO', 'TV', 'REDES']
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        Tuple con (conteo_pct, long_df, conteo_abs):
        - conteo_pct: DataFrame con porcentajes
        - long_df: DataFrame en formato largo para gráfico
        - conteo_abs: DataFrame con conteos absolutos
    """
    
    print(f"DEBUG grafico14: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugares={lugares}, fuentes={fuentes}, option_nota={option_nota}")
    
    # Obtener IDs de lugares
    ids_lugares = []
    if lugares:  # Si hay lugares específicos
        ids_lugares = obtener_ids_lugares(lugares)
        if not ids_lugares:
            print(f"⚠️ No se encontraron IDs para los lugares: {lugares}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        print(f"✅ IDs de lugares: {ids_lugares}")
    else:  # Si está vacío, significa "todas las regiones"
        print(f"✅ Consultando todas las regiones")
    
    # Separar fuentes entre Radio/TV y Redes
    fuentes_radio_tv = [f for f in fuentes if f in ['RADIO', 'TV']]
    incluye_redes = 'REDES' in fuentes
    
    # Inicializar DataFrames vacíos
    resultado_radio_tv = pd.DataFrame()
    resultado_redes = pd.DataFrame()
    
    # Obtener datos de Radio/TV
    if fuentes_radio_tv:
        print(f"🔍 Consultando Radio/TV...")
        resultado_radio_tv = conteo_favor_contra_radio_tv(
            fecha_inicio, fecha_fin, ids_lugares, fuentes_radio_tv, option_nota
        )
        print(f"📻📺 Radio/TV: {len(resultado_radio_tv)} filas")
    
    # Obtener datos de Redes
    if incluye_redes:
        print(f"🔍 Consultando Redes...")
        resultado_redes = conteo_favor_contra_redes(
            fecha_inicio, fecha_fin, ids_lugares, option_nota
        )
        print(f"📱 Redes: {len(resultado_redes)} filas")
    
    # Combinar resultados
    if not resultado_radio_tv.empty and not resultado_redes.empty:
        # Combinar y agrupar por año_mes
        resultado_combinado = pd.concat([resultado_radio_tv, resultado_redes], ignore_index=True)
        conteo_abs = resultado_combinado.groupby('año_mes').agg({
            'a_favor': 'sum',
            'en_contra': 'sum',
            'neutral': 'sum'
        }).reset_index()
    elif not resultado_radio_tv.empty:
        conteo_abs = resultado_radio_tv
    elif not resultado_redes.empty:
        conteo_abs = resultado_redes
    else:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Calcular porcentajes
    conteo_pct = conteo_abs.copy()
    conteo_pct["total"] = (
        conteo_pct["a_favor"] + conteo_pct["en_contra"] + conteo_pct["neutral"]
    )
    
    conteo_pct["a_favor_pct"] = (conteo_pct['a_favor'] / conteo_pct['total'] * 100).round(2)
    conteo_pct["en_contra_pct"] = (conteo_pct['en_contra'] / conteo_pct['total'] * 100).round(2)
    conteo_pct["neutral_pct"] = (conteo_pct['neutral'] / conteo_pct['total'] * 100).round(2)
    
    conteo_pct = conteo_pct[["año_mes", "a_favor_pct", "neutral_pct", "en_contra_pct"]].dropna()
    
    # Crear formato largo para gráfico
    long_df = conteo_pct.melt(
        id_vars=["año_mes"],
        var_name="Tipo de Nota",
        value_name="Porcentaje"
    )
    
    return conteo_pct, long_df, conteo_abs