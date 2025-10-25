# sections/functions/grafico18.py

import pandas as pd
import psycopg2
import os
from typing import Optional, List, Any

# Importar constantes
from config.constants import ID_POSICION_DICT

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


def conteo_por_canal_posicion(fecha_inicio: str, fecha_fin: str, lugar: int, 
                               fuente: str, option_nota: str) -> pd.DataFrame:
    """
    Conteo de acontecimientos por canal y posición para Radio/TV
    
    IMPORTANTE: Cuenta rebotes - si un acontecimiento aparece en 2 programas del mismo canal,
    cuenta 2 veces para ese canal.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: ID del lugar
        fuente: 'Radio' o 'TV'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: medio_nombre, id_posicion, frecuencia
    """
    
    # Filtro de fuente
    if fuente == "Radio":
        filtro_fuente = "AND p.id_fuente = 1"
    elif fuente == "TV":
        filtro_fuente = "AND p.id_fuente = 2"
    else:
        return pd.DataFrame()  # Redes no usa esta función
    
    # Filtro de cóctel
    filtro_coctel = ""
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    
    query = f"""
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            c.nombre as canal_nombre,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN canales c ON p.id_canal = c.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            {filtro_fuente}
            {filtro_coctel}
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por acontecimiento_id, canal, programa, posicion
        -- Si acontecimiento tiene 2 programas del mismo canal = cuenta 2 veces
        SELECT DISTINCT
            acontecimiento_id,
            id_posicion,
            canal_nombre,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_posicion, canal_nombre, id_fuente, programa_nombre
    )
    SELECT 
        canal_nombre as medio_nombre,
        id_posicion,
        COUNT(*) as frecuencia
    FROM acontecimientos_deduplicados
    GROUP BY canal_nombre, id_posicion
    ORDER BY SUM(COUNT(*)) OVER (PARTITION BY canal_nombre) DESC, canal_nombre, id_posicion;
    """
    
    params = [lugar, fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_por_canal_posicion: {e}")
        return pd.DataFrame()


def conteo_por_pagina_posicion(fecha_inicio: str, fecha_fin: str, lugar: int, 
                                option_nota: str) -> pd.DataFrame:
    """
    Conteo de acontecimientos por página de Facebook y posición para Redes
    
    IMPORTANTE: Cuenta rebotes - si un acontecimiento aparece en 2 facebook_posts,
    cuenta 2 veces.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: ID del lugar
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: medio_nombre, id_posicion, frecuencia
    """
    
    # Filtro de cóctel
    filtro_coctel = ""
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    
    query = f"""
    SELECT 
        fbp.nombre as medio_nombre,
        a.id_posicion,
        COUNT(*) as frecuencia
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    INNER JOIN facebook_posts fp ON afp.id_facebook_post = fp.id
    INNER JOIN facebook_pages fbp ON fp.id_facebook_page = fbp.id
    WHERE a.id_lugar = %s
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        {filtro_coctel}
    GROUP BY fbp.nombre, a.id_posicion
    ORDER BY SUM(COUNT(*)) OVER (PARTITION BY fbp.nombre) DESC, fbp.nombre, a.id_posicion;
    """
    
    params = [lugar, fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_por_pagina_posicion: {e}")
        return pd.DataFrame()


def data_section_18_tendencia_por_medio_sql(fecha_inicio: str, fecha_fin: str, 
                                            lugar: str, fuente: str, 
                                            option_nota: str) -> pd.DataFrame:
    """
    Función principal para calcular tendencia de notas por medio/canal (Gráfico 18)
    
    Retorna el conteo de acontecimientos por medio (canal/página) y posición,
    ordenado por frecuencia total descendente.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: Nombre del lugar
        fuente: 'Radio', 'TV', o 'Redes'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: medio_nombre, id_posicion, frecuencia
    """
    
    print(f"DEBUG grafico18: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugar={lugar}, fuente={fuente}, option_nota={option_nota}")
    
    # Obtener ID del lugar
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        return pd.DataFrame()
    
    # Obtener datos según la fuente seleccionada
    if fuente == "Redes":
        print(f"🔍 Consultando Redes (Facebook)...")
        resultado = conteo_por_pagina_posicion(
            fecha_inicio, fecha_fin, id_lugar, option_nota
        )
        if not resultado.empty:
            print(f"📱 Redes: {len(resultado)} combinaciones página-posición encontradas")
    else:  # Radio o TV
        print(f"🔍 Consultando {fuente}...")
        resultado = conteo_por_canal_posicion(
            fecha_inicio, fecha_fin, id_lugar, fuente, option_nota
        )
        if not resultado.empty:
            print(f"📻📺 {fuente}: {len(resultado)} combinaciones canal-posición encontradas")
    
    # Si tenemos datos, mapear posiciones a nombres
    if not resultado.empty:
        # Mapear id_posicion a nombres
        resultado['id_posicion'] = resultado['id_posicion'].map(ID_POSICION_DICT)
        
        return resultado
    
    return pd.DataFrame()