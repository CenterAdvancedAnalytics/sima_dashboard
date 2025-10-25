# sections/functions/grafico16.py

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


def conteo_temas_posiciones_radio_tv(fecha_inicio: str, fecha_fin: str, lugar: int, 
                                      fuente: str, option_nota: str) -> pd.DataFrame:
    """
    Conteo de temas y posiciones para Radio y TV
    
    IMPORTANTE: Un acontecimiento puede tener múltiples temas Y múltiples programas.
    Por lo tanto, si acontecimiento tiene 2 programas y 2 temas = cuenta 4 veces (2×2)
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: ID del lugar
        fuente: 'Radio', 'TV', o 'Todos'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: descripcion, id_posicion, frecuencia
    """
    
    # Filtro de fuente
    filtro_fuente = ""
    if fuente == "Radio":
        filtro_fuente = "AND p.id_fuente = 1"
    elif fuente == "TV":
        filtro_fuente = "AND p.id_fuente = 2"
    elif fuente == "Todos":
        filtro_fuente = "AND p.id_fuente IN (1, 2)"
    
    # Filtro de cóctel
    filtro_coctel = ""
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    
    query = f"""
    WITH acontecimientos_programas_temas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            t.descripcion as tema_descripcion,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN acontecimiento_tema at ON a.id = at.id_acontecimiento
        INNER JOIN temas t ON at.id_tema = t.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            {filtro_fuente}
            {filtro_coctel}
            AND t.descripcion IS NOT NULL
            AND t.descripcion != ''
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por acontecimiento_id, tema, programa_nombre, posicion
        -- Si acontecimiento tiene 2 programas y 2 temas = 4 filas (2x2)
        SELECT DISTINCT
            acontecimiento_id,
            id_posicion,
            tema_descripcion,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas_temas
        GROUP BY acontecimiento_id, id_posicion, tema_descripcion, id_fuente, programa_nombre
    )
    SELECT 
        tema_descripcion as descripcion,
        id_posicion,
        COUNT(*) as frecuencia
    FROM acontecimientos_deduplicados
    GROUP BY tema_descripcion, id_posicion
    ORDER BY tema_descripcion, id_posicion;
    """
    
    params = [lugar, fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_temas_posiciones_radio_tv: {e}")
        return pd.DataFrame()


def conteo_temas_posiciones_redes(fecha_inicio: str, fecha_fin: str, lugar: int, 
                                   option_nota: str) -> pd.DataFrame:
    """
    Conteo de temas y posiciones para Redes Sociales
    
    IMPORTANTE: Un acontecimiento puede tener múltiples temas Y múltiples facebook_posts.
    Por lo tanto, si acontecimiento tiene 2 posts y 2 temas = cuenta 4 veces (2×2)
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: ID del lugar
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: descripcion, id_posicion, frecuencia
    """
    
    # Filtro de cóctel
    filtro_coctel = ""
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    
    query = f"""
    SELECT 
        t.descripcion,
        a.id_posicion,
        COUNT(*) as frecuencia
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    INNER JOIN acontecimiento_tema at ON a.id = at.id_acontecimiento
    INNER JOIN temas t ON at.id_tema = t.id
    WHERE a.id_lugar = %s
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        {filtro_coctel}
        AND t.descripcion IS NOT NULL
        AND t.descripcion != ''
    GROUP BY t.descripcion, a.id_posicion
    ORDER BY t.descripcion, a.id_posicion;
    """
    
    params = [lugar, fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_temas_posiciones_redes: {e}")
        return pd.DataFrame()


def data_section_16_mensajes_por_tema_sql(fecha_inicio: str, fecha_fin: str, 
                                          lugar: str, fuente: str, 
                                          option_nota: str, top_n: int = 10) -> pd.DataFrame:
    """
    Función principal para calcular mensajes por tema con posiciones (Gráfico 16)
    
    Retorna los TOP N temas por frecuencia, desglosados por posición (a favor, en contra, etc.)
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: Nombre del lugar
        fuente: 'Radio', 'TV', 'Redes', o 'Todos'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
        top_n: Número de temas top a retornar (default: 10)
    
    Returns:
        DataFrame con columnas: descripcion, id_posicion, frecuencia
    """
    
    print(f"DEBUG grafico16: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugar={lugar}, fuente={fuente}, option_nota={option_nota}")
    
    # Obtener ID del lugar
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        return pd.DataFrame()
    
    # Inicializar resultado
    resultado_final = pd.DataFrame()
    
    # Obtener datos según la fuente seleccionada
    if fuente in ["Radio", "TV", "Todos"]:
        print(f"🔍 Consultando Radio/TV...")
        resultado_radio_tv = conteo_temas_posiciones_radio_tv(
            fecha_inicio, fecha_fin, id_lugar, fuente, option_nota
        )
        if not resultado_radio_tv.empty:
            resultado_final = pd.concat([resultado_final, resultado_radio_tv], ignore_index=True)
            print(f"📻📺 Radio/TV: {len(resultado_radio_tv)} combinaciones tema-posicion encontradas")
    
    if fuente in ["Redes", "Todos"]:
        print(f"🔍 Consultando Redes...")
        resultado_redes = conteo_temas_posiciones_redes(
            fecha_inicio, fecha_fin, id_lugar, option_nota
        )
        if not resultado_redes.empty:
            resultado_final = pd.concat([resultado_final, resultado_redes], ignore_index=True)
            print(f"📱 Redes: {len(resultado_redes)} combinaciones tema-posicion encontradas")
    
    # Si tenemos datos, procesar
    if not resultado_final.empty:
        # Agrupar por descripcion y id_posicion (sumar frecuencias de Radio/TV y Redes)
        df_grouped = resultado_final.groupby(['descripcion', 'id_posicion'])['frecuencia'].sum().reset_index()
        
        # Encontrar TOP N temas por frecuencia total
        top_temas = df_grouped.groupby('descripcion')['frecuencia'].sum().nlargest(top_n).index
        
        # Filtrar solo los TOP N temas
        df_top = df_grouped[df_grouped['descripcion'].isin(top_temas)]
        
        # Mapear id_posicion a nombres
        df_top['id_posicion'] = df_top['id_posicion'].map(ID_POSICION_DICT)
        
        return df_top
    
    return pd.DataFrame()