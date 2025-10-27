# sections/functions/grafico20.py

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


def conteo_actores_posiciones_radio_tv(fecha_inicio: str, fecha_fin: str, lugar: int, 
                                        fuente: str, option_nota: str) -> pd.DataFrame:
    """
    Conteo de actores y posiciones para Radio/TV
    
    LÓGICA DE REBOTES:
    - Si acontecimiento tiene 2 programas y 2 actores = cuenta 4 veces (2×2)
    - Excluye actores con nombre 'periodista'
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: ID del lugar
        fuente: 'Radio', 'TV', o 'Todos'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: nombre, posicion, frecuencia
    """
    
    # Filtro de fuente
    if fuente == "Radio":
        filtro_fuente = "AND p.id_fuente = 1"
    elif fuente == "TV":
        filtro_fuente = "AND p.id_fuente = 2"
    elif fuente == "Todos":
        filtro_fuente = "AND p.id_fuente IN (1, 2)"
    else:
        return pd.DataFrame()
    
    # Filtro de cóctel
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    else:
        filtro_coctel = ""  # 'Todos'
    
    query = f"""
    WITH acontecimientos_programas_actores AS (
        -- Paso 1: Producto cartesiano de programas × actores
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            ac.nombre as actor_nombre,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN acontecimiento_actor aa ON a.id = aa.id_acontecimiento
        INNER JOIN actores ac ON aa.id_actor = ac.id
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            {filtro_fuente}
            {filtro_coctel}
            AND LOWER(ac.nombre) != 'periodista'  -- Excluir 'periodista'
            AND ac.nombre IS NOT NULL
            AND ac.nombre != ''
    ),
    acontecimientos_deduplicados AS (
        -- Paso 2: Eliminar duplicados EXACTOS
        -- Mantiene: 2 programas × 2 actores = 4 conteos
        SELECT DISTINCT
            acontecimiento_id,
            id_posicion,
            actor_nombre,
            programa_nombre
        FROM acontecimientos_programas_actores
        GROUP BY acontecimiento_id, id_posicion, actor_nombre, programa_nombre
    )
    -- Paso 3: Contar por actor y posición
    SELECT 
        actor_nombre as nombre,
        id_posicion as posicion,
        COUNT(*) as frecuencia
    FROM acontecimientos_deduplicados
    GROUP BY actor_nombre, id_posicion
    ORDER BY actor_nombre, id_posicion;
    """
    
    params = [lugar, fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_actores_posiciones_radio_tv: {e}")
        return pd.DataFrame()


def conteo_actores_posiciones_redes(fecha_inicio: str, fecha_fin: str, lugar: int, 
                                     option_nota: str) -> pd.DataFrame:
    """
    Conteo de actores y posiciones para Redes
    
    LÓGICA DE REBOTES:
    - Si acontecimiento tiene 2 facebook_posts y 2 actores = cuenta 4 veces (2×2)
    - Excluye actores con nombre 'periodista'
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: ID del lugar
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: nombre, posicion, frecuencia
    """
    
    # Filtro de cóctel
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    else:
        filtro_coctel = ""  # 'Todos'
    
    query = f"""
    SELECT 
        ac.nombre,
        a.id_posicion as posicion,
        COUNT(*) as frecuencia
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    INNER JOIN acontecimiento_actor aa ON a.id = aa.id_acontecimiento
    INNER JOIN actores ac ON aa.id_actor = ac.id
    WHERE a.id_lugar = %s
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        {filtro_coctel}
        AND LOWER(ac.nombre) != 'periodista'  -- Excluir 'periodista'
        AND ac.nombre IS NOT NULL
        AND ac.nombre != ''
    GROUP BY ac.nombre, a.id_posicion
    ORDER BY ac.nombre, a.id_posicion;
    """
    
    params = [lugar, fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_actores_posiciones_redes: {e}")
        return pd.DataFrame()


def data_section_20_actores_posiciones_sql(fecha_inicio: str, fecha_fin: str, 
                                            lugar: str, fuente: str, 
                                            option_nota: str, top_n: int = 10) -> pd.DataFrame:
    """
    Función principal para calcular actores y posiciones (Gráfico 20)
    
    Retorna los TOP N actores por frecuencia total, con sus posiciones desglosadas.
    Excluye actores con nombre 'periodista'.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugar: Nombre del lugar
        fuente: 'Radio', 'TV', 'Redes', o 'Todos'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
        top_n: Número de actores top a retornar (default: 10)
    
    Returns:
        DataFrame con columnas: nombre, posicion, frecuencia
    """
    
    print(f"DEBUG grafico20: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, lugar={lugar}, fuente={fuente}, option_nota={option_nota}")
    
    # Obtener ID del lugar
    id_lugar = obtener_id_lugar(lugar)
    if id_lugar is None:
        return pd.DataFrame()
    
    # Inicializar resultado
    resultado_final = pd.DataFrame()
    
    # Obtener datos según la fuente seleccionada
    if fuente in ["Radio", "TV", "Todos"]:
        print(f"🔍 Consultando Radio/TV...")
        resultado_radio_tv = conteo_actores_posiciones_radio_tv(
            fecha_inicio, fecha_fin, id_lugar, fuente, option_nota
        )
        if not resultado_radio_tv.empty:
            resultado_final = pd.concat([resultado_final, resultado_radio_tv], ignore_index=True)
            print(f"📻📺 Radio/TV: {len(resultado_radio_tv)} combinaciones actor-posición encontradas")
    
    if fuente in ["Redes", "Todos"]:
        print(f"🔍 Consultando Redes...")
        resultado_redes = conteo_actores_posiciones_redes(
            fecha_inicio, fecha_fin, id_lugar, option_nota
        )
        if not resultado_redes.empty:
            resultado_final = pd.concat([resultado_final, resultado_redes], ignore_index=True)
            print(f"📱 Redes: {len(resultado_redes)} combinaciones actor-posición encontradas")
    
    # Si tenemos datos, procesar
    if not resultado_final.empty:
        # Agrupar por nombre y posición (sumar frecuencias de Radio/TV y Redes)
        df_grouped = resultado_final.groupby(['nombre', 'posicion'])['frecuencia'].sum().reset_index()
        
        # Encontrar TOP N actores por frecuencia total
        top_actores = df_grouped.groupby('nombre')['frecuencia'].sum().nlargest(top_n).index
        
        # Filtrar solo los TOP N actores
        df_top = df_grouped[df_grouped['nombre'].isin(top_actores)]
        
        # Mapear posicion a nombres
        df_top['posicion'] = df_top['posicion'].map(ID_POSICION_DICT)
        
        # Ordenar por frecuencia total descendente
        actor_totals = df_top.groupby('nombre')['frecuencia'].sum().sort_values(ascending=False)
        df_top['nombre'] = pd.Categorical(df_top['nombre'], categories=actor_totals.index, ordered=True)
        df_top = df_top.sort_values(['nombre', 'posicion'])
        
        print(f"✅ TOP {len(top_actores)} actores encontrados (excluyendo 'periodista')")
        return df_top
    
    return pd.DataFrame()