# sections/functions/grafico9.py

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

def conteo_posiciones_agregado_radio_tv(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int], option_fuente: str = "Todos", option_nota: str = "Todos") -> pd.DataFrame:
    """
    Conteo agregado de posiciones para Radio y TV con múltiples lugares
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    # Crear placeholders para la query IN
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    # Filtros adicionales según opciones
    filtro_fuente = ""
    if option_fuente == "Radio":
        filtro_fuente = "AND p.id_fuente = 1"
    elif option_fuente == "TV":
        filtro_fuente = "AND p.id_fuente = 2"
    elif option_fuente == "Todos":
        filtro_fuente = "AND p.id_fuente IN (1, 2)"
    
    filtro_nota = ""
    if option_nota == "Con coctel":
        filtro_nota = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_nota = "AND a.id_nota IS NULL"
    
    query = f"""
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            a.id_lugar,
            a.fecha_registro,
            CASE 
                WHEN a.id_nota IS NOT NULL THEN 'CON_COCTEL'
                ELSE 'SIN_COCTEL'
            END as tipo_coctel,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar IN ({placeholders})
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            {filtro_fuente}
            {filtro_nota}
    ),
    acontecimientos_deduplicados AS (
        SELECT DISTINCT
            acontecimiento_id,
            id_posicion,
            id_lugar,
            fecha_registro,
            tipo_coctel,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_posicion, id_lugar, fecha_registro, tipo_coctel, id_fuente, programa_nombre
    )

    SELECT 
        ad.id_posicion as posicion,
        COUNT(*) as count
    FROM acontecimientos_deduplicados ad
    GROUP BY ad.id_posicion
    ORDER BY ad.id_posicion;
    """
    
    # Parámetros: ids_lugares + fecha_inicio + fecha_fin
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_posiciones_agregado_radio_tv: {e}")
        return pd.DataFrame()

def conteo_posiciones_agregado_redes(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int], option_nota: str = "Todos") -> pd.DataFrame:
    """
    Conteo agregado de posiciones para Redes Sociales con múltiples lugares
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    # Crear placeholders para la query IN
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    # Filtros adicionales según opciones
    filtro_nota = ""
    if option_nota == "Con coctel":
        filtro_nota = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_nota = "AND a.id_nota IS NULL"
    
    query = f"""
    SELECT 
        a.id_posicion as posicion,
        COUNT(*) as count
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE a.id_lugar IN ({placeholders})
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        {filtro_nota}
    GROUP BY a.id_posicion
    ORDER BY a.id_posicion;
    """
    
    # Parámetros: ids_lugares + fecha_inicio + fecha_fin
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_posiciones_agregado_redes: {e}")
        return pd.DataFrame()

def data_section_9_distribucion_posiciones_sql(fecha_inicio: str, fecha_fin: str, lugares: List[str], option_fuente: str = "Todos", option_nota: str = "Todos") -> pd.DataFrame:
    """
    Función principal que combina los conteos agregados de Radio/TV y Redes según los filtros
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugares: Lista de nombres de lugares ['Lima', 'Arequipa']
        option_fuente: 'Radio', 'TV', 'Redes', o 'Todos'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: posicion, count, Posición
    """
    
    print(f"DEBUG: Parámetros recibidos:")
    print(f"  fecha_inicio: {fecha_inicio}")
    print(f"  fecha_fin: {fecha_fin}")
    print(f"  lugares: {lugares}")
    print(f"  option_fuente: {option_fuente}")
    print(f"  option_nota: {option_nota}")
    
    # Convertir nombres de lugares a IDs
    ids_lugares = obtener_ids_lugares(lugares)
    print(f"DEBUG: IDs lugares obtenidos: {ids_lugares}")
    if not ids_lugares:
        print(f"ERROR: No se encontraron IDs para los lugares: {lugares}")
        return pd.DataFrame()
    
    resultado_final = pd.DataFrame()
    
    try:
        # Obtener datos según la fuente seleccionada
        if option_fuente in ["Radio", "TV", "Todos"]:
            print(f"DEBUG: Consultando Radio/TV...")
            resultado_radio_tv = conteo_posiciones_agregado_radio_tv(
                fecha_inicio, fecha_fin, ids_lugares, option_fuente, option_nota
            )
            print(f"DEBUG: Resultado Radio/TV: {len(resultado_radio_tv)} filas")
            print(f"DEBUG: Datos Radio/TV:\n{resultado_radio_tv}")
            if not resultado_radio_tv.empty:
                resultado_final = pd.concat([resultado_final, resultado_radio_tv], ignore_index=True)
        
        if option_fuente in ["Redes", "Todos"]:
            print(f"DEBUG: Consultando Redes...")
            resultado_redes = conteo_posiciones_agregado_redes(
                fecha_inicio, fecha_fin, ids_lugares, option_nota
            )
            print(f"DEBUG: Resultado Redes: {len(resultado_redes)} filas")
            print(f"DEBUG: Datos Redes:\n{resultado_redes}")
            if not resultado_redes.empty:
                resultado_final = pd.concat([resultado_final, resultado_redes], ignore_index=True)
        
        print(f"DEBUG: Resultado combinado antes de procesar: {len(resultado_final)} filas")
        print(f"DEBUG: Datos combinados:\n{resultado_final}")
        
        # Si tenemos datos de múltiples fuentes, sumar por posición
        if not resultado_final.empty and option_fuente == "Todos":
            print(f"DEBUG: Sumando por posición para 'Todos'...")
            resultado_final = resultado_final.groupby('posicion').agg({
                'count': 'sum'
            }).reset_index()
            print(f"DEBUG: Resultado después de sumar:\n{resultado_final}")
        
        # Agregar columna de nombre descriptivo para posiciones
        if not resultado_final.empty:
            resultado_final['Posición'] = resultado_final['posicion'].apply(lambda x: f'Posición {x}')
            print(f"DEBUG: Resultado final con nombres:\n{resultado_final}")
        
        return resultado_final
        
    except Exception as e:
        print(f"ERROR en data_section_9_distribucion_posiciones_sql: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def convertir_posicion_a_nombre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte los IDs de posición a nombres descriptivos si es necesario
    """
    # Mapeo de posiciones (ajustar según tus necesidades)
    posicion_dict = {
        1: 'Posición 1',
        2: 'Posición 2', 
        3: 'Posición 3',
        4: 'Posición 4',
        5: 'Posición 5'
    }
    
    if not df.empty and 'Posición' in df.columns:
        df['Posición'] = df['posicion'].map(posicion_dict).fillna(df['Posición'])
    
    return df