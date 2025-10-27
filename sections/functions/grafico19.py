# sections/functions/grafico19.py

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


def conteo_por_posicion_radio_tv(fecha_inicio: str, fecha_fin: str, option_nota: str) -> pd.DataFrame:
    """
    Conteo de acontecimientos por posición para Radio/TV en un rango de tiempo
    
    LÓGICA DE REBOTES:
    - Si acontecimiento 123 aparece en Programa A y Programa B = cuenta 2 veces
    - Deduplicación: Solo elimina duplicados EXACTOS (mismo acontecimiento + mismo programa)
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: id_posicion, frecuencia
    """
    
    # Construir filtro de cóctel (evitar redundancia)
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    else:
        filtro_coctel = ""  # 'Todos' - no filtra
    
    query = f"""
    WITH acontecimientos_programas AS (
        -- Paso 1: Obtener todas las combinaciones acontecimiento-programa
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN (1, 2)  -- Radio (1) y TV (2)
            {filtro_coctel}
    ),
    acontecimientos_deduplicados AS (
        -- Paso 2: Eliminar duplicados EXACTOS
        -- Mantiene rebotes: mismo acontecimiento en diferentes programas
        SELECT DISTINCT
            acontecimiento_id,
            id_posicion,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_posicion, programa_nombre
    )
    -- Paso 3: Contar por posición
    SELECT 
        id_posicion,
        COUNT(*) as frecuencia
    FROM acontecimientos_deduplicados
    GROUP BY id_posicion
    ORDER BY id_posicion;
    """
    
    params = [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_por_posicion_radio_tv: {e}")
        return pd.DataFrame()


def conteo_por_posicion_redes(fecha_inicio: str, fecha_fin: str, option_nota: str) -> pd.DataFrame:
    """
    Conteo de acontecimientos por posición para Redes en un rango de tiempo
    
    LÓGICA DE REBOTES:
    - Si acontecimiento tiene 2 facebook_posts = cuenta 2 veces
    - El INNER JOIN con acontecimiento_facebook_post multiplica las filas automáticamente
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: id_posicion, frecuencia
    """
    
    # Construir filtro de cóctel (evitar redundancia)
    if option_nota == "Con coctel":
        filtro_coctel = "AND a.id_nota IS NOT NULL"
    elif option_nota == "Sin coctel":
        filtro_coctel = "AND a.id_nota IS NULL"
    else:
        filtro_coctel = ""  # 'Todos' - no filtra
    
    query = f"""
    -- Query simple: El JOIN ya multiplica por cada facebook_post
    SELECT 
        a.id_posicion,
        COUNT(*) as frecuencia
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        {filtro_coctel}
    GROUP BY a.id_posicion
    ORDER BY a.id_posicion;
    """
    
    params = [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_por_posicion_redes: {e}")
        return pd.DataFrame()


def data_section_19_notas_tiempo_posicion_sql(fecha_inicio: str, fecha_fin: str, 
                                               option_nota: str) -> pd.DataFrame:
    """
    Función principal para calcular notas por posición en un rango de tiempo (Gráfico 19)
    
    Retorna el conteo de acontecimientos por posición (a favor, en contra, neutral, etc.)
    en un rango de tiempo, incluyendo TODOS los lugares y TODAS las fuentes (Radio + TV + Redes).
    Cuenta rebotes.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        option_nota: 'Con coctel', 'Sin coctel', o 'Todos'
    
    Returns:
        DataFrame con columnas: id_posicion, frecuencia
    """
    
    print(f"DEBUG grafico19: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}, option_nota={option_nota}")
    print(f"📍 TODOS los lugares | 📻📺📱 TODAS las fuentes (Radio + TV + Redes)")
    
    # Inicializar resultado
    resultado_final = pd.DataFrame()
    
    # Obtener datos de Radio/TV
    print(f"🔍 Consultando Radio + TV...")
    resultado_radio_tv = conteo_por_posicion_radio_tv(fecha_inicio, fecha_fin, option_nota)
    if not resultado_radio_tv.empty:
        resultado_final = pd.concat([resultado_final, resultado_radio_tv], ignore_index=True)
        print(f"📻📺 Radio/TV: {len(resultado_radio_tv)} posiciones encontradas")
    
    # Obtener datos de Redes
    print(f"🔍 Consultando Redes...")
    resultado_redes = conteo_por_posicion_redes(fecha_inicio, fecha_fin, option_nota)
    if not resultado_redes.empty:
        resultado_final = pd.concat([resultado_final, resultado_redes], ignore_index=True)
        print(f"📱 Redes: {len(resultado_redes)} posiciones encontradas")
    
    # Si tenemos datos, agrupar por posición
    if not resultado_final.empty:
        # Sumar frecuencias de Radio/TV y Redes por posición
        df_grouped = resultado_final.groupby('id_posicion')['frecuencia'].sum().reset_index()
        
        # Mapear id_posicion a nombres
        df_grouped['id_posicion'] = df_grouped['id_posicion'].map(ID_POSICION_DICT)
        
        # Ordenar por id_posicion para consistencia visual
        df_grouped = df_grouped.sort_values('id_posicion').reset_index(drop=True)
        
        print(f"✅ Total: {len(df_grouped)} posiciones con datos")
        return df_grouped
    
    return pd.DataFrame()