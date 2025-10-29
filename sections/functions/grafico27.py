# sections/functions/grafico27.py

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
    Obtiene el ID de un lugar dado su nombre.
    """
    query = "SELECT id FROM lugares WHERE nombre = %s LIMIT 1;"
    resultado = ejecutar_query(query, params=[nombre_lugar])
    
    if resultado is not None and not resultado.empty:
        return int(resultado.iloc[0]['id'])
    return None


def favor_contra_mensual_radio_tv(
    fecha_inicio: str,
    fecha_fin: str,
    ids_lugares: List[int],
    medio: str
) -> pd.DataFrame:
    """
    Cuenta notas a favor vs en contra por mes para Radio/TV.
    CUENTA REBOTES: Si acontecimiento está en 2 programas = cuenta 2 veces
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        ids_lugares: Lista de IDs de lugares
        medio: 'Radio', 'TV', o 'Todos'
    
    Returns:
        DataFrame con columnas: mes, a_favor, en_contra, total
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    # Filtro de fuente
    if medio == "Radio":
        filtro_fuente = "AND p.id_fuente = 1"
    elif medio == "TV":
        filtro_fuente = "AND p.id_fuente = 2"
    else:  # "Todos"
        filtro_fuente = "AND p.id_fuente IN (1, 2)"
    
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    WITH acontecimientos_programas AS (
        -- Paso 1: Obtener todas las combinaciones acontecimiento-programa
        SELECT DISTINCT
            a.id as acontecimiento_id,
            a.id_posicion,
            a.fecha_registro,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar IN ({placeholders})
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            {filtro_fuente}
            AND a.id_posicion IS NOT NULL
    ),
    acontecimientos_deduplicados AS (
        -- Paso 2: Eliminar duplicados EXACTOS
        -- Mantiene rebotes: mismo acontecimiento en diferentes programas
        SELECT DISTINCT
            acontecimiento_id,
            id_posicion,
            fecha_registro,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, id_posicion, fecha_registro, programa_nombre
    )
    SELECT 
        DATE_TRUNC('month', fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima') as mes,
        SUM(CASE WHEN id_posicion IN (1, 2) THEN 1 ELSE 0 END) as a_favor,
        SUM(CASE WHEN id_posicion IN (4, 5) THEN 1 ELSE 0 END) as en_contra,
        COUNT(*) as total
    FROM acontecimientos_deduplicados
    GROUP BY DATE_TRUNC('month', fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')
    ORDER BY mes;
    """
    
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en favor_contra_mensual_radio_tv: {e}")
        return pd.DataFrame()


def favor_contra_mensual_redes(
    fecha_inicio: str,
    fecha_fin: str,
    ids_lugares: List[int]
) -> pd.DataFrame:
    """
    Cuenta notas a favor vs en contra por mes para Redes.
    CUENTA REBOTES: Si acontecimiento está en 2 facebook_posts = cuenta 2 veces
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        ids_lugares: Lista de IDs de lugares
    
    Returns:
        DataFrame con columnas: mes, a_favor, en_contra, total
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    SELECT 
        DATE_TRUNC('month', a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima') as mes,
        SUM(CASE WHEN a.id_posicion IN (1, 2) THEN 1 ELSE 0 END) as a_favor,
        SUM(CASE WHEN a.id_posicion IN (4, 5) THEN 1 ELSE 0 END) as en_contra,
        COUNT(*) as total
    FROM acontecimientos a
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE a.id_lugar IN ({placeholders})
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        AND a.id_posicion IS NOT NULL
    GROUP BY DATE_TRUNC('month', a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')
    ORDER BY mes;
    """
    
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en favor_contra_mensual_redes: {e}")
        return pd.DataFrame()


def data_section_27_favor_contra_mensual_sql(
    fecha_inicio: str,
    fecha_fin: str,
    regiones: List[str],
    medio: str
) -> pd.DataFrame:
    """
    Función principal para Gráfico 27: Notas a favor vs en contra por mes
    
    Retorna el porcentaje de notas a favor y en contra agrupado por mes.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        regiones: Lista de nombres de regiones
        medio: 'Radio', 'TV', 'Redes', o 'Todos'
    
    Returns:
        DataFrame con columnas: mes_str, Tipo, Porcentaje
        Ejemplo:
        mes_str   | Tipo            | Porcentaje
        ----------|-----------------|------------
        Ene-25    | A favor (%)     | 45.2
        Ene-25    | En contra (%)   | 23.5
        Feb-25    | A favor (%)     | 48.1
        Feb-25    | En contra (%)   | 21.3
    """
    
    print(f"DEBUG grafico27: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}")
    print(f"📊 Regiones: {regiones} | 📻📺📱 Medio: {medio}")
    
    # Convertir nombres de lugares a IDs
    ids_lugares = []
    for region in regiones:
        id_lugar = obtener_id_lugar(region)
        if id_lugar:
            ids_lugares.append(id_lugar)
    
    if not ids_lugares:
        print(f"⚠️ No se encontraron IDs para las regiones: {regiones}")
        return pd.DataFrame()
    
    print(f"🔍 IDs de lugares: {ids_lugares}")
    
    # Obtener datos según el medio
    resultado = pd.DataFrame()
    
    if medio in ["Radio", "TV", "Todos"]:
        print(f"🔍 Consultando Radio/TV...")
        resultado_radio_tv = favor_contra_mensual_radio_tv(
            fecha_inicio, fecha_fin, ids_lugares, medio
        )
        if not resultado_radio_tv.empty:
            resultado = resultado_radio_tv
            print(f"📻📺 Radio/TV: {len(resultado_radio_tv)} meses encontrados")
    
    if medio == "Redes":
        print(f"🔍 Consultando Redes...")
        resultado_redes = favor_contra_mensual_redes(
            fecha_inicio, fecha_fin, ids_lugares
        )
        if not resultado_redes.empty:
            resultado = resultado_redes
            print(f"📱 Redes: {len(resultado_redes)} meses encontrados")
    
    elif medio == "Todos":
        # Combinar Radio/TV + Redes
        print(f"🔍 Consultando Redes...")
        resultado_redes = favor_contra_mensual_redes(
            fecha_inicio, fecha_fin, ids_lugares
        )
        
        if not resultado_redes.empty:
            # Sumar ambos resultados por mes
            if not resultado.empty:
                resultado = pd.concat([resultado, resultado_redes], ignore_index=True)
                resultado = resultado.groupby('mes', as_index=False).agg({
                    'a_favor': 'sum',
                    'en_contra': 'sum',
                    'total': 'sum'
                })
                print(f"📱 Redes: {len(resultado_redes)} meses encontrados")
            else:
                resultado = resultado_redes
    
    if resultado.empty:
        print(f"⚠️ No se encontraron datos en el rango de fechas")
        return pd.DataFrame()
    
    # Calcular porcentajes
    resultado['A favor (%)'] = (resultado['a_favor'] / resultado['total'] * 100).round(1)
    resultado['En contra (%)'] = (resultado['en_contra'] / resultado['total'] * 100).round(1)
    
    # Convertir a formato "long" para Plotly
    long_df = resultado.melt(
        id_vars=['mes'],
        value_vars=['A favor (%)', 'En contra (%)'],
        var_name='Tipo',
        value_name='Porcentaje'
    )
    
    # Formatear mes como string (Ene-25, Feb-25, etc.)
    long_df['mes_str'] = pd.to_datetime(long_df['mes']).dt.strftime('%b-%y')
    
    # Traducir meses a español
    meses_es = {
        'Jan': 'Ene', 'Feb': 'Feb', 'Mar': 'Mar', 'Apr': 'Abr',
        'May': 'May', 'Jun': 'Jun', 'Jul': 'Jul', 'Aug': 'Ago',
        'Sep': 'Sep', 'Oct': 'Oct', 'Nov': 'Nov', 'Dec': 'Dic'
    }
    
    for eng, esp in meses_es.items():
        long_df['mes_str'] = long_df['mes_str'].str.replace(eng, esp)
    
    # Ordenar por mes
    long_df = long_df.sort_values('mes').reset_index(drop=True)
    
    print(f"✅ Total: {len(long_df)} filas (2 × {len(resultado)} meses)")
    
    return long_df[['mes_str', 'Tipo', 'Porcentaje']]