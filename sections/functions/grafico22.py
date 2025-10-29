# sections/functions/grafico22.py

import pandas as pd
import psycopg2
import os
from typing import Optional, List, Any
from datetime import datetime
from dateutil.relativedelta import relativedelta

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
    Obtiene los IDs de los lugares por sus nombres
    """
    if not nombres_lugares:
        return []
    
    placeholders = ','.join(['%s'] * len(nombres_lugares))
    query = f"""
    SELECT id, nombre 
    FROM lugares 
    WHERE nombre IN ({placeholders});
    """
    
    try:
        resultado = ejecutar_query(query, params=nombres_lugares)
        if resultado is None or resultado.empty:
            print(f"No se encontraron lugares: {nombres_lugares}")
            return []
        return resultado['id'].tolist()
    except Exception as e:
        print(f"Error al buscar lugares: {e}")
        return []


def conteo_coctel_radio_tv_ultimos_3_meses(
    fecha_inicio: str,
    fecha_fin: str,
    ids_lugares: List[int],
    fuente: str
) -> pd.DataFrame:
    """
    Conteo de cocteles para Radio o TV en los últimos 3 meses agrupado por mes y lugar.
    Incluye deduplicación por nombre de programa.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        ids_lugares: Lista de IDs de lugares
        fuente: 'Radio' o 'TV'
    
    Returns:
        DataFrame con columnas: lugar, fecha_mes, total_coctel
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    # Mapeo de fuente
    id_fuente = 1 if fuente == 'Radio' else 2
    
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            l.nombre as lugar,
            DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')) as fecha_mes,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN lugares l ON a.id_lugar = l.id
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar IN ({placeholders})
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND a.id_nota IS NOT NULL  -- Solo cocteles
            AND p.id_fuente = %s
    ),
    acontecimientos_deduplicados AS (
        -- Deduplicar por nombre de programa
        SELECT DISTINCT
            acontecimiento_id,
            lugar,
            fecha_mes,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, lugar, fecha_mes, programa_nombre
    )
    SELECT 
        lugar,
        fecha_mes,
        COUNT(*) as total_coctel
    FROM acontecimientos_deduplicados
    GROUP BY lugar, fecha_mes
    ORDER BY lugar, fecha_mes;
    """
    
    params = ids_lugares + [fecha_inicio, fecha_fin, id_fuente]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_coctel_radio_tv_ultimos_3_meses: {e}")
        return pd.DataFrame()


def conteo_coctel_redes_ultimos_3_meses(
    fecha_inicio: str,
    fecha_fin: str,
    ids_lugares: List[int]
) -> pd.DataFrame:
    """
    Conteo de cocteles para Redes en los últimos 3 meses agrupado por mes y lugar.
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        ids_lugares: Lista de IDs de lugares
    
    Returns:
        DataFrame con columnas: lugar, fecha_mes, total_coctel
    """
    
    if not ids_lugares:
        return pd.DataFrame()
    
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    SELECT 
        l.nombre as lugar,
        DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')) as fecha_mes,
        COUNT(*) as total_coctel
    FROM acontecimientos a
    INNER JOIN lugares l ON a.id_lugar = l.id
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE a.id_lugar IN ({placeholders})
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        AND a.id_nota IS NOT NULL  -- Solo cocteles
    GROUP BY l.nombre, DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima'))
    ORDER BY l.nombre, fecha_mes;
    """
    
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_coctel_redes_ultimos_3_meses: {e}")
        return pd.DataFrame()


def data_section_22_ultimos_3_meses_sql(
    ano_fin: int,
    mes_fin: int,
    lugares: List[str],
    fuente: str
) -> pd.DataFrame:
    """
    Función principal para Gráfico 22: Porcentaje de cóctel en los últimos 3 meses por fuente
    
    Calcula automáticamente el rango de 3 meses hacia atrás desde la fecha de referencia.
    El porcentaje se calcula como: (coctel_lugar_mes / total_coctel_mes) * 100
    
    Args:
        ano_fin: Año de referencia (ej: 2024)
        mes_fin: Mes de referencia (1-12)
        lugares: Lista de nombres de lugares
        fuente: 'Radio', 'TV', o 'Redes'
    
    Returns:
        DataFrame con columnas: lugar, mes, fecha_mes, total_coctel, porcentaje_coctel
    """
    
    print(f"DEBUG grafico22: ano_fin={ano_fin}, mes_fin={mes_fin}, lugares={lugares}, fuente={fuente}")
    
    if not lugares:
        print("⚠️ No se especificaron lugares")
        return pd.DataFrame()
    
    # Calcular el rango de 3 meses
    fecha_fin_dt = datetime(ano_fin, mes_fin, 1)
    fecha_inicio_dt = fecha_fin_dt - relativedelta(months=2)  # 2 meses atrás = 3 meses total
    
    # Último día del mes final
    import calendar
    ultimo_dia = calendar.monthrange(fecha_fin_dt.year, fecha_fin_dt.month)[1]
    
    fecha_inicio = fecha_inicio_dt.strftime('%Y-%m-%d')
    fecha_fin = f"{fecha_fin_dt.year}-{fecha_fin_dt.month:02d}-{ultimo_dia:02d}"
    
    print(f"📅 Rango calculado: {fecha_inicio} a {fecha_fin}")
    
    # Convertir nombres de lugares a IDs
    ids_lugares = obtener_ids_lugares(lugares)
    
    if not ids_lugares:
        print(f"❌ No se encontraron IDs para los lugares: {lugares}")
        return pd.DataFrame()
    
    # Obtener datos según la fuente
    if fuente in ['Radio', 'TV']:
        print(f"🔍 Consultando {fuente}...")
        resultado = conteo_coctel_radio_tv_ultimos_3_meses(fecha_inicio, fecha_fin, ids_lugares, fuente)
    elif fuente == 'Redes':
        print(f"🔍 Consultando Redes...")
        resultado = conteo_coctel_redes_ultimos_3_meses(fecha_inicio, fecha_fin, ids_lugares)
    else:
        print(f"❌ Fuente inválida: {fuente}")
        return pd.DataFrame()
    
    if resultado.empty:
        print(f"⚠️ No se encontraron datos para {fuente}")
        return pd.DataFrame()
    
    print(f"✅ {fuente}: {len(resultado)} filas")
    
    # Calcular totales por mes (para el porcentaje)
    totales_mes = resultado.groupby('fecha_mes')['total_coctel'].sum().reset_index()
    totales_mes.columns = ['fecha_mes', 'total_mes']
    
    # Merge para calcular porcentajes
    resultado_final = resultado.merge(totales_mes, on='fecha_mes')
    resultado_final['porcentaje_coctel'] = round(
        (resultado_final['total_coctel'] / resultado_final['total_mes']) * 100,
        2
    )
    
    # Agregar nombre del mes en español
    meses_es = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    
    resultado_final['mes'] = resultado_final['fecha_mes'].apply(lambda x: meses_es[x.month])
    
    # Ordenar por lugar y fecha
    resultado_final = resultado_final.sort_values(['lugar', 'fecha_mes'])
    
    # Seleccionar columnas finales
    resultado_final = resultado_final[['lugar', 'mes', 'fecha_mes', 'total_coctel', 'porcentaje_coctel']]
    
    print(f"✅ Total combinaciones lugar-mes: {len(resultado_final)}")
    
    return resultado_final