# sections/functions/grafico23.py

from .sn import ejecutar_query
import pandas as pd
from typing import List


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

def conteo_acontecimientos_radio_tv_por_lugar_mes(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int]) -> pd.DataFrame:
    """
    Conteo mensual de acontecimientos con cóctel para Radio/TV por lugar y mes
    FILTRAR SOLO LOS QUE TIENEN CÓCTEL (igual que grafico13.py)
    """
    if not ids_lugares:
        return pd.DataFrame()
    
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    WITH acontecimientos_programas AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            l.nombre as lugar_nombre,
            DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')) as año_mes,
            p.id_fuente,
            p.nombre as programa_nombre
        FROM acontecimientos a
        INNER JOIN lugares l ON a.id_lugar = l.id
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        WHERE a.id_lugar IN ({placeholders})
            AND a.id_nota IS NOT NULL  -- ✅ SOLO CON CÓCTEL (igual que grafico13.py)
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN (1, 2)  -- Solo Radio (1) y TV (2)
    ),
    acontecimientos_deduplicados AS (
        SELECT DISTINCT
            acontecimiento_id,
            lugar_nombre,
            año_mes,
            id_fuente,
            programa_nombre
        FROM acontecimientos_programas
        GROUP BY acontecimiento_id, lugar_nombre, año_mes, id_fuente, programa_nombre
    )
    SELECT 
        ad.lugar_nombre as lugar,
        TO_CHAR(ad.año_mes, 'YYYY-MM') as año_mes,
        CASE 
            WHEN ad.id_fuente = 1 THEN 'Radio'
            WHEN ad.id_fuente = 2 THEN 'TV'
        END as fuente,
        COUNT(*) as coctel  -- ✅ COUNT directo (ya filtrado solo cóctel)
    FROM acontecimientos_deduplicados ad
    GROUP BY 
        ad.lugar_nombre,
        ad.año_mes, 
        ad.id_fuente
    ORDER BY 
        ad.lugar_nombre,
        ad.año_mes, 
        ad.id_fuente;
    """
    
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_acontecimientos_radio_tv_por_lugar_mes: {e}")
        return pd.DataFrame()

def conteo_acontecimientos_redes_por_lugar_mes(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int]) -> pd.DataFrame:
    """
    Conteo de acontecimientos con coctel para Redes por lugar y mes
    FILTRAR SOLO LOS QUE TIENEN CÓCTEL (igual que grafico13.py)
    """
    if not ids_lugares:
        return pd.DataFrame()
    
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    SELECT 
        l.nombre as lugar,
        TO_CHAR(DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')), 'YYYY-MM') as año_mes,
        'Redes' as fuente,
        COUNT(*) as coctel  -- ✅ COUNT directo (ya filtrado solo cóctel)
    FROM acontecimientos a
    INNER JOIN lugares l ON a.id_lugar = l.id
    INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    WHERE a.id_lugar IN ({placeholders})
        AND a.id_nota IS NOT NULL  -- ✅ SOLO CON CÓCTEL (igual que grafico13.py)
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
        AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
    GROUP BY 
        l.nombre,
        DATE_TRUNC('month', (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima'))
    ORDER BY 
        l.nombre,
        año_mes;
    """
    
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en conteo_acontecimientos_redes_por_lugar_mes: {e}")
        return pd.DataFrame()
    
def data_section_23_evolucion_mensual_sql(fecha_inicio: str, fecha_fin: str, lugares: List[str]) -> pd.DataFrame:
    """
    Evolución mensual de cocteles por fuente (Radio/TV/Redes) para gráfico de líneas
    MANEJA REDES POR SEPARADO porque no tiene id_fuente en la tabla fuentes
    """
    
    # Convertir nombres de lugares a IDs
    ids_lugares = obtener_ids_lugares(lugares)
    
    if not ids_lugares:
        return pd.DataFrame()
    
    print(f"🔍 Consultando Radio/TV...")
    # 1. Obtener datos de Radio/TV (tienen id_fuente en tabla fuentes)
    resultado_radio_tv = conteo_acontecimientos_radio_tv_por_lugar_mes(fecha_inicio, fecha_fin, ids_lugares)
    print(f"📻📺 Radio/TV: {len(resultado_radio_tv)} filas")
    
    print(f"🔍 Consultando Redes...")
    # 2. Obtener datos de Redes (NO tienen id_fuente, se maneja por separado)
    resultado_redes = conteo_acontecimientos_redes_por_lugar_mes(fecha_inicio, fecha_fin, ids_lugares)
    print(f"📱 Redes: {len(resultado_redes)} filas")
    
    # 3. Combinar resultados
    resultado_combinado = pd.DataFrame()
    
    if not resultado_radio_tv.empty:
        resultado_combinado = pd.concat([resultado_combinado, resultado_radio_tv], ignore_index=True)
    
    if not resultado_redes.empty:
        resultado_combinado = pd.concat([resultado_combinado, resultado_redes], ignore_index=True)
    
    if not resultado_combinado.empty:
        # 4. Agrupar por año_mes y fuente (sumar todos los lugares)
        resultado_final = resultado_combinado.groupby(['año_mes', 'fuente'], as_index=False).agg({'coctel': 'sum'})
        
        print(f"✅ Datos finales: {len(resultado_final)} filas")
        print(f"📊 Fuentes encontradas: {resultado_final['fuente'].unique()}")
        print(f"📅 Meses encontrados: {sorted(resultado_final['año_mes'].unique())}")
        
        return resultado_final
    else:
        print("⚠️ No se encontraron datos")
        return pd.DataFrame()

def data_section_23_add_total_line(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregar línea Total al DataFrame del gráfico 23
    """
    if df.empty:
        return df
    
    # Calcular totales por mes
    total_monthly = df.groupby('año_mes', as_index=False).agg({'coctel': 'sum'})
    total_monthly['fuente'] = 'Total'
    
    # Combinar datos originales con totales
    combined = pd.concat([df, total_monthly], ignore_index=True)
    
    print(f"📈 Líneas en gráfico: {combined['fuente'].unique()}")
    
    return combined.sort_values(['año_mes', 'fuente'])