# sections/functions/grafico12.py
"""
Gráfico 12: Reporte semanal de cuántas radios, redes y TV generaron cóctel
Cuenta MEDIOS ÚNICOS (canales/páginas) que generaron al menos un cóctel
"""

from .sn import ejecutar_query
import pandas as pd
from typing import List


def obtener_ids_lugares(nombres_lugares: List[str]) -> List[int]:
    """
    Convierte lista de nombres de lugares a lista de IDs
    """
    if not nombres_lugares:
        return []
    
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


def contar_canales_radio_tv_con_coctel(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int]) -> pd.DataFrame:
    """
    Cuenta CANALES ÚNICOS de Radio y TV que generaron al menos un cóctel
    Devuelve tanto el resumen agregado como el detalle desagregado
    
    Returns:
        DataFrame con columnas: lugar, fuente, canal_nombre, cantidad_cocteles
    """
    if not ids_lugares:
        return pd.DataFrame()
    
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    WITH acontecimientos_con_coctel AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            l.nombre as lugar,
            c.nombre as canal_nombre,
            p.id_fuente,
            CASE 
                WHEN p.id_fuente = 1 THEN 'Radio'
                WHEN p.id_fuente = 2 THEN 'TV'
            END as fuente
        FROM acontecimientos a
        INNER JOIN lugares l ON a.id_lugar = l.id
        INNER JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        INNER JOIN programas p ON ap.id_programa = p.id
        INNER JOIN canales c ON p.id_canal = c.id
        WHERE a.id_lugar IN ({placeholders})
            AND a.id_nota IS NOT NULL  -- Solo con cóctel
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND p.id_fuente IN (1, 2)  -- Radio y TV
    )
    SELECT 
        lugar,
        fuente,
        canal_nombre,
        COUNT(*) as cantidad_cocteles
    FROM acontecimientos_con_coctel
    GROUP BY lugar, fuente, canal_nombre
    ORDER BY lugar, fuente, cantidad_cocteles DESC;
    """
    
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en contar_canales_radio_tv_con_coctel: {e}")
        return pd.DataFrame()


def contar_paginas_redes_con_coctel(fecha_inicio: str, fecha_fin: str, ids_lugares: List[int]) -> pd.DataFrame:
    """
    Cuenta PÁGINAS ÚNICAS de Facebook que generaron al menos un cóctel
    IMPORTANTE: Para Redes, cada acontecimiento puede tener múltiples posts de diferentes páginas
    Desagregamos por nombre_facebook_page para contar páginas únicas correctamente
    
    Returns:
        DataFrame con columnas: lugar, fuente, canal_nombre, cantidad_cocteles
    """
    if not ids_lugares:
        return pd.DataFrame()
    
    placeholders = ','.join(['%s'] * len(ids_lugares))
    
    query = f"""
    WITH acontecimientos_redes AS (
        SELECT DISTINCT
            a.id as acontecimiento_id,
            l.nombre as lugar,
            fbp.nombre as canal_nombre,
            'Redes' as fuente
        FROM acontecimientos a
        INNER JOIN lugares l ON a.id_lugar = l.id
        INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
        INNER JOIN facebook_posts fp ON afp.id_facebook_post = fp.id
        INNER JOIN facebook_pages fbp ON fp.id_facebook_page = fbp.id
        WHERE a.id_lugar IN ({placeholders})
            AND a.id_nota IS NOT NULL  -- Solo con cóctel
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            AND fbp.nombre IS NOT NULL
            AND fbp.nombre != ''
    )
    SELECT 
        lugar,
        fuente,
        canal_nombre,
        COUNT(*) as cantidad_cocteles
    FROM acontecimientos_redes
    GROUP BY lugar, fuente, canal_nombre
    ORDER BY lugar, fuente, cantidad_cocteles DESC;
    """
    
    params = ids_lugares + [fecha_inicio, fecha_fin]
    
    try:
        resultado = ejecutar_query(query, params=params)
        return resultado if resultado is not None else pd.DataFrame()
    except Exception as e:
        print(f"Error en contar_paginas_redes_con_coctel: {e}")
        return pd.DataFrame()


def data_section_12_medios_generan_coctel_sql(fecha_inicio: str, fecha_fin: str, lugares: List[str]) -> tuple:
    """
    Función principal para Sección 12: Medios que generan cóctel
    
    Devuelve TRES DataFrames:
    1. tabla_resumen: Conteo de medios únicos por lugar y fuente (para tabla pivot)
    2. tabla_desagregada: Detalle de cada canal/página con su cantidad de cocteles
    3. datos_grafico: Datos en formato largo para el gráfico de barras apiladas
    
    Args:
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'
        lugares: Lista de nombres de lugares ['Lima', 'Arequipa', ...]
    
    Returns:
        tuple: (tabla_resumen, tabla_desagregada, datos_grafico)
    """
    
    print(f"DEBUG Sección 12:")
    print(f"  fecha_inicio: {fecha_inicio}")
    print(f"  fecha_fin: {fecha_fin}")
    print(f"  lugares: {lugares}")
    
    # Convertir nombres de lugares a IDs
    ids_lugares = obtener_ids_lugares(lugares)
    print(f"DEBUG: IDs lugares obtenidos: {ids_lugares}")
    
    if not ids_lugares:
        print(f"ERROR: No se encontraron IDs para los lugares: {lugares}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    try:
        # 1. Obtener datos desagregados de Radio/TV
        print(f"🔍 Consultando canales de Radio/TV...")
        resultado_radio_tv = contar_canales_radio_tv_con_coctel(fecha_inicio, fecha_fin, ids_lugares)
        print(f"📻📺 Radio/TV: {len(resultado_radio_tv)} canales encontrados")
        
        # 2. Obtener datos desagregados de Redes
        print(f"🔍 Consultando páginas de Redes...")
        resultado_redes = contar_paginas_redes_con_coctel(fecha_inicio, fecha_fin, ids_lugares)
        print(f"📱 Redes: {len(resultado_redes)} páginas encontradas")
        
        # 3. Combinar resultados desagregados
        tabla_desagregada = pd.DataFrame()
        
        if not resultado_radio_tv.empty:
            tabla_desagregada = pd.concat([tabla_desagregada, resultado_radio_tv], ignore_index=True)
        
        if not resultado_redes.empty:
            tabla_desagregada = pd.concat([tabla_desagregada, resultado_redes], ignore_index=True)
        
        if tabla_desagregada.empty:
            print("⚠️ No se encontraron datos")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        # Renombrar columnas para mejor presentación
        tabla_desagregada = tabla_desagregada.rename(columns={
            'canal_nombre': 'Canal/Página',
            'cantidad_cocteles': 'Cantidad de Cocteles'
        })
        
        print(f"✅ Total de medios encontrados: {len(tabla_desagregada)}")
        
        # 4. Crear tabla resumen: contar MEDIOS ÚNICOS por lugar y fuente
        tabla_resumen = (
            tabla_desagregada
            .groupby(['lugar', 'fuente'])['Canal/Página']
            .nunique()
            .reset_index(name='conteo_medios')
        )
        
        # Convertir a formato pivot para mostrar como tabla
        tabla_resumen_pivot = tabla_resumen.pivot(
            index='lugar',
            columns='fuente',
            values='conteo_medios'
        ).fillna(0).astype(int).reset_index()
        
        # Asegurar que tengamos las columnas Radio, TV, Redes (aunque estén en 0)
        for col in ['Radio', 'TV', 'Redes']:
            if col not in tabla_resumen_pivot.columns:
                tabla_resumen_pivot[col] = 0
        
        # Reordenar columnas
        columnas_orden = ['lugar', 'Radio', 'TV', 'Redes']
        tabla_resumen_pivot = tabla_resumen_pivot[[c for c in columnas_orden if c in tabla_resumen_pivot.columns]]
        
        print(f"📊 Tabla resumen creada: {tabla_resumen_pivot.shape}")
        
        # 5. Crear datos para gráfico (formato largo)
        datos_grafico = tabla_resumen.rename(columns={'conteo_medios': 'Cantidad'})
        
        print(f"📈 Datos para gráfico creados: {datos_grafico.shape}")
        
        return tabla_resumen_pivot, tabla_desagregada, datos_grafico
        
    except Exception as e:
        print(f"❌ Error en data_section_12_medios_generan_coctel_sql: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


