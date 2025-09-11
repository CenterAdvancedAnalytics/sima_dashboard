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

def obtener_id_lugar(nombre_lugar):
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

def calcular_porcentajes_radio_tv_combinado(resultado_radio_tv):
    """
    Calcula los porcentajes de radio y TV COMBINADOS con y sin cóctel (nota)
    Adapta la función original para trabajar con múltiples lugares
    """
    
    if resultado_radio_tv is None or resultado_radio_tv.empty:
        return pd.DataFrame({'tipo_coctel': ['SIN_COCTEL', 'CON_COCTEL'], 'cantidad': [0, 0], 'porcentaje': [0.0, 0.0]})
    
    # Clasificar cada registro como CON_COCTEL o SIN_COCTEL
    resultado_radio_tv['tipo_coctel'] = resultado_radio_tv['id_nota'].apply(
        lambda x: 'CON_COCTEL' if pd.notna(x) else 'SIN_COCTEL'
    )
    
    # Agrupar por tipo_coctel y contar (SIN separar por fuente, todo junto)
    agrupado = resultado_radio_tv.groupby('tipo_coctel').size().reset_index(name='cantidad')
    
    # Calcular total
    total = agrupado['cantidad'].sum()
    
    # Calcular porcentajes
    agrupado['porcentaje'] = round(agrupado['cantidad'] * 100.0 / total, 2) if total > 0 else 0
    
    # Asegurar que tenemos ambos tipos (CON_COCTEL y SIN_COCTEL)
    tipos_completos = pd.DataFrame({'tipo_coctel': ['SIN_COCTEL', 'CON_COCTEL']})
    resultado_completo = tipos_completos.merge(agrupado, on='tipo_coctel', how='left')
    resultado_completo['cantidad'] = resultado_completo['cantidad'].fillna(0).astype(int)
    resultado_completo['porcentaje'] = resultado_completo['porcentaje'].fillna(0.0)
    
    return resultado_completo

def data_section_1_proporcion_combinada_sql(f_inicio, f_final, lugares_lista, fuentes_lista):
    """
    Versión que ejecuta todo para múltiples lugares y retorna resultado COMBINADO
    Mantiene la misma lógica que la función original pero para múltiples lugares
    Maneja Radio/TV y Redes por separado como en sn.py original
    """
    
    # Obtener IDs de lugares
    lugar_ids = []
    for lugar_nombre in lugares_lista:
        lugar_id = obtener_id_lugar(lugar_nombre)
        if lugar_id:
            lugar_ids.append(lugar_id)
    
    if not lugar_ids:
        return pd.DataFrame({'tipo_coctel': ['SIN_COCTEL', 'CON_COCTEL'], 'cantidad': [0, 0], 'porcentaje': [0.0, 0.0]})
    
    lugares_ids_str = ','.join(map(str, lugar_ids))
    resultado_combinado = pd.DataFrame()
    
    # RADIO/TV - usando acontecimiento_programa
    if any(f in ['Radio', 'TV'] for f in fuentes_lista):
        fuentes_radio_tv = [f for f in fuentes_lista if f in ['Radio', 'TV']]
        fuente_map = {"Radio": 1, "TV": 2}
        fuentes_ids = [fuente_map[f] for f in fuentes_radio_tv]
        fuentes_ids_str = ','.join(map(str, fuentes_ids))
        
        query_radio_tv = f"""
            SELECT 
                f.nombre as tipo_fuente,
                p.id as programa_id,
                p.nombre as programa_nombre,
                a.id_lugar as lugar,
                a.id as acontecimiento_id,
                a.id_nota
            FROM acontecimiento_programa ap
            JOIN programas p ON ap.id_programa = p.id
            JOIN fuentes f ON p.id_fuente = f.id
            JOIN acontecimientos a ON ap.id_acontecimiento = a.id
            WHERE a.id_lugar IN ({lugares_ids_str})
                AND p.id_fuente IN ({fuentes_ids_str})
                AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
                AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            ORDER BY f.nombre, p.id, a.id;
            """
        
        resultado_radio_tv = ejecutar_query(query_radio_tv, params=[f_inicio, f_final])
        if resultado_radio_tv is not None and not resultado_radio_tv.empty:
            resultado_radio_tv = resultado_radio_tv.drop_duplicates(subset=['programa_nombre', 'acontecimiento_id'])
            resultado_combinado = pd.concat([resultado_combinado, resultado_radio_tv], ignore_index=True)
    
    # REDES - usando acontecimiento_facebook_post
    if 'Redes' in fuentes_lista:
        query_redes = f"""
            SELECT 
                'REDES' as tipo_fuente,
                'Facebook' as programa_nombre,
                a.id_lugar as lugar,
                a.id as acontecimiento_id,
                a.id_nota
            FROM acontecimientos a
            INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
            WHERE a.id_lugar IN ({lugares_ids_str})
                AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
                AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
            ORDER BY a.id;
            """
        
        resultado_redes = ejecutar_query(query_redes, params=[f_inicio, f_final])
        if resultado_redes is not None and not resultado_redes.empty:
            resultado_combinado = pd.concat([resultado_combinado, resultado_redes], ignore_index=True)
    
    if resultado_combinado.empty:
        return pd.DataFrame({'tipo_coctel': ['SIN_COCTEL', 'CON_COCTEL'], 'cantidad': [0, 0], 'porcentaje': [0.0, 0.0]})
    
    return calcular_porcentajes_radio_tv_combinado(resultado_combinado)
def convertir_a_formato_streamlit(resultado_df):
    """
    Convierte el resultado al formato que espera Streamlit
    Mapea SIN_COCTEL -> Otras Fuentes, CON_COCTEL -> Coctel Noticias
    """
    if resultado_df.empty:
        return pd.DataFrame()
    
    # Mapear los nombres
    resultado_df = resultado_df.copy()
    resultado_df['Fuente'] = resultado_df['tipo_coctel'].map({
        'SIN_COCTEL': 'Otras Fuentes',
        'CON_COCTEL': 'Coctel Noticias'
    })
    
    # Renombrar columnas y formatear
    resultado_final = resultado_df[['Fuente', 'cantidad', 'porcentaje']].copy()
    resultado_final = resultado_final.rename(columns={'cantidad': 'Cantidad'})
    resultado_final['Proporción'] = resultado_final['porcentaje'].apply(lambda x: f"{x:.0f}%")
    resultado_final = resultado_final[['Fuente', 'Cantidad', 'Proporción']]
    
    return resultado_final

# Ejemplo de uso:
# resultado = data_section_1_proporcion_combinada_sql('2025-09-10', '2025-09-10', ['Lima', 'Arequipa'], ['Radio', 'TV'])
# df_para_streamlit = convertir_a_formato_streamlit(resultado)