import pandas as pd
import psycopg2
import os
from typing import Optional, List, Any, Tuple

def calcular_porcentajes_radio_tv(resultado_radio_tv):
    """
    Calcula los porcentajes de radio y TV con y sin cóctel (nota)
    
    Args:
        resultado_radio_tv: DataFrame con los datos de radio y TV
        
    Returns:
        dict: Diccionario con DataFrames por tipo de fuente (formato como redes sociales)
    """
    
    if resultado_radio_tv is None or resultado_radio_tv.empty:
        return {
            'RADIO': pd.DataFrame({'tipo_coctel': ['CON_COCTEL', 'SIN_COCTEL'], 'cantidad': [0, 0], 'porcentaje': [0.0, 0.0]}),
            'TV': pd.DataFrame({'tipo_coctel': ['CON_COCTEL', 'SIN_COCTEL'], 'cantidad': [0, 0], 'porcentaje': [0.0, 0.0]})
        }
    
    # Clasificar cada registro como CON_COCTEL o SIN_COCTEL
    resultado_radio_tv['tipo_coctel'] = resultado_radio_tv['id_nota'].apply(
        lambda x: 'CON_COCTEL' if pd.notna(x) else 'SIN_COCTEL'
    )
    
    # Inicializar el diccionario resultado
    resultado = {}
    
    # Procesar cada tipo de fuente (RADIO y TV)
    for tipo_fuente in ['RADIO', 'TV']:
        datos_fuente = resultado_radio_tv[resultado_radio_tv['tipo_fuente'] == tipo_fuente]
        
        if datos_fuente.empty:
            # Si no hay datos para este tipo de fuente
            resultado[tipo_fuente] = pd.DataFrame({
                'tipo_coctel': ['CON_COCTEL', 'SIN_COCTEL'], 
                'cantidad': [0, 0], 
                'porcentaje': [0.0, 0.0]
            })
        else:
            # Agrupar por tipo_coctel y contar
            agrupado = datos_fuente.groupby('tipo_coctel').size().reset_index(name='cantidad')
            
            # Calcular total
            total = agrupado['cantidad'].sum()
            
            # Calcular porcentajes
            agrupado['porcentaje'] = round(agrupado['cantidad'] * 100.0 / total, 2)
            
            # Asegurar que tenemos ambos tipos (CON_COCTEL y SIN_COCTEL)
            tipos_completos = pd.DataFrame({'tipo_coctel': ['CON_COCTEL', 'SIN_COCTEL']})
            resultado_completo = tipos_completos.merge(agrupado, on='tipo_coctel', how='left')
            resultado_completo['cantidad'] = resultado_completo['cantidad'].fillna(0).astype(int)
            resultado_completo['porcentaje'] = resultado_completo['porcentaje'].fillna(0.0)
            
            resultado[tipo_fuente] = resultado_completo
    
    return resultado

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

def data_section_sn_proporcion_simple_sql(f_inicio, f_final, lugar) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Versión que ejecuta todo en una sola query SQL y retorna los resultados por separado
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (resultado_radio, resultado_tv, resultado_redes_sociales)
    """
    lugar = obtener_id_lugar(lugar)
    
    query_radio_tv = """
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
        WHERE a.id_lugar = %s
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
            AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
        ORDER BY f.nombre, p.id, a.id;
        """
    
    query_redes_sociales = """
           WITH acontecimientos_redes AS (
             SELECT 
               a.id,
               afp.id_facebook_post,
               a.acontecimiento,
               CASE 
                 WHEN n.id IS NOT NULL THEN 'CON_COCTEL'
                 ELSE 'SIN_COCTEL'
               END as tipo_coctel
             FROM acontecimientos a
             INNER JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
             LEFT JOIN notas n ON a.id_nota = n.id
             WHERE a.id_lugar = %s
               AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date >= %s::date
               AND (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima')::date <= %s::date
           )
           SELECT 
             tipo_coctel,
             COUNT(*) as cantidad,
             ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje
           FROM acontecimientos_redes
           GROUP BY tipo_coctel
           ORDER BY tipo_coctel;
           """
    
    try:
        # Radio + TV
        resultado_radio_tv = ejecutar_query(query_radio_tv, params=[lugar, f_inicio, f_final])
        resultado_radio_tv = resultado_radio_tv.drop_duplicates(subset=['programa_nombre', 'acontecimiento_id'])
        
        # Redes sociales
        resultado_redes_sociales = ejecutar_query(query_redes_sociales, params=[lugar, f_inicio, f_final])
        
        # Procesar radio y TV
        porcentajes_radio_tv = calcular_porcentajes_radio_tv(resultado_radio_tv)
        
        # Separar los resultados
        resultado_radio = porcentajes_radio_tv['RADIO']
        resultado_tv = porcentajes_radio_tv['TV']
        
        # Debug prints (puedes comentarlos en producción)
        print(f"Resultado RADIO: {resultado_radio}")
        print(f"Resultado TV: {resultado_tv}")
        print(f"Resultado redes sociales: {resultado_redes_sociales}")
        
        if resultado_redes_sociales is None or resultado_redes_sociales.empty:
            # Crear DataFrame vacío con estructura esperada para redes sociales
            resultado_redes_sociales = pd.DataFrame({
                'tipo_coctel': ['CON_COCTEL', 'SIN_COCTEL'], 
                'cantidad': [0, 0], 
                'porcentaje': [0.0, 0.0]
            })
        
        return resultado_radio, resultado_tv, resultado_redes_sociales
        
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        # Retornar DataFrames vacíos en caso de error
        df_vacio = pd.DataFrame({'tipo_coctel': ['CON_COCTEL', 'SIN_COCTEL'], 'cantidad': [0, 0], 'porcentaje': [0.0, 0.0]})
        return df_vacio, df_vacio, df_vacio

# Ejemplo de uso:
# resultado_radio, resultado_tv, resultado_redes_sociales = data_section_sn_proporcion_simple_sql('2024-01-01', '2024-12-31', 'Lima')