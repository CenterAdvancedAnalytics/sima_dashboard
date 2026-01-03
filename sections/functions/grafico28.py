import pandas as pd
import psycopg2
import os
from typing import Optional, List, Any

# --- FUNCIÓN DE CONEXIÓN LOCAL ---
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

# --- LÓGICA DEL GRÁFICO 28 ---

def obtener_data_grafico28(id_fuente=None):
    """
    Recupera la data para el Gráfico 28.
    Argumentos:
        id_fuente (int, opcional): ID de la fuente para filtrar (1=Radio, 2=TV, 3=Redes).
    """
    
    # Lógica de filtrado en el WHERE
    filtro_fuente = ""
    if id_fuente == 1: # Radio
        filtro_fuente = "AND p.id_fuente = 1"
    elif id_fuente == 2: # TV
        filtro_fuente = "AND p.id_fuente = 2"
    elif id_fuente == 3: # Redes
        # Para redes, buscamos que exista enlace con facebook_page
        filtro_fuente = "AND fpage.id IS NOT NULL"
        
    query = f"""
    SELECT 
        TO_CHAR(a.fecha_registro, 'YYYY-MM') as mes_sort,
        TRIM(CONCAT(u.nombre, ' ', u.apellido)) as nombre_usuario,
        COALESCE(l.nombre, 'Sin Región') as region,
        CASE 
            WHEN p.nombre IS NOT NULL THEN p.nombre
            WHEN fpage.nombre IS NOT NULL THEN fpage.nombre
            ELSE 'Sin Programa/Medio'
        END as nombre_programa,
        SUM(CASE WHEN a.id_nota IS NOT NULL THEN 1 ELSE 0 END) as cantidad_con_coctel,
        SUM(CASE WHEN a.id_nota IS NULL THEN 1 ELSE 0 END) as cantidad_sin_coctel
    FROM acontecimientos a
    JOIN usuarios u ON a.id_usuario_registro = u.id
    LEFT JOIN lugares l ON a.id_lugar = l.id
    
    -- Joins para Radio y TV
    LEFT JOIN acontecimiento_programa ap ON a.id = ap.id_acontecimiento
    LEFT JOIN programas p ON ap.id_programa = p.id
    
    -- Joins para Redes Sociales
    LEFT JOIN acontecimiento_facebook_post afp ON a.id = afp.id_acontecimiento
    LEFT JOIN facebook_posts fp ON afp.id_facebook_post = fp.id
    LEFT JOIN facebook_pages fpage ON fp.id_facebook_page = fpage.id
    
    WHERE 
        a.fecha_registro >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months'
        {filtro_fuente}
        
    GROUP BY 
        TO_CHAR(a.fecha_registro, 'YYYY-MM'),
        u.nombre,
        u.apellido,
        l.nombre,
        p.nombre,
        fpage.nombre
    ORDER BY 
        mes_sort DESC, 
        nombre_usuario ASC,
        region ASC;
    """
    
    print(f"DEBUG grafico28: Ejecutando consulta compleja (id_fuente={id_fuente})...")
    
    try:
        df = ejecutar_query(query)
        
        if df is None or df.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # Calcular TOTAL
        df['cantidad_total'] = df['cantidad_con_coctel'] + df['cantidad_sin_coctel']

        meses_es = {
            '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
            '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
            '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
        }

        def procesar_pivot(df_raw, col_valor):
            df_filtrado = df_raw[df_raw[col_valor] > 0].copy()
            
            if df_filtrado.empty:
                return pd.DataFrame()

            # Ahora incluimos 'nombre_programa' en el índice del pivot
            df_pivot = df_filtrado.pivot_table(
                index=['nombre_usuario', 'region', 'nombre_programa'], 
                columns='mes_sort', 
                values=col_valor, 
                aggfunc='sum',
                fill_value=0
            )

            nuevas_columnas = []
            for col in df_pivot.columns:
                try:
                    anio, mes = col.split('-')
                    nombre_mes = meses_es.get(mes, mes)
                    anio_corto = anio[-2:]
                    nuevas_columnas.append(f"{nombre_mes}-{anio_corto}")
                except:
                    nuevas_columnas.append(col)
            
            df_pivot.columns = nuevas_columnas

            df_final = df_pivot.reset_index()
            # Renombrar columnas para la vista final
            df_final.rename(columns={
                'nombre_usuario': 'Usuario',
                'region': 'Región',
                'nombre_programa': 'Programa/Medio'
            }, inplace=True)

            return df_final

        df_con = procesar_pivot(df, 'cantidad_con_coctel')
        df_sin = procesar_pivot(df, 'cantidad_sin_coctel')
        df_total = procesar_pivot(df, 'cantidad_total')

        return df_con, df_sin, df_total

    except Exception as e:
        print(f"❌ Error en grafico28: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()