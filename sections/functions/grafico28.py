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

# --- LÓGICA DEL GRÁFICO 28 (CORREGIDA - CON DISTINCT PARA IGUALAR A SN.PY) ---

def obtener_data_grafico28(id_fuente=None):
    """
    Recupera la data para el Gráfico 28.
    Usa COUNT(DISTINCT a.id) para eliminar duplicados y coincidir con la lógica de sn.py (drop_duplicates).
    """
    
    # Definimos las partes comunes de la consulta
    select_cols = """
        TO_CHAR((a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima'), 'YYYY-MM') as mes_sort,
        TRIM(CONCAT(u.nombre, ' ', u.apellido)) as nombre_usuario,
        COALESCE(l.nombre, 'Sin Región') as region,
    """
    
    where_time = """
        WHERE (a.fecha_registro AT TIME ZONE 'UTC' AT TIME ZONE 'America/Lima') >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months'
    """

    query = ""

    # --- ESTRATEGIA: Consultas separadas + COUNT(DISTINCT) ---
    
    # CASO 1: Radio (1) o TV (2)
    if id_fuente == 1 or id_fuente == 2:
        query = f"""
        SELECT 
            {select_cols}
            p.nombre as nombre_programa,
            -- AQUI EL CAMBIO: Usamos DISTINCT a.id para contar eventos únicos por programa
            COUNT(DISTINCT CASE WHEN a.id_nota IS NOT NULL THEN a.id END) as cantidad_con_coctel,
            COUNT(DISTINCT CASE WHEN a.id_nota IS NULL THEN a.id END) as cantidad_sin_coctel
        FROM acontecimiento_programa ap
        JOIN acontecimientos a ON ap.id_acontecimiento = a.id
        LEFT JOIN usuarios u ON a.id_usuario_registro = u.id
        LEFT JOIN lugares l ON a.id_lugar = l.id
        JOIN programas p ON ap.id_programa = p.id
        {where_time}
        AND p.id_fuente = {id_fuente}
        GROUP BY 1, u.nombre, u.apellido, l.nombre, p.nombre
        ORDER BY mes_sort DESC, nombre_usuario ASC, region ASC;
        """

    # CASO 2: Redes Sociales (3)
    elif id_fuente == 3:
        query = f"""
        SELECT 
            {select_cols}
            fpage.nombre as nombre_programa,
            -- AQUI EL CAMBIO: Usamos DISTINCT a.id
            COUNT(DISTINCT CASE WHEN a.id_nota IS NOT NULL THEN a.id END) as cantidad_con_coctel,
            COUNT(DISTINCT CASE WHEN a.id_nota IS NULL THEN a.id END) as cantidad_sin_coctel
        FROM acontecimiento_facebook_post afp
        JOIN acontecimientos a ON afp.id_acontecimiento = a.id
        LEFT JOIN usuarios u ON a.id_usuario_registro = u.id
        LEFT JOIN lugares l ON a.id_lugar = l.id
        JOIN facebook_posts fp ON afp.id_facebook_post = fp.id
        JOIN facebook_pages fpage ON fp.id_facebook_page = fpage.id
        {where_time}
        GROUP BY 1, u.nombre, u.apellido, l.nombre, fpage.nombre
        ORDER BY mes_sort DESC, nombre_usuario ASC, region ASC;
        """

    # CASO 3: Todos (None)
    else:
        query = f"""
        (
            SELECT 
                {select_cols}
                p.nombre as nombre_programa,
                COUNT(DISTINCT CASE WHEN a.id_nota IS NOT NULL THEN a.id END) as cantidad_con_coctel,
                COUNT(DISTINCT CASE WHEN a.id_nota IS NULL THEN a.id END) as cantidad_sin_coctel
            FROM acontecimiento_programa ap
            JOIN acontecimientos a ON ap.id_acontecimiento = a.id
            LEFT JOIN usuarios u ON a.id_usuario_registro = u.id
            LEFT JOIN lugares l ON a.id_lugar = l.id
            JOIN programas p ON ap.id_programa = p.id
            {where_time}
            GROUP BY 1, u.nombre, u.apellido, l.nombre, p.nombre
        )
        UNION ALL
        (
            SELECT 
                {select_cols}
                fpage.nombre as nombre_programa,
                COUNT(DISTINCT CASE WHEN a.id_nota IS NOT NULL THEN a.id END) as cantidad_con_coctel,
                COUNT(DISTINCT CASE WHEN a.id_nota IS NULL THEN a.id END) as cantidad_sin_coctel
            FROM acontecimiento_facebook_post afp
            JOIN acontecimientos a ON afp.id_acontecimiento = a.id
            LEFT JOIN usuarios u ON a.id_usuario_registro = u.id
            LEFT JOIN lugares l ON a.id_lugar = l.id
            JOIN facebook_posts fp ON afp.id_facebook_post = fp.id
            JOIN facebook_pages fpage ON fp.id_facebook_page = fpage.id
            {where_time}
            GROUP BY 1, u.nombre, u.apellido, l.nombre, fpage.nombre
        )
        ORDER BY mes_sort DESC, nombre_usuario ASC, region ASC;
        """

    print(f"DEBUG grafico28: Ejecutando consulta CON DISTINCT (id_fuente={id_fuente})...")
    
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