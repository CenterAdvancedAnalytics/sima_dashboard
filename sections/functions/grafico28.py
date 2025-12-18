import pandas as pd
from .grafico27 import ejecutar_query

def obtener_data_grafico28():
    """
    Recupera la data para el Gráfico 28 y retorna dos DataFrames pivoteados:
    1. df_con: Usuarios y sus notas CON coctel (id_nota NOT NULL)
    2. df_sin: Usuarios y sus notas SIN coctel (id_nota NULL)
    
    Cada fila representa una combinación única de Usuario + Región.
    """
    
    # Modificamos la query para agrupar por usuario Y región (sin concatenar)
    query = """
    SELECT 
        TO_CHAR(a.fecha_registro, 'YYYY-MM') as mes_sort,
        u.nombre as nombre_usuario,
        COALESCE(l.nombre, 'Sin Región') as region,
        SUM(CASE WHEN a.id_nota IS NOT NULL THEN 1 ELSE 0 END) as cantidad_con_coctel,
        SUM(CASE WHEN a.id_nota IS NULL THEN 1 ELSE 0 END) as cantidad_sin_coctel
    FROM acontecimientos a
    JOIN usuarios u ON a.id_usuario_registro = u.id
    LEFT JOIN lugares l ON a.id_lugar = l.id
    WHERE 
        a.fecha_registro >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months'
    GROUP BY 
        TO_CHAR(a.fecha_registro, 'YYYY-MM'),
        u.nombre,
        l.nombre
    ORDER BY 
        mes_sort DESC, 
        u.nombre ASC,
        region ASC;
    """
    
    print("DEBUG grafico28: Ejecutando consulta desagregada por región...")
    
    try:
        df = ejecutar_query(query)
        
        if df is None or df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Diccionario para formatear columnas de fecha
        meses_es = {
            '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
            '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
            '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
        }

        def procesar_pivot(df_raw, col_valor):
            # Filtramos solo registros que tengan valor > 0 en esa categoría
            df_filtrado = df_raw[df_raw[col_valor] > 0].copy()
            
            if df_filtrado.empty:
                return pd.DataFrame()

            # Pivotear: Usuarios y Región en filas, Meses en columnas
            df_pivot = df_filtrado.pivot_table(
                index=['nombre_usuario', 'region'], 
                columns='mes_sort', 
                values=col_valor, 
                aggfunc='sum',
                fill_value=0
            )

            # Renombrar columnas de fecha (2025-01 -> Ene-25)
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

            # Aplanar índice y renombrar columnas fijas
            df_final = df_pivot.reset_index()
            df_final.rename(columns={
                'nombre_usuario': 'Nombre del usuario',
                'region': 'Región'
            }, inplace=True)

            return df_final

        # Generar los dos DataFrames
        df_con = procesar_pivot(df, 'cantidad_con_coctel')
        df_sin = procesar_pivot(df, 'cantidad_sin_coctel')

        return df_con, df_sin

    except Exception as e:
        print(f"❌ Error en grafico28: {e}")
        return pd.DataFrame(), pd.DataFrame()