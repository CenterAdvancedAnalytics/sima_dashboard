import pandas as pd
from .grafico27 import ejecutar_query

def obtener_data_grafico28():
    """
    Recupera y separa la data para el Gráfico 28.
    Retorna dos DataFrames:
    1. df_con_coctel: Usuarios y sus notas CON coctel (id_nota NOT NULL)
    2. df_sin_coctel: Usuarios y sus notas SIN coctel (id_nota NULL)
    """
    
    query = """
    SELECT 
        TO_CHAR(a.fecha_registro, 'YYYY-MM') as mes,
        u.nombre as nombre_usuario,
        COALESCE(STRING_AGG(DISTINCT l.nombre, ', '), 'Sin Región') as regiones,
        SUM(CASE WHEN a.id_nota IS NOT NULL THEN 1 ELSE 0 END) as cantidad_con_coctel,
        SUM(CASE WHEN a.id_nota IS NULL THEN 1 ELSE 0 END) as cantidad_sin_coctel
    FROM acontecimientos a
    JOIN usuarios u ON a.id_usuario_registro = u.id
    LEFT JOIN lugares l ON a.id_lugar = l.id
    WHERE 
        a.fecha_registro >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months'
    GROUP BY 
        TO_CHAR(a.fecha_registro, 'YYYY-MM'),
        u.nombre
    ORDER BY 
        mes DESC, 
        u.nombre ASC;
    """
    
    print("DEBUG grafico28: Ejecutando consulta desglosada (Con/Sin Coctel)...")
    
    try:
        df = ejecutar_query(query)
        
        if df is None or df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Convertir columnas numéricas
        df['cantidad_con_coctel'] = df['cantidad_con_coctel'].astype(int)
        df['cantidad_sin_coctel'] = df['cantidad_sin_coctel'].astype(int)

        # --- SEPARAR EN 2 CUADROS ---

        # Cuadro 1: Con Coctel (Filtramos donde haya al menos 1)
        df_con = df[df['cantidad_con_coctel'] > 0].copy()
        df_con = df_con[['mes', 'nombre_usuario', 'regiones', 'cantidad_con_coctel']]
        df_con.columns = ['Mes', 'Usuario', 'Regiones', 'Cantidad Notas (Con Coctel)']

        # Cuadro 2: Sin Coctel (Filtramos donde haya al menos 1)
        df_sin = df[df['cantidad_sin_coctel'] > 0].copy()
        df_sin = df_sin[['mes', 'nombre_usuario', 'regiones', 'cantidad_sin_coctel']]
        df_sin.columns = ['Mes', 'Usuario', 'Regiones', 'Cantidad Notas (Sin Coctel)']

        return df_con, df_sin

    except Exception as e:
        print(f"❌ Error en grafico28: {e}")
        return pd.DataFrame(), pd.DataFrame()