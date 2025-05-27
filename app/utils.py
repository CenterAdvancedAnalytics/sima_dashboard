import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.elements import TextClause
from queries import coctel_queries, user_queries

def _get_engine():
    """
    Crea un SQLAlchemy Engine leyendo las credenciales de las variables de entorno.
    """
    host     = os.getenv("DB_HOST")
    port     = os.getenv("DB_PORT", "5432")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    dbname   = os.getenv("DB_NAME")
    sslmode  = os.getenv("DB_SSLMODE", "require")

    # DEBUG: mostrar en logs qué variables está recibiendo
    print(f"[DEBUG _get_engine] DB_HOST={host!r} DB_PORT={port!r} DB_USER={user!r} "
          f"DB_NAME={dbname!r} DB_SSLMODE={sslmode!r}")
    
    # URI de conexión PostgreSQL
    dsn = (
        f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        f"?sslmode={sslmode}"
    )
    # Creamos el engine; SQLAlchemy se encargará de abrir/cerrar conexiones
    return create_engine(dsn)

def cargar_datos(query: TextClause) -> pd.DataFrame:
    """
    Ejecuta el query (SQLAlchemy TextClause) y devuelve un DataFrame.
    Ahora usa un SQLAlchemy Engine para evitar el UserWarning de pandas.
    """
    engine = _get_engine()
    # pd.read_sql_query acepta como 'con' un engine de SQLAlchemy
    df = pd.read_sql_query(str(query), con=engine)
    return df

# Mapa de categorías a sus diccionarios de queries
ALL_QUERIES = {
    "cocteles": coctel_queries.queries,
    "usuarios": user_queries.queries,
}

def get_query(category: str, table_name: str, mode: str = "read") -> pd.DataFrame:
    """
    category   : 'cocteles' o 'usuarios'
    table_name : nombre de la consulta en tu módulo queries (por ej. 'coctel_completo')
    mode       : generalmente 'read'
    """
    qdict = ALL_QUERIES.get(category)
    if not qdict:
        raise ValueError(f"Categoría '{category}' no encontrada.")
    entry = qdict.get(table_name)
    if not entry:
        raise ValueError(f"Query '{table_name}' no encontrado en '{category}'.")
    query = entry.get(mode)
    if query is None:
        raise ValueError(f"Modo '{mode}' no definido para '{table_name}'.")
    return cargar_datos(query)
