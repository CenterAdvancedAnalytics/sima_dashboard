import psycopg2
import pandas as pd
import numpy as np
from typing import Optional, List, Any, Dict
from sqlalchemy import create_engine, text
import warnings
import os
from dotenv import load_dotenv


load_dotenv()

# Configuración de la base de datos usando variables de entorno
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

def convert_numpy_params(params: Optional[List[Any]]) -> Optional[List[Any]]:
    """
    Convierte parámetros numpy a tipos nativos de Python para evitar errores de PostgreSQL
    """
    if not params:
        return params
    
    converted_params = []
    for param in params:
        if isinstance(param, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            converted_params.append(int(param))
        elif isinstance(param, (np.floating, np.float64, np.float32, np.float16)):
            converted_params.append(float(param))
        elif isinstance(param, np.bool_):
            converted_params.append(bool(param))
        elif isinstance(param, np.ndarray):
            converted_params.append(param.tolist())
        elif hasattr(param, 'item'):  # Para otros tipos numpy
            converted_params.append(param.item())
        else:
            converted_params.append(param)
    
    return converted_params

def convert_numpy_dict_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Convierte parámetros numpy en diccionario a tipos nativos de Python
    """
    if not params:
        return params
    
    converted_params = {}
    for key, param in params.items():
        if isinstance(param, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            converted_params[key] = int(param)
        elif isinstance(param, (np.floating, np.float64, np.float32, np.float16)):
            converted_params[key] = float(param)
        elif isinstance(param, np.bool_):
            converted_params[key] = bool(param)
        elif isinstance(param, np.ndarray):
            converted_params[key] = param.tolist()
        elif hasattr(param, 'item'):  # Para otros tipos numpy
            converted_params[key] = param.item()
        else:
            converted_params[key] = param
    
    return converted_params

def create_sqlalchemy_engine():
    """
    Crea engine de SQLAlchemy para uso con pandas (recomendado)
    """
    DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?sslmode={DB_CONFIG['sslmode']}"
    return create_engine(DATABASE_URL)

def ejecutar_query(query: str, params: Optional[List[Any]] = None, return_dataframe: bool = True, use_sqlalchemy: bool = True):
    """
    Función madre para ejecutar cualquier query en PostgreSQL
    
    Args:
        query (str): La consulta SQL a ejecutar
        params (list, optional): Parámetros para la consulta (para evitar SQL injection)
        return_dataframe (bool): Si True retorna DataFrame, si False retorna lista de tuplas
        use_sqlalchemy (bool): Si True usa SQLAlchemy (recomendado para pandas), si False usa psycopg2
    
    Returns:
        pandas.DataFrame o list: Resultados de la consulta
        None: Si hay error
    """
    
    # Convertir parámetros numpy a tipos nativos
    params = convert_numpy_params(params)
    
    # Debug: mostrar tipos de parámetros si hay alguno
    if params:
        print("Parámetros convertidos:")
        for i, param in enumerate(params):
            print(f"  Param {i}: {param} (tipo: {type(param)})")
    
    if return_dataframe and use_sqlalchemy:
        # Método recomendado: SQLAlchemy + pandas
        engine = None
        try:
            engine = create_sqlalchemy_engine()
            
            # Suprimir warning de pandas sobre SQLAlchemy
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                
                if params:
                    # Para SQLAlchemy, convertir query con %s a parámetros posicionales
                    # SQLAlchemy con pandas.read_sql_query espera parámetros como lista
                    resultado = pd.read_sql_query(query, engine, params=params)
                else:
                    resultado = pd.read_sql_query(query, engine)
                
            return resultado
            
        except Exception as e:
            print(f"Error con SQLAlchemy: {e}")
            print(f"Query: {query}")
            print(f"Params: {params}")
            return None
        finally:
            if engine:
                engine.dispose()
    
    else:
        # Método alternativo: psycopg2 directo
        conn = None
        try:
            # Conectar a la base de datos
            conn = psycopg2.connect(**DB_CONFIG)
            
            if return_dataframe:
                # Usar pandas con psycopg2 (puede mostrar warning)
                if params:
                    resultado = pd.read_sql_query(query, conn, params=params)
                else:
                    resultado = pd.read_sql_query(query, conn)
                return resultado
            else:
                # Usar cursor para operaciones más básicas
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Para SELECT retornar resultados, para INSERT/UPDATE/DELETE retornar None
                if query.strip().upper().startswith('SELECT'):
                    resultado = cursor.fetchall()
                    return resultado
                else:
                    # Para INSERT, UPDATE, DELETE
                    conn.commit()
                    return cursor.rowcount  # Número de filas afectadas
                    
        except psycopg2.Error as e:
            print(f"Error de PostgreSQL: {e}")
            print(f"Query: {query}")
            print(f"Params: {params}")
            if conn:
                conn.rollback()
            return None
        except Exception as e:
            print(f"Error general: {e}")
            print(f"Query: {query}")
            print(f"Params: {params}")
            return None
        finally:
            if conn:
                conn.close()

def ejecutar_query_con_nombres(query: str, params: Optional[Dict[str, Any]] = None):
    """
    Función para ejecutar queries con parámetros nombrados (como en chart_data_processor)
    Usa SQLAlchemy con text() para manejo correcto de parámetros nombrados
    """
    # Convertir parámetros numpy a tipos nativos
    params = convert_numpy_dict_params(params)
    
    # Debug: mostrar tipos de parámetros si hay alguno
    if params:
        print("Parámetros nombrados convertidos:")
        for key, param in params.items():
            print(f"  {key}: {param} (tipo: {type(param)})")
    
    engine = None
    try:
        engine = create_sqlalchemy_engine()
        
        with engine.connect() as conn:
            # Usar text() para queries con parámetros nombrados
            if params:
                result = conn.execute(text(query), params)
            else:
                result = conn.execute(text(query))
            
            # Convertir a DataFrame
            columns = result.keys()
            data = result.fetchall()
            
            if data:
                df = pd.DataFrame(data, columns=columns)
                return df
            else:
                return pd.DataFrame(columns=columns)
                
    except Exception as e:
        print(f"Error con query nombrada: {e}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        return None
    finally:
        if engine:
            engine.dispose()

