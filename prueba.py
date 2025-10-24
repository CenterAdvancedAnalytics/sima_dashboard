# utils_debug.py

import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.elements import TextClause
import time

def test_database_connection():
    """
    Prueba la conexión a la base de datos y muestra información de debug
    """
    print("=" * 60)
    print("🔍 DIAGNÓSTICO DE CONEXIÓN A BASE DE DATOS")
    print("=" * 60)
    
    # Verificar variables de entorno
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER")
    password_present = "✓" if os.getenv("DB_PASSWORD") else "✗"
    dbname = os.getenv("DB_NAME")
    sslmode = os.getenv("DB_SSLMODE", "require")
    
    print(f"\n📋 Variables de Entorno:")
    print(f"   DB_HOST: {host}")
    print(f"   DB_PORT: {port}")
    print(f"   DB_USER: {user}")
    print(f"   DB_PASSWORD: {password_present} (presente)")
    print(f"   DB_NAME: {dbname}")
    print(f"   DB_SSLMODE: {sslmode}")
    
    # Validar que todas las variables estén presentes
    missing_vars = []
    if not host:
        missing_vars.append("DB_HOST")
    if not user:
        missing_vars.append("DB_USER")
    if not os.getenv("DB_PASSWORD"):
        missing_vars.append("DB_PASSWORD")
    if not dbname:
        missing_vars.append("DB_NAME")
    
    if missing_vars:
        print(f"\n❌ ERROR: Faltan las siguientes variables de entorno:")
        for var in missing_vars:
            print(f"   - {var}")
        return False
    
    # Intentar crear el engine
    print(f"\n🔌 Intentando conectar a PostgreSQL...")
    try:
        dsn = (
            f"postgresql://{user}:{os.getenv('DB_PASSWORD')}@{host}:{port}/{dbname}"
            f"?sslmode={sslmode}"
        )
        engine = create_engine(dsn, pool_pre_ping=True, connect_args={'connect_timeout': 10})
        
        # Probar la conexión
        with engine.connect() as conn:
            result = conn.execute("SELECT version();")
            version = result.fetchone()[0]
            print(f"✅ Conexión exitosa!")
            print(f"   PostgreSQL Version: {version[:50]}...")
            
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"❌ ERROR al conectar:")
        print(f"   {str(e)}")
        return False


def test_simple_query():
    """
    Ejecuta una query simple para verificar que la base de datos responde
    """
    print("\n" + "=" * 60)
    print("🧪 PRUEBA DE QUERY SIMPLE")
    print("=" * 60)
    
    try:
        from queries import coctel_queries
        from utils import get_query
        
        print("\n⏱️  Ejecutando query 'ultima_fecha'...")
        start_time = time.time()
        
        df = get_query("cocteles", "ultima_fecha")
        
        elapsed = time.time() - start_time
        print(f"✅ Query ejecutada en {elapsed:.2f} segundos")
        print(f"   Resultado: {df.to_dict('records') if not df.empty else 'Sin datos'}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR en query simple:")
        print(f"   {str(e)}")
        return False


def test_coctel_completo_with_limit():
    """
    Prueba la query coctel_completo con LIMIT para ver si funciona con menos datos
    """
    print("\n" + "=" * 60)
    print("🧪 PRUEBA DE QUERY COCTEL_COMPLETO (con límite)")
    print("=" * 60)
    
    try:
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", "5432")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        dbname = os.getenv("DB_NAME")
        sslmode = os.getenv("DB_SSLMODE", "require")
        
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}"
        engine = create_engine(dsn, pool_pre_ping=True)
        
        # Query simplificada con LIMIT
        query = """
        SELECT
            a.id AS id,
            a.fecha_registro,
            a.acontecimiento,
            a.coctel,
            l.nombre AS lugar
        FROM
            acontecimientos a
        JOIN
            lugares l ON a.id_lugar = l.id
        WHERE
            a.fecha_registro >= NOW() - INTERVAL '3 months'
        LIMIT 10;
        """
        
        print("\n⏱️  Ejecutando query simplificada (10 registros)...")
        start_time = time.time()
        
        df = pd.read_sql_query(query, engine)
        
        elapsed = time.time() - start_time
        print(f"✅ Query ejecutada en {elapsed:.2f} segundos")
        print(f"   Registros obtenidos: {len(df)}")
        print(f"   Columnas: {list(df.columns)}")
        
        if not df.empty:
            print(f"\n   Primeros 3 registros:")
            print(df.head(3).to_string(index=False))
        
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"❌ ERROR en query coctel_completo:")
        print(f"   {str(e)}")
        import traceback
        print("\n📜 Traceback completo:")
        traceback.print_exc()
        return False


def run_full_diagnostic():
    """
    Ejecuta todos los diagnósticos
    """
    print("\n\n" + "=" * 60)
    print("🚀 INICIANDO DIAGNÓSTICO COMPLETO")
    print("=" * 60)
    
    # Paso 1: Verificar conexión
    if not test_database_connection():
        print("\n⚠️  No se pudo conectar a la base de datos.")
        print("   Verifica tu archivo .env y las credenciales.")
        return False
    
    # Paso 2: Query simple
    if not test_simple_query():
        print("\n⚠️  La query simple falló.")
        return False
    
    # Paso 3: Query compleja limitada
    if not test_coctel_completo_with_limit():
        print("\n⚠️  La query coctel_completo falló incluso con límite.")
        return False
    
    print("\n" + "=" * 60)
    print("✅ TODOS LOS DIAGNÓSTICOS PASARON")
    print("=" * 60)
    print("\n💡 Sugerencia: El problema puede ser:")
    print("   1. La query completa es muy pesada (muchos JOINs)")
    print("   2. La base de datos tiene muchos registros")
    print("   3. La conexión es lenta")
    print("\n🔧 Soluciones:")
    print("   - Agregar índices a las tablas")
    print("   - Reducir el intervalo de fechas (de 3 meses a 1 mes)")
    print("   - Usar paginación en la query")
    print("   - Implementar caché de datos")
    
    return True


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    run_full_diagnostic()