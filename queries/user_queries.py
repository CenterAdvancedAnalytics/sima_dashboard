#user_queries.py

from sqlalchemy.sql import text

queries = {
    # Queries de usuarios
    "usuarios_por_dia": {
        "read": text("""
        SELECT
            DATE(a.fecha_registro) AS fecha,
            COUNT(DISTINCT a.id_usuario_registro) AS usuarios_distintos
        FROM
            acontecimientos a
        GROUP BY
            DATE(a.fecha_registro)
        ORDER BY
            fecha;
        """)
    },
    "acontecimientos_por_dia": {
        "read": text("""
        SELECT
            DATE(a.fecha_registro) AS fecha,
            COUNT(a.id) AS total_acontecimientos
        FROM
            acontecimientos a
        GROUP BY
            DATE(a.fecha_registro)
        ORDER BY
            fecha;
        """)
    },
    "usuarios_ultimo_dia": {
        "read": text("""
        SELECT
            u.id AS id_usuario,
            u.nombre AS nombre_usuario,
            MAX(a.fecha_update) AS ultima_actualizacion
        FROM
            usuarios u
        LEFT JOIN
            acontecimientos a ON u.id = a.id_usuario_registro
        WHERE a.fecha_registro IS NOT null
        GROUP BY
            u.id, u.nombre 
        ORDER BY
            ultima_actualizacion DESC;
        """)
    },
    "usuarios_7_dias": {
        "read": text("""
        SELECT
            u.id AS id_usuario,
            u.nombre AS nombre_usuario,
            SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '6 days' THEN 1 ELSE 0 END) AS dia_1,
            SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '5 days' THEN 1 ELSE 0 END) AS dia_2,
            SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '4 days' THEN 1 ELSE 0 END) AS dia_3,
            SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '3 days' THEN 1 ELSE 0 END) AS dia_4,
            SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '2 days' THEN 1 ELSE 0 END) AS dia_5,
            SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '1 days' THEN 1 ELSE 0 END) AS dia_6,
            SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE THEN 1 ELSE 0 END) AS dia_7
        FROM
            usuarios u
        LEFT JOIN
            acontecimientos a ON u.id = a.id_usuario_registro
        GROUP BY
            u.id, u.nombre
        ORDER BY
            u.nombre;
        """)
    }
}