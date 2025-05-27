from sqlalchemy.sql import text

queries = {
    # Queries de cocteles
    "coctel_completo": {
        "read": text("""
        SELECT
            a.id AS id,
            a.fecha_registro,
            a.acontecimiento,
            a.coctel,
            a.id_posicion,
            l.nombre AS lugar,
            p.color AS color,
            pr.id_fuente AS id_fuente,
            f.nombre AS fuente_nombre,
            pr.id_canal AS id_canal,
            c.nombre AS canal_nombre,
            ac.nombre AS nombre,
            fb.num_reacciones,
            fb.num_comentarios,
            fb.num_compartidos,
            fb.fecha AS fecha_post,
            fbp.nombre AS nombre_facebook_page,
            t.descripcion AS descripcion,
            mf.mensaje AS mensaje_fuerza
        FROM
            acontecimientos a
        JOIN
            lugares l ON a.id_lugar = l.id
        JOIN
            posiciones p ON a.id_posicion = p.id
        LEFT JOIN
            acontecimiento_programa ap ON a.id = ap.id_acontecimiento
        LEFT JOIN
            programas pr ON ap.id_programa = pr.id
        LEFT JOIN
            fuentes f ON pr.id_fuente = f.id
        LEFT JOIN
            canales c ON pr.id_canal = c.id
        LEFT JOIN
            acontecimiento_actor aa ON a.id = aa.id_acontecimiento
        LEFT JOIN
            actores ac ON aa.id_actor = ac.id
        LEFT JOIN
            acontecimiento_facebook_post afb ON a.id = afb.id_acontecimiento
        LEFT JOIN
            facebook_posts fb ON afb.id_facebook_post = fb.id
        LEFT JOIN
            facebook_pages fbp ON fb.id_facebook_page = fbp.id
        LEFT JOIN
            acontecimiento_tema at ON a.id = at.id_acontecimiento
        LEFT JOIN
            temas t ON at.id_tema = t.id
        LEFT JOIN
            notas n ON a.id_nota = n.id
        LEFT JOIN
            mensaje_fuerza mf ON n.id = mf.id
        WHERE
            a.fecha_registro >= NOW() - INTERVAL '2 years';
        """)
    }
}