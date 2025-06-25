# Dashboard SIMA — Versión Modular

## Arquitectura

```
├── config/
│   ├── constants.py         # Constantes globales
│   └── chart_configs.py     # Configuraciones de visualización
├── core/
│   ├── auth.py              # Autenticación de usuarios
│   ├── data_loader.py       # Carga y caché de datos
│   ├── filters.py           # Filtros globales y por sección
│   └── analytics.py         # Lógica de análisis
├── sections/
│   └── coctel_sections.py   # Secciones de análisis por tipo
├── queries/                 # SQL preexistente
├── utils.py                 # Funciones auxiliares
└── main_app.py              # Entrypoint de la app
```

## Login

Usuarios predefinidos:
* **admin**: `admin` / `admin123` — Acceso total
* **analista**: `analista` / `analista123` — Acceso a todo menos configuraciones
* **viewer**: `viewer` / `viewer123` — Solo lectura

## Filtros Globales

* Fechas (sincronizadas entre secciones)
* Ubicaciones
* Fuentes (radio, TV, redes)
* Override por sección opcional

## Instalación

```bash
git clone <repo>
cd dashboard-sima
pip install -r requirements_updated.txt
cp .env.example .env
# Editar .env manualmente
streamlit run main_app.py
```

## Docker (opcional)

```bash
docker-compose up --build
```

## Cómo Agregar Nuevas Secciones

1. Definir función de análisis en `analytics.py`
2. Crear layout en `coctel_sections.py`
3. Registrar la pestaña en `main_app.py`

### Ejemplo mínimo:

```python
def nueva_seccion(self, global_filters):
    st.subheader("Nueva sección")
    fechas = self.filter_manager.get_section_dates("nueva", global_filters)
    ubicaciones = self.filter_manager.get_section_locations("nueva", global_filters)
    data_filtrada = self.filter_manager.apply_filters(self.temp_coctel_fuente, global_filters)
    resultado = self.analytics.nueva_funcion_analisis(data_filtrada)
    st.plotly_chart(resultado)
```