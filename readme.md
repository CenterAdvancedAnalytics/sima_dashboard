# 📊 Dashboard SIMA - Versión Modular


### 🔧 **Arquitectura**

```
├── config/                 # Configuraciones y constantes
│   ├── constants.py        # Mapeos y constantes globales
│   └── chart_configs.py    # Configuraciones de gráficos
├── core/                   # Funcionalidades core
│   ├── auth.py            # Sistema de autenticación
│   ├── data_loader.py     # Carga y cache de datos
│   ├── filters.py         # Gestión de filtros
│   └── analytics.py       # Motor de análisis
├── sections/              # Secciones modulares
│   └── coctel_sections.py # Secciones de cocteles
├── queries/               # Consultas SQL (existente)
├── utils.py               # Utilidades (existente)
└── main_app.py           # Aplicación principal
```

### 🔐 **Sistema de Login**

**Usuarios de demostración:**
- **Admin**: `admin` / `admin123` (acceso completo)
- **Analista**: `analista` / `analista123` (análisis completo)
- **Viewer**: `viewer` / `viewer123` (solo visualización)

### 📅 **Filtros Globales**

Los filtros globales permiten:
- **Fechas**: Aplicar mismo rango a todas las secciones
- **Ubicaciones**: Filtrar por ubicaciones específicas
- **Fuentes**: Filtrar por Radio, TV, Redes
- **Override**: Posibilidad de usar filtros específicos por sección

### 🎯 **Beneficios de la Nueva Arquitectura**

1. **Mantenibilidad**: Código organizado en módulos especializados
2. **Escalabilidad**: Fácil agregar nuevas secciones y funcionalidades
3. **Reutilización**: Componentes reutilizables entre secciones
4. **Testeo**: Componentes aislados fáciles de testear
5. **Performance**: Caching inteligente y carga optimizada

### 🚧 **Próximas Mejoras**

- [ ] Migrar todas las 30+ secciones restantes
- [ ] Sistema de exportación de reportes
- [ ] Dashboards personalizables por usuario
- [ ] API REST para integraciones
- [ ] Tests automatizados
- [ ] Métricas de performance

### 🔄 **Migración desde Código Anterior**

Esta nueva versión mantiene compatibilidad con:
- ✅ Mismas consultas SQL (`queries/`)
- ✅ Misma lógica de conexión (`utils.py`)
- ✅ Mismos datos y estructura
- ✅ Funcionalidad de usuarios existente

### 📦 **Instalación**

```bash
# 1. Clonar repositorio
git clone <your-repo>
cd dashboard-sima

# 2. Instalar dependencias
pip install -r requirements_updated.txt

# 3. Configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales

# 4. Ejecutar aplicación
streamlit run main_app.py
```

### 🐳 **Docker (Opcional)**

```bash
# Ejecutar con Docker Compose
docker-compose up --build
```

### 🔧 **Desarrollo**

Para agregar nuevas secciones:

1. **Crear función de análisis** en `core/analytics.py`
2. **Agregar sección** en `sections/coctel_sections.py`
3. **Registrar en tabs** en `main_app.py`

Ejemplo:
```python
def nueva_seccion(self, global_filters):
    st.subheader("Nueva Sección")
    
    # Usar filtros globales
    fecha_inicio, fecha_fin = self.filter_manager.get_section_dates("nueva", global_filters)
    lugares = self.filter_manager.get_section_locations("nueva", global_filters)
    
    # Aplicar filtros y análisis
    data = self.filter_manager.apply_filters(self.temp_coctel_fuente, global_filters)
    result = self.analytics.nueva_funcion_analisis(data)
    
    # Mostrar resultado
    st.plotly_chart(result)
```

Esta arquitectura modular hace que agregar nuevas funcionalidades sea cuestión de minutos en lugar de horas.