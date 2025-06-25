import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, List, Optional, Dict, Any

class FilterManager:
    """Gestor centralizado de filtros"""
    
    def __init__(self, lugares_uniques: List[str]):
        self.lugares_uniques = lugares_uniques
        self.min_date = None
        self.max_date = None
        
    def set_date_bounds(self, min_date: datetime, max_date: datetime):
        """Establecer límites de fechas basados en los datos"""
        self.min_date = min_date.date() if isinstance(min_date, datetime) else min_date
        self.max_date = max_date.date() if isinstance(max_date, datetime) else max_date
        
    def create_global_filters(self) -> Dict[str, Any]:
        """Crear filtros globales en la sidebar"""
        st.sidebar.header("🗓️ Filtros Globales")
        
        filters = {}
        
        # Filtro de fechas global
        st.sidebar.subheader("Rango de Fechas")
        use_global_dates = st.sidebar.checkbox("Usar fechas globales", value=True)
        
        if use_global_dates:
            default_start = self.max_date - timedelta(days=90) if self.max_date else datetime.now().date() - timedelta(days=90)
            default_end = self.max_date if self.max_date else datetime.now().date()
            
            global_fecha_inicio = st.sidebar.date_input(
                "Fecha Inicio Global",
                value=default_start,
                min_value=self.min_date,
                max_value=self.max_date,
                format="DD.MM.YYYY"
            )
            global_fecha_fin = st.sidebar.date_input(
                "Fecha Fin Global", 
                value=default_end,
                min_value=self.min_date,
                max_value=self.max_date,
                format="DD.MM.YYYY"
            )
            
            # Validar rango de fechas
            if global_fecha_inicio > global_fecha_fin:
                st.sidebar.error("La fecha de inicio debe ser anterior a la fecha de fin")
                return None
                
            filters['use_global_dates'] = True
            filters['global_fecha_inicio'] = pd.to_datetime(global_fecha_inicio)
            filters['global_fecha_fin'] = pd.to_datetime(global_fecha_fin)
        else:
            filters['use_global_dates'] = False
            
        # Filtro de ubicaciones global
        st.sidebar.subheader("Ubicaciones")
        use_global_locations = st.sidebar.checkbox("Usar ubicaciones globales", value=True)
        
        if use_global_locations:
            global_lugares = st.sidebar.multiselect(
                "Lugares Globales",
                self.lugares_uniques,
                default=self.lugares_uniques
            )
            filters['use_global_locations'] = True
            filters['global_lugares'] = global_lugares
        else:
            filters['use_global_locations'] = False
            
        # Mostrar información de filtros
        self._display_filter_info(filters)
        
        return filters
        
    def _display_filter_info(self, filters: Dict[str, Any]):
        """Mostrar información de los filtros aplicados"""
        if filters.get('use_global_dates'):
            inicio = filters['global_fecha_inicio']
            fin = filters['global_fecha_fin']
            st.sidebar.info(f"📅 Período: {inicio.strftime('%d/%m/%Y')} - {fin.strftime('%d/%m/%Y')}")
            days_diff = (fin - inicio).days
            st.sidebar.info(f"⏱️ Duración: {days_diff} días")
            
        if filters.get('use_global_locations'):
            lugares = filters['global_lugares']
            st.sidebar.info(f"📍 {len(lugares)} ubicaciones seleccionadas")
    
    def get_section_dates(self, section_name: str, global_filters: Dict[str, Any],
                         default_days: int = 30) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """Obtener fechas para una sección específica"""
        if global_filters.get('use_global_dates'):
            return global_filters['global_fecha_inicio'], global_filters['global_fecha_fin']
        else:
            col1, col2 = st.columns(2)
            default_end = self.max_date if self.max_date else datetime.now().date()
            default_start = default_end - timedelta(days=default_days)
            
            with col1:
                fecha_inicio = st.date_input(
                    f"Fecha Inicio {section_name}",
                    value=default_start,
                    format="DD.MM.YYYY",
                    key=f"inicio_{section_name}"
                )
            with col2:
                fecha_fin = st.date_input(
                    f"Fecha Fin {section_name}",
                    value=default_end,
                    format="DD.MM.YYYY", 
                    key=f"fin_{section_name}"
                )
            return pd.to_datetime(fecha_inicio), pd.to_datetime(fecha_fin)
    
    def get_section_locations(self, section_name: str, global_filters: Dict[str, Any],
                            multi: bool = True, default: Optional[List[str]] = None):
        """Obtener ubicaciones para una sección específica"""
        if global_filters.get('use_global_locations'):
            return global_filters['global_lugares']
        else:
            if multi:
                return st.multiselect(
                    f"Lugar {section_name}",
                    self.lugares_uniques,
                    default=default or self.lugares_uniques,
                    key=f"lugar_{section_name}"
                )
            else:
                return st.selectbox(
                    f"Lugar {section_name}",
                    self.lugares_uniques,
                    key=f"lugar_{section_name}"
                )