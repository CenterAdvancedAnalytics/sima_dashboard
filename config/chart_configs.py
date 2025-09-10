from typing import Dict, Any

CHART_CONFIGS = {
    "coctel_proportion": {
        "title": "Proporción de cocteles",
        "chart_type": "dataframe",
        "filters": ["date_range", "location"],
        "aggregation": "coctel_by_source"
    },
    "weekly_trend": {
        "title": "Gráfico semanal por porcentaje de cocteles", 
        "chart_type": "line",
        "filters": ["date_range", "location", "source"],
        "aggregation": "weekly_percentage"
    },
    "weekly_favor_contra": {
        "title": "Gráfico semanal de noticias a favor y en contra",
        "chart_type": "line_multi",
        "filters": ["date_range", "location", "source"], 
        "aggregation": "weekly_favor_contra"
    },
    "monthly_evolution": {
        "title": "Conteo mensual de coctel por región",
        "chart_type": "bar_stacked",
        "filters": ["month_range", "locations"],
        "aggregation": "monthly_count"
    },
    "position_distribution": {
        "title": "Distribución de posiciones",
        "chart_type": "pie",
        "filters": ["date_range", "locations", "source", "coctel_type"],
        "aggregation": "position_count"
    }
}