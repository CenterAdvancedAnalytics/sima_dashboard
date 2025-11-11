from typing import Dict, List, Any

# Mapeos de datos
ID_POSICION_DICT = {
    1: 'a favor',
    2: 'potencialmente a favor', 
    3: 'neutral',
    4: 'potencialmente en contra',
    5: 'en contra'
}

COCTEL_DICT = {
    0: 'Sin coctel',
    1: 'Con coctel'
}

ID_FUENTE_DICT = {
    1: 'Radio',
    2: 'TV', 
    3: 'Redes'
}

# ✅ PALETA DE COLORES ACTUALIZADA - Coincide con nombres en grafico9.py
COLOR_POSICION_DICT = {
    "A favor": "#2E7D32",  # Verde oscuro
    "Potencialmente a favor": "#66BB6A",  # Verde claro
    "Neutral": "#9E9E9E",  # Gris
    "Potencialmente en contra": "#FF9800",  # Naranja
    "En contra": "#D32F2F",  # Rojo
    # Mantener backward compatibility con minúsculas
    "a favor": "#2E7D32",
    "potencialmente a favor": "#66BB6A",
    "neutral": "#9E9E9E",
    "potencialmente en contra": "#FF9800",
    "en contra": "#D32F2F"
}

COLOR_DISCRETE_MAP = {
    'Celeste': 'lightblue',
    'Rojo': 'Red',
    'Naranja': '#FFA500', 
    'Gris': 'Gray',
    'Azul': 'Blue',
}

FUENTE_COLORS = {
    'Radio': '#3F6EC3',
    'TV': '#A1A1A1', 
    'Redes': '#C00000'
}

# Macroregiones
MACROREGIONES = {
    "Macro región Sur 1": ["Tacna", "Puno", "Cusco"],
    "Macro región Sur 2": ["Ayacucho", "Arequipa"],
    "Macro región Norte": ["Piura", "Trujillo"],
    "Macro región Centro": ["Lima", "Ica", "Huanuco"],
    "Macro región UNACEM": ["Lima Sur", "Cañete", "Tarma"],
    "Macro región TV": ["Ayacucho", "Piura", "Arequipa"]
}

MACROREGIONES_RADIO_REDES = ["Macro región Sur 1", "Macro región Sur 2", "Macro región Norte", "Macro región Centro", "Macro región UNACEM"]
MACROREGIONES_TV = ["Macro región TV"]

# Configuraciones de meses
MESES_ES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
]