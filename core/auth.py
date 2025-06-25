import streamlit as st
import hashlib
from typing import Dict, Optional

class AuthManager:
    """Sistema de autenticación simple"""
    
    def __init__(self):
        # En producción, esto debería estar en una base de datos
        self.users = {
            "admin": {
                "password_hash": self._hash_password("admin123"),
                "role": "admin",
                "name": "Administrador"
            },
            "analista": {
                "password_hash": self._hash_password("analista123"), 
                "role": "analyst",
                "name": "Analista"
            },
            "viewer": {
                "password_hash": self._hash_password("viewer123"),
                "role": "viewer", 
                "name": "Visualizador"
            }
        }
        
    def _hash_password(self, password: str) -> str:
        """Hash de contraseña usando SHA256"""
        return hashlib.sha256(password.encode()).hexdigest()
        
    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        """Autenticar usuario"""
        if username in self.users:
            user = self.users[username]
            if user["password_hash"] == self._hash_password(password):
                return {
                    "username": username,
                    "role": user["role"], 
                    "name": user["name"]
                }
        return None
        
    def is_logged_in(self) -> bool:
        """Verificar si hay usuario logueado"""
        return "user" in st.session_state
        
    def get_current_user(self) -> Optional[Dict]:
        """Obtener usuario actual"""
        return st.session_state.get("user")
        
    def logout(self):
        """Cerrar sesión"""
        if "user" in st.session_state:
            del st.session_state["user"]
            
    def login_form(self):
        """Mostrar formulario de login"""
        st.title("🔐 Acceso al Dashboard SIMA")
        
        with st.form("login_form"):
            st.markdown("### Iniciar Sesión")
            username = st.text_input("Usuario")
            password = st.text_input("Contraseña", type="password")
            login_button = st.form_submit_button("Ingresar")
            
            if login_button:
                user = self.authenticate(username, password)
                if user:
                    st.session_state["user"] = user
                    st.success(f"¡Bienvenido, {user['name']}!")
                    st.rerun()
                else:
                    st.error("Usuario o contraseña incorrectos")
