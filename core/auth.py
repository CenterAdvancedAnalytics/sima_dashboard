# core/auth.py
import streamlit as st
import hashlib
import bcrypt
import json
import os
import secrets
from datetime import datetime, timedelta
from typing import Dict, Optional

class AuthManager:
    """Sistema de autenticación mejorado con gestión de contraseñas"""
    
    def __init__(self, config_file="config/users.json"):
        self.config_file = config_file
        self.password_reset_tokens = {}
        self.failed_attempts = {}
        
        # Crear directorio de configuración si no existe
        os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
        
        self.users = self._load_users()
        
        # Inicializar session state si no existe
        if 'auth_initialized' not in st.session_state:
            st.session_state.auth_initialized = True
    
    def _hash_password(self, password: str) -> str:
        """Hash seguro de contraseña usando bcrypt"""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    def _verify_password(self, password: str, hash_str: str) -> bool:
        """Verificar contraseña contra hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), hash_str.encode('utf-8'))
        except ValueError:
            # Fallback para hashes SHA256 antiguos (migración)
            return hashlib.sha256(password.encode()).hexdigest() == hash_str
    
    def _load_users(self) -> Dict:
        """Cargar usuarios desde archivo JSON o variables de entorno"""
        # Intentar cargar desde archivo primero
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                pass
        
        # Si no existe archivo, cargar desde variables de entorno
        users_json = os.getenv('USERS_CONFIG')
        if users_json:
            try:
                return json.loads(users_json)
            except json.JSONDecodeError:
                st.error("Error: USERS_CONFIG mal formateado")
        
        return {}
    
    def _save_users(self):
        """Guardar usuarios en archivo JSON"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.users, f, indent=2, ensure_ascii=False)
        except Exception as e:
            st.error(f"Error guardando datos de usuario: {e}")
    
    def _create_default_admin(self):
        """Crear usuario admin por defecto"""
        if not self.users:
            # En producción, usar variables de entorno
            admin_password = os.getenv('ADMIN_PASSWORD')
            admin_name = os.getenv('ADMIN_NAME', 'Administrator')
            
            if admin_password:
                # Configuración desde variables de entorno
                self.users["admin"] = {
                    "password_hash": self._hash_password(admin_password),
                    "role": "admin",
                    "name": admin_name,
                    "created_at": datetime.now().isoformat()
                }
                self._save_users()
                st.success("✅ Admin configurado desde variables de entorno")
            else:
                # Fallback para desarrollo local
                default_password = "admin123"
                self.users["admin"] = {
                    "password_hash": self._hash_password(default_password),
                    "role": "admin",
                    "name": "Administrador",
                    "created_at": datetime.now().isoformat(),
                    "is_default": True
                }
                self._save_users()
                st.warning("⚠️ Usuario admin creado con contraseña por defecto.")
    
    def setup_initial_admin(self):
        """Configurar el primer administrador de forma segura"""
        if self.users:
            return False  # Ya hay usuarios
        
        st.error("⚠️ No se encontraron usuarios en el sistema.")
        st.info("Configura el primer usuario administrador:")
        
        with st.form("initial_admin_setup"):
            st.markdown("### Configuración Inicial del Administrador")
            
            admin_username = st.text_input("Nombre de usuario administrador", value="admin")
            admin_password = st.text_input("Contraseña administrador", type="password")
            confirm_password = st.text_input("Confirmar contraseña", type="password")
            admin_name = st.text_input("Nombre completo", value="Administrador")
            
            if st.form_submit_button("Crear Administrador"):
                if not admin_password or len(admin_password) < 8:
                    st.error("La contraseña debe tener al menos 8 caracteres")
                elif admin_password != confirm_password:
                    st.error("Las contraseñas no coinciden")
                elif not admin_username:
                    st.error("El nombre de usuario es requerido")
                else:
                    self.users[admin_username] = {
                        "password_hash": self._hash_password(admin_password),
                        "role": "admin",
                        "name": admin_name,
                        "created_at": datetime.now().isoformat()
                    }
                    self._save_users()
                    st.success("✅ Administrador creado exitosamente")
                    st.rerun()
        
        return True  # Indica que se necesita configuración
    
    def _is_rate_limited(self, username: str) -> bool:
        """Verificar si el usuario está limitado por intentos fallidos"""
        if username in self.failed_attempts:
            attempts, last_attempt = self.failed_attempts[username]
            if attempts >= 5 and datetime.now() - last_attempt < timedelta(minutes=15):
                return True
        return False
    
    def _record_failed_attempt(self, username: str):
        """Registrar intento de login fallido"""
        if username in self.failed_attempts:
            attempts, _ = self.failed_attempts[username]
            self.failed_attempts[username] = (attempts + 1, datetime.now())
        else:
            self.failed_attempts[username] = (1, datetime.now())
    
    def _clear_failed_attempts(self, username: str):
        """Limpiar intentos fallidos después de login exitoso"""
        if username in self.failed_attempts:
            del self.failed_attempts[username]
    
    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        """Autenticar usuario con rate limiting"""
        if self._is_rate_limited(username):
            return None
            
        if username in self.users:
            user = self.users[username]
            if self._verify_password(password, user["password_hash"]):
                self._clear_failed_attempts(username)
                return {
                    "username": username,
                    "role": user["role"], 
                    "name": user["name"]
                }
        
        self._record_failed_attempt(username)
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
        # Limpiar otros estados relacionados con auth
        for key in list(st.session_state.keys()):
            if key.startswith("show_") or key.startswith("auth_"):
                del st.session_state[key]
    
    def require_auth(self):
        """Decorator/método para requerir autenticación"""
        if not self.users:
            # Si no hay usuarios, intentar crear admin por defecto o mostrar setup
            if self.setup_initial_admin():
                st.stop()
            else:
                self._create_default_admin()
        
        if not self.is_logged_in():
            self.login_form()
            st.stop()
        
        return self.get_current_user()
    
    def check_permission(self, required_role: str = None) -> bool:
        """Verificar permisos del usuario actual"""
        user = self.get_current_user()
        if not user:
            return False
        
        if not required_role:
            return True
        
        role_hierarchy = {"admin": 3, "analyst": 2, "viewer": 1}
        user_level = role_hierarchy.get(user["role"], 0)
        required_level = role_hierarchy.get(required_role, 0)
        
        return user_level >= required_level
    
    def change_display_name(self, username: str, password: str, new_name: str) -> bool:
        """Cambiar nombre de visualización (usuario debe autenticarse)"""
        if not self.authenticate(username, password):
            return False
        
        if not new_name or len(new_name.strip()) < 2:
            return False
            
        self.users[username]["name"] = new_name.strip()
        self.users[username]["name_changed_at"] = datetime.now().isoformat()
        self._save_users()
        
        # Actualizar también en session_state si es el usuario actual
        if "user" in st.session_state and st.session_state["user"]["username"] == username:
            st.session_state["user"]["name"] = new_name.strip()
        
        return True
    
    def change_password(self, username: str, old_password: str, new_password: str) -> bool:
        """Cambiar contraseña (usuario debe conocer la contraseña actual)"""
        if not self.authenticate(username, old_password):
            return False
        
        if len(new_password) < 8:
            return False
            
        self.users[username]["password_hash"] = self._hash_password(new_password)
        self.users[username]["password_changed_at"] = datetime.now().isoformat()
        # Marcar que ya no es contraseña por defecto
        if "is_default" in self.users[username]:
            del self.users[username]["is_default"]
        self._save_users()
        return True
    
    def admin_reset_password(self, admin_username: str, admin_password: str, 
                           target_username: str, new_password: str) -> bool:
        """Admin puede resetear contraseña de cualquier usuario"""
        admin_user = self.authenticate(admin_username, admin_password)
        if not admin_user or admin_user["role"] != "admin":
            return False
            
        if target_username not in self.users:
            return False
        
        if len(new_password) < 8:
            return False
            
        self.users[target_username]["password_hash"] = self._hash_password(new_password)
        self.users[target_username]["password_reset_by"] = admin_username
        self.users[target_username]["password_reset_at"] = datetime.now().isoformat()
        self._save_users()
        return True
    
    def create_user(self, admin_username: str, admin_password: str, 
                   new_username: str, new_password: str, role: str, name: str) -> bool:
        """Admin puede crear nuevos usuarios"""
        admin_user = self.authenticate(admin_username, admin_password)
        if not admin_user or admin_user["role"] != "admin":
            return False
        
        if new_username in self.users:
            return False  # Usuario ya existe
        
        if len(new_password) < 8:
            return False
        
        if role not in ["admin", "analyst", "viewer"]:
            return False
        
        self.users[new_username] = {
            "password_hash": self._hash_password(new_password),
            "role": role,
            "name": name,
            "created_by": admin_username,
            "created_at": datetime.now().isoformat()
        }
        self._save_users()
        return True
    
    def delete_user(self, admin_username: str, admin_password: str, target_username: str) -> bool:
        """Admin puede eliminar usuarios"""
        admin_user = self.authenticate(admin_username, admin_password)
        if not admin_user or admin_user["role"] != "admin":
            return False
        
        if target_username not in self.users:
            return False
        
        if target_username == admin_username:
            return False  # No puede eliminarse a sí mismo
        
        del self.users[target_username]
        self._save_users()
        return True
    
    def login_form(self):
        """Mostrar formulario de login"""
        st.title("🔐 Acceso al Dashboard SIMA")
        
        # Advertencia si hay contraseñas por defecto
        if any(user.get("is_default", False) for user in self.users.values()):
            st.warning("⚠️ Se detectaron contraseñas por defecto. Cambia las contraseñas inmediatamente por seguridad.")
        
        with st.form("login_form"):
            st.markdown("### Iniciar Sesión")
            username = st.text_input("Usuario")
            password = st.text_input("Contraseña", type="password")
            login_button = st.form_submit_button("Ingresar")
            
            if login_button:
                if self._is_rate_limited(username):
                    st.error("Demasiados intentos fallidos. Intenta nuevamente en 15 minutos.")
                    return
                
                user = self.authenticate(username, password)
                if user:
                    st.session_state["user"] = user
                    st.success(f"¡Bienvenido, {user['name']}!")
                    st.rerun()
                else:
                    st.error("Usuario o contraseña incorrectos")
    
    def render_sidebar_user_info(self):
        """Renderizar información del usuario en la barra lateral"""
        if not self.is_logged_in():
            return
        
        user = self.get_current_user()
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("### Usuario")
        st.sidebar.markdown(f"**{user['name']}**")
        st.sidebar.markdown(f"*Rol: {user['role']}*")
        
        # Solo mostrar panel de administración para admins
        if user["role"] == "admin":
            if st.sidebar.button("Panel de Administración"):
                st.session_state["show_admin_panel"] = True
        
        if st.sidebar.button("Cerrar Sesión"):
            self.logout()
            st.rerun()
    
    def render_admin_panel(self):
        """Renderizar panel de administración"""
        if not st.session_state.get("show_admin_panel", False):
            return
        
        user = self.get_current_user()
        if user["role"] != "admin":
            return
        
        st.markdown("### Panel de Administración")
        
        tab1, tab2, tab3, tab4 = st.tabs(["Usuarios", "Crear Usuario", "Resetear Contraseña", "Cambiar Nombres"])
        
        with tab1:
            st.markdown("#### Usuarios del Sistema")
            for username, user_data in self.users.items():
                col1, col2, col3, col4, col5 = st.columns([2, 2, 1, 1, 1])
                with col1:
                    st.write(f"**{username}**")
                with col2:
                    st.write(user_data["name"])
                with col3:
                    st.write(user_data["role"])
                with col4:
                    if user_data.get("is_default", False):
                        st.write("⚠️ Default")
                with col5:
                    if username != user["username"]:
                        if st.button("🗑️", key=f"delete_{username}"):
                            st.session_state[f"confirm_delete_{username}"] = True
                
                # Confirmación de eliminación
                if st.session_state.get(f"confirm_delete_{username}", False):
                    st.warning(f"¿Confirmas eliminar al usuario {username}?")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Sí, eliminar", key=f"confirm_yes_{username}"):
                            admin_pass = st.text_input("Tu contraseña:", type="password", key=f"admin_pass_{username}")
                            if admin_pass and self.delete_user(user["username"], admin_pass, username):
                                st.success(f"Usuario {username} eliminado")
                                del st.session_state[f"confirm_delete_{username}"]
                                st.rerun()
                    with col2:
                        if st.button("Cancelar", key=f"confirm_no_{username}"):
                            del st.session_state[f"confirm_delete_{username}"]
                            st.rerun()
        
        with tab2:
            st.markdown("#### Crear Nuevo Usuario")
            with st.form("create_user_form"):
                new_username = st.text_input("Nombre de usuario")
                new_name = st.text_input("Nombre completo")
                new_role = st.selectbox("Rol", ["viewer", "analyst", "admin"])
                new_password = st.text_input("Contraseña", type="password")
                admin_password = st.text_input("Tu contraseña (confirmación)", type="password")
                
                if st.form_submit_button("Crear Usuario"):
                    if self.create_user(user["username"], admin_password, new_username, new_password, new_role, new_name):
                        st.success(f"Usuario {new_username} creado exitosamente")
                        st.rerun()
                    else:
                        st.error("Error al crear usuario. Verifica los datos y tu contraseña.")
        
        with tab3:
            st.markdown("#### Resetear Contraseña de Usuario")
            with st.form("reset_password_form"):
                target_user = st.selectbox("Usuario", list(self.users.keys()))
                new_password = st.text_input("Nueva contraseña", type="password")
                admin_password = st.text_input("Tu contraseña (confirmación)", type="password")
                
                if st.form_submit_button("Resetear Contraseña"):
                    if self.admin_reset_password(user["username"], admin_password, target_user, new_password):
                        st.success(f"Contraseña de {target_user} reseteada exitosamente")
                    else:
                        st.error("Error al resetear contraseña. Verifica tu contraseña.")
        
        with tab4:
            st.markdown("#### Cambiar Nombre de Usuario")
            with st.form("admin_change_name_form"):
                target_user = st.selectbox("Usuario", list(self.users.keys()), key="admin_name_user")
                current_name = self.users.get(target_user, {}).get("name", "")
                st.info(f"Nombre actual: **{current_name}**")
                new_display_name = st.text_input("Nuevo nombre", value=current_name)
                admin_password = st.text_input("Tu contraseña (confirmación)", type="password", key="admin_name_pass")
                
                if st.form_submit_button("Cambiar Nombre"):
                    admin_user = self.authenticate(user["username"], admin_password)
                    if not admin_user or admin_user["role"] != "admin":
                        st.error("Contraseña de administrador incorrecta")
                    elif not new_display_name.strip() or len(new_display_name.strip()) < 2:
                        st.error("El nombre debe tener al menos 2 caracteres")
                    else:
                        self.users[target_user]["name"] = new_display_name.strip()
                        self.users[target_user]["name_changed_by"] = user["username"]
                        self.users[target_user]["name_changed_at"] = datetime.now().isoformat()
                        self._save_users()
                        
                        # Actualizar session_state si es el usuario actual
                        if target_user == user["username"]:
                            st.session_state["user"]["name"] = new_display_name.strip()
                        
                        st.success(f"Nombre de {target_user} cambiado exitosamente")
                        st.rerun()
        
        if st.button("Cerrar Panel"):
            st.session_state["show_admin_panel"] = False
            st.rerun()