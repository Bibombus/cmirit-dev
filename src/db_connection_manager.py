import json
import hashlib
import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from sqlalchemy import create_engine, inspect

@dataclass
class ConnectionParams:
    """Параметры подключения к БД."""
    dbms: str
    user: str
    password_hash: str  # Хеш пароля
    host: str
    port: str
    db_name: str
    schema: str

class DBConnectionManager:
    """Менеджер для работы с сохраненными параметрами подключения к БД."""
    
    def __init__(self, config_file: str = "db_connections.json"):
        """Инициализация менеджера.
        
        Args:
            config_file: Путь к файлу с сохраненными параметрами.
        """
        self.config_file = config_file
        self.connections: Dict[str, List[ConnectionParams]] = {}  # {user: [params]}
        self.host_ports: Dict[str, List[str]] = {}  # {host: [ports]}
        self.db_schemas: Dict[str, List[str]] = {}  # {db_name: [schemas]}
        self._load_config()
    
    def _load_config(self):
        """Загружает конфигурацию из файла."""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Загружаем сохраненные подключения
                    for user, params_list in data.get('connections', {}).items():
                        self.connections[user] = [ConnectionParams(**p) for p in params_list]
                    # Загружаем порты для хостов
                    self.host_ports = data.get('host_ports', {})
                    # Загружаем схемы для БД
                    self.db_schemas = data.get('db_schemas', {})
            except Exception as e:
                print(f"Ошибка при загрузке конфигурации: {e}")
    
    def _save_config(self):
        """Сохраняет конфигурацию в файл."""
        try:
            data = {
                'connections': {
                    user: [vars(p) for p in params_list]
                    for user, params_list in self.connections.items()
                },
                'host_ports': self.host_ports,
                'db_schemas': self.db_schemas
            }
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Ошибка при сохранении конфигурации: {e}")
    
    def _hash_password(self, password: str) -> str:
        """Хеширует пароль с помощью MD5.
        
        Args:
            password: Пароль для хеширования.
            
        Returns:
            str: MD5-хеш пароля.
        """
        return hashlib.md5(password.encode()).hexdigest()
    
    def save_connection(self, params: ConnectionParams, password: str):
        """Сохраняет параметры подключения.
        
        Args:
            params: Параметры подключения.
            password: Пароль для хеширования.
        """
        # Хешируем пароль
        params.password_hash = self._hash_password(password)
        
        # Сохраняем параметры подключения
        if params.user not in self.connections:
            self.connections[params.user] = []
        self.connections[params.user].append(params)
        
        # Сохраняем порт для хоста
        if params.host not in self.host_ports:
            self.host_ports[params.host] = []
        if params.port not in self.host_ports[params.host]:
            self.host_ports[params.host].append(params.port)
        
        # Сохраняем схему для БД
        if params.db_name not in self.db_schemas:
            self.db_schemas[params.db_name] = []
        if params.schema not in self.db_schemas[params.db_name]:
            self.db_schemas[params.db_name].append(params.schema)
        
        self._save_config()
    
    def get_connection_params(self, user: str) -> List[ConnectionParams]:
        """Получает сохраненные параметры подключения для пользователя.
        
        Args:
            user: Имя пользователя.
            
        Returns:
            List[ConnectionParams]: Список сохраненных параметров подключения.
        """
        return self.connections.get(user, [])
    
    def get_host_ports(self, host: str) -> List[str]:
        """Получает список портов для хоста.
        
        Args:
            host: Имя хоста.
            
        Returns:
            List[str]: Список портов.
        """
        return self.host_ports.get(host, [])
    
    def get_db_schemas(self, db_name: str) -> List[str]:
        """Получает список схем для БД.
        
        Args:
            db_name: Имя БД.
            
        Returns:
            List[str]: Список схем.
        """
        return self.db_schemas.get(db_name, [])
    
    def update_db_schemas(self, dbms: str, user: str, password: str, host: str, port: str, db_name: str):
        """Обновляет список схем для БД, подключаясь к ней.
        
        Args:
            dbms: Тип СУБД.
            user: Имя пользователя.
            password: Пароль.
            host: Хост.
            port: Порт.
            db_name: Имя БД.
        """
        try:
            # Создаем строку подключения
            dbms_cases = {
                "Oracle": 'oracle',
                "PostgreSQL": 'postgresql', 
                "MSSQL Server": 'mssql'
            }
            url = f"{dbms_cases[dbms]}://{user}:{password}@{host}:{port}/{db_name}"
            
            # Подключаемся к БД
            engine = create_engine(url)
            with engine.connect() as conn:
                # Получаем список схем
                inspector = inspect(engine)
                schemas = inspector.get_schema_names()
                
                # Обновляем список схем
                if db_name not in self.db_schemas:
                    self.db_schemas[db_name] = []
                self.db_schemas[db_name] = list(set(self.db_schemas[db_name] + schemas))
                
                self._save_config()
        except Exception as e:
            print(f"Ошибка при обновлении списка схем: {e}") 