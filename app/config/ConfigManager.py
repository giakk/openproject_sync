#!/usr/bin/env python3

import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
import yaml
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """Configurazione database gestionale Microsoft SQL(readonly)"""
    host: str
    port: int
    database: str
    username: str
    password: str
    extract_projects_query: str
    
@dataclass
class CacheDBConfig:
    """Configurazione database di appoggio"""
    host: str
    port: int
    database: str
    username: str
    password: str
    query_path: str
    
@dataclass
class OpenProjectConfig:
    """Configurazione API OpenProject"""
    base_url: str
    api_key: str
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    
@dataclass
class SyncConfig:
    """Configurazione sincronizzazione"""
    batch_size: int = 5
    max_concurrent_requests: int = 10
    full_sync_interval: int = 3600  # secondi
    incremental_sync_interval: int = 300  # secondi
    max_retry_attempts: int = 3
    enable_auto_correction: bool = False

@dataclass
class LoggerConfig:

    level: str = "INFO"
    filename: str = "/home/riccardo/syncer/logs/sync.log"
    max_file_size: int
    backup_count: int


class ConfigManager:
    """Gestisce la configurazione dell'applicazione"""
    
    def __init__(self, config_path: str = "/home/riccardo/syncer/config/config.yaml"):
        self.config_path = Path(config_path)
        self._config = None
        self.gestionale_db = None
        self.openproject = None
        self.cache_db = None
        self.sync = None
        self.logger = None
        self.load_config()
    
    def load_config(self) -> None:
        """Carica la configurazione dal file YAML"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
                
            self.gestionale_db = DatabaseConfig(
                host=self._config['databases']['sql_server']['host'],
                port=self._config['databases']['sql_server']['port'],
                database=self._config['databases']['sql_server']['database'],
                username=self._config['databases']['sql_server']['username'],
                password=self._config['databases']['sql_server']['password'],
                extract_projects_query=self._config['databases']['sql_server']['extract_projects_query']
            )
            
            self.cache_db = CacheDBConfig(
                host=self._config['databases']['postgresql']['host'],
                port=self._config['databases']['postgresql']['port'],
                database=self._config['databases']['postgresql']['database'],
                username=self._config['databases']['postgresql']['username'],
                password=self._config['databases']['postgresql']['password'],
                query_path=self._config['databases']['postgresql']['query_path']
            )
            
            self.openproject = OpenProjectConfig(
                api_key=self._config['openproject']['apikey'],
                base_url=self._config['openproject']['url']
            )

            self.logger = LoggerConfig(
                level=self.config['logging']['level'],
                filename=self.config['logging']['file'],
                backup_count=self.config['logging']['backup_count'],
                max_file_size=self.config['logging']['max_file_size']
            )
            
            self.sync = SyncConfig()
            
            logging.info(f"Configurazione caricata da {self.config_path}")
        except Exception as e:
            logging.error(f"Errore nel caricamento della configurazione: {e}")
            sys.exit(1)
            
    def validate(self) -> list[str]:
        """Valida la configurazione e restituisce lista di errori"""
        errors = []
        
        # Validazione database gestionale
        if not all([self.gestionale_db.host, self.gestionale_db.database, 
                   self.gestionale_db.username, self.gestionale_db.password]):
            errors.append("Configurazione database gestionale incompleta")
        
        # Validazione OpenProject
        if not all([self.openproject.base_url, self.openproject.api_key]):
            errors.append("Configurazione OpenProject incompleta")
        
        # Validazione database cache
        if not all([self.cache_db.host, self.cache_db.database, 
                   self.cache_db.username, self.cache_db.password]):
            errors.append("Configurazione database cache incompleta")
        
        return errors
    
    @property
    def cacheDB_config(self) -> CacheDBConfig:
        return self.cache_db
    
    @property
    def gestionale_config(self) -> DatabaseConfig:
        return self.gestionale_db
    
    @property
    def config(self) -> Dict:
        return self._config
    
    @property
    def sql_server_config(self) -> Dict:
        return self._config['databases']['sql_server']
    
    @property
    def postgresql_config(self) -> Dict:
        return self._config['databases']['postgresql']
    
    @property
    def openproject_config(self) -> Dict:
        return self._config['openproject']
    
    @property
    def sync_config(self) -> Dict:
        return self._config['sync']