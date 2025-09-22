#!/usr/bin/env python3

import io
import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import aiofiles
import aiohttp
import requests
from requests.auth import HTTPBasicAuth
import asyncpg
import pyodbc
import yaml
from dataclasses import dataclass

class ConfigManager:
    """Gestisce la configurazione dell'applicazione"""
    
    def __init__(self, config_path: str = "/home/riccardo/syncer/config/config.yaml"):
        self.config_path = Path(config_path)
        self._config = None
        self.load_config()
    
    def load_config(self) -> None:
        """Carica la configurazione dal file YAML"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            logging.info(f"Configurazione caricata da {self.config_path}")
        except Exception as e:
            logging.error(f"Errore nel caricamento della configurazione: {e}")
            sys.exit(1)
    
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