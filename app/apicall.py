#!/usr/bin/env python3

import requests
from requests.auth import HTTPBasicAuth
import json
from sync_service_test import ConfigManager
from datetime import datetime, timedelta

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
import asyncpg
import pyodbc
import yaml
from dataclasses import dataclass


class OpenProjectAPI:
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.base_url = config.openproject_config['url'].rstrip('/')
        self.api_key = config.openproject_config['api_key']
        self._session = None
        self._custom_fields_cache = {}
        self.cache_timestamp = None
        self.cache_duration = timedelta(hours=1)  # Cache valida per 1 ora

    def _is_cache_valid(self) -> bool:
        """Verifica se la cache è ancora valida"""
        if self.cache_timestamp is None:
            return False
        return datetime.now() - self.cache_timestamp < self.cache_duration

    def get_projects_schema(self):
        """
        Effettua una chiamata GET all'endpoint /api/v3/projects/schema
        
        Returns:
            dict: Risposta JSON dell'API o None in caso di errore
        """
        try:
            if self._session is None:
                with requests.Session() as s:
                    s.auth = HTTPBasicAuth("apikey", self.api_key)
                    s.headers.update({"Content-Type": "application/hal+json"})
                    response = s.get(f"{self.base_url}/api/v3/projects/schema")
                    response.raise_for_status()
                
                return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Errore nel parsing del JSON: {e}")
            return None
        
    def extract_custom_fields_from_schema(self, schema: Dict) -> Dict[str, Dict]:
        """
        Estrae i custom fields dallo schema del progetto
        Returns: Dict con structure {field_key: field_info}
        """
        custom_fields = {}
        
        if not schema:
            return custom_fields
        
        # Cerca tutti i campi che iniziano con "customField"
        for field_key, field_info in schema.items():
            if field_key.startswith('customField'):
                custom_fields[field_info.get('name')] = {
                    'id': field_key,
                    'name': field_info.get('name', ''),
                    'type': field_info.get('type', ''),
                    'required': field_info.get('required', False),
                    'writable': field_info.get('writable', False),
                    'location': field_info.get('location', ''),
                    'options': field_info.get('options', {})
                }
        
        logging.info(f"Estratti {len(custom_fields)} custom fields dallo schema")
        return custom_fields
    
    def load_custom_fields_to_cache(self) -> bool:
        """
        Carica i custom fields nella cache
        """
        try:
            # Ottieni lo schema
            schema = self.get_projects_schema()
            if not schema:
                return False
            
            # Aggiorna la cache
            self._custom_fields_cache = self.extract_custom_fields_from_schema(schema)
            print(self._custom_fields_cache['Numero Impianto'])
            
            # Aggiorna timestamp cache
            self.cache_timestamp = datetime.now()
            
            logging.info(f"Cache aggiornata con {len(self._custom_fields_cache)} custom fields")
            return True
            
        except Exception as e:
            logging.error(f"Errore nel caricamento della cache: {e}")
            return False

    def get_projects_form(self):
        """
        Effettua una chiamata GET all'endpoint /api/v3/projects/form
        
        Returns:
            dict: Risposta JSON dell'API o None in caso di errore
        """
        try:
            if self._session is None:
                with requests.Session() as s:
                    s.auth = HTTPBasicAuth("apikey", self.api_key)
                    s.headers.update({"Content-Type": "application/hal+json"})
                    response = s.get(f"{self.base_url}/api/v3/projects/form")
                    response.raise_for_status()
                
                return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Errore nel parsing del JSON: {e}")
            return None

    def _build_project_payload(self, project_data: Dict[str, Any], custom_field_values: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Costruisce il payload per la creazione del progetto utilizzando il form template
        
        Args:
            project_data: Dati base del progetto (name, identifier, description, etc.)
            custom_field_values: Valori per i custom fields {field_key: value}
        
        Returns:
            dict: Payload formattato per l'API
        """
        # Ottieni il form template
        form_data = self.get_projects_form()
        if not form_data:
            raise Exception("Impossibile ottenere il form template")
        
        # Base payload dal form
        payload = form_data.get('_embedded', {}).get('payload', {})
        
        # Aggiorna i campi base
        if 'name' in project_data:
            payload['name'] = project_data['name']
        
        if 'identifier' in project_data:
            payload['identifier'] = project_data['identifier']
        
        if 'description' in project_data:
            payload['description'] = {
                'format': 'markdown',
                'raw': project_data['description']
            }
        
        if 'public' in project_data:
            payload['public'] = project_data['public']
        
        if 'active' in project_data:
            payload['active'] = project_data['active']
        
        # Gestisci parent project se specificato
        if 'parent_id' in project_data:
            payload['parent'] = {
                'href': f"/api/v3/projects/{project_data['parent_id']}"
            }
        
        # Aggiungi custom fields se forniti
        if custom_field_values and self._custom_fields_cache:
            for field_key, value in custom_field_values.items():
                if field_key in self._custom_fields_cache:
                    field_info = self._custom_fields_cache[field_key]
                    
                    # Formatta il valore in base al tipo di campo
                    formatted_value = self._format_custom_field_value(field_info, value)
                    
                    if formatted_value is not None:
                        payload[field_key] = formatted_value
        
        return payload
    
    def _format_custom_field_value(self, field_info: Dict, value: Any) -> Any:
        """
        Formatta il valore del custom field in base al tipo
        
        Args:
            field_info: Informazioni del campo dalla cache
            value: Valore da formattare
        
        Returns:
            Valore formattato o None se non valido
        """
        field_type = field_info.get('type', '')
        
        if field_type == 'String':
            return str(value) if value is not None else None
        
        elif field_type == 'Text':
            return {
                'format': 'plain',
                'raw': str(value)
            } if value is not None else None
        
        elif field_type == 'Integer':
            try:
                return int(value) if value is not None else None
            except (ValueError, TypeError):
                logging.warning(f"Valore non valido per campo integer {field_info['id']}: {value}")
                return None
        
        elif field_type == 'Float':
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                logging.warning(f"Valore non valido per campo float {field_info['id']}: {value}")
                return None
        
        elif field_type == 'Boolean':
            return bool(value) if value is not None else None
        
        elif field_type == 'Date':
            if isinstance(value, str):
                # Assumi formato ISO date string
                return value
            elif hasattr(value, 'isoformat'):
                return value.isoformat()
            else:
                logging.warning(f"Valore data non valido per campo {field_info['id']}: {value}")
                return None
        
        elif field_type == 'List':
            # Per campi select/multi-select
            options = field_info.get('options', {})
            if isinstance(value, (list, tuple)):
                # Multi-select
                formatted_values = []
                for v in value:
                    if str(v) in options:
                        formatted_values.append({
                            'href': options[str(v)].get('href', '')
                        })
                return formatted_values if formatted_values else None
            else:
                # Single select
                if str(value) in options:
                    return {
                        'href': options[str(value)].get('href', '')
                    }
                else:
                    logging.warning(f"Valore opzione non valido per campo {field_info['id']}: {value}")
                    return None
        
        else:
            # Tipo non riconosciuto, restituisci as-is
            logging.warning(f"Tipo campo non riconosciuto: {field_type}")
            return value

    def create_project(self, project_data: Dict[str, Any], custom_field_values: Dict[str, Any] = None) -> Optional[Dict]:
        """
        Crea un nuovo progetto in OpenProject
        
        Args:
            project_data: Dati base del progetto
                - name (str): Nome del progetto (required)
                - identifier (str): Identificatore unico (required)
                - description (str): Descrizione del progetto (optional)
                - public (bool): Se il progetto è pubblico (optional, default False)
                - active (bool): Se il progetto è attivo (optional, default True)
                - parent_id (str/int): ID del progetto parent (optional)
            custom_field_values: Valori per i custom fields {field_key: value}
        
        Returns:
            dict: Dati del progetto creato o None in caso di errore
        """
        try:
            # Verifica che la cache sia valida
            if not self._is_cache_valid():
                logging.info("Cache non valida, ricarico custom fields...")
                if not self.load_custom_fields_to_cache():
                    logging.warning("Impossibile caricare la cache dei custom fields")
            
            # Costruisci il payload
            payload = self._build_project_payload(project_data, custom_field_values)
            
            # Effettua la chiamata API
            with requests.Session() as s:
                s.auth = HTTPBasicAuth("apikey", self.api_key)
                s.headers.update({
                    "Content-Type": "application/json",
                    "Accept": "application/hal+json"
                })
                
                response = s.post(
                    f"{self.base_url}/api/v3/projects",
                    json=payload
                )
                
                # Log della richiesta per debug
                logging.debug(f"Request payload: {json.dumps(payload, indent=2)}")
                logging.debug(f"Response status: {response.status_code}")
                logging.debug(f"Response body: {response.text}")
                
                response.raise_for_status()
                
                created_project = response.json()
                logging.info(f"Progetto creato con successo: {created_project.get('name')} (ID: {created_project.get('id')})")
                
                return created_project
                
        except requests.exceptions.HTTPError as e:
            logging.error(f"Errore HTTP nella creazione del progetto: {e}")
            if hasattr(e, 'response') and e.response:
                logging.error(f"Response body: {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Errore di rete nella creazione del progetto: {e}")
            return None
        except Exception as e:
            logging.error(f"Errore generico nella creazione del progetto: {e}")
            return None


def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Crea un'istanza della classe API
    api = OpenProjectAPI(config=ConfigManager())
    
    # Carica la cache dei custom fields
    print("Caricando cache custom fields...")
    cache_loaded = api.load_custom_fields_to_cache()

    if cache_loaded:
        print("Cache caricata con successo!")
        
        # Esempio di creazione progetto
        project_data = {
            'name': 'Test Project API',
            'identifier': 'test-project-api',
            'description': 'Progetto di test creato via API',
            'public': False,
            'active': True
        }
        
        # Esempio di custom field values (sostituisci con i tuoi field keys)
        custom_fields = {
            'customField1': 'Valore custom 1',
            'customField2': 42,
            'customField3': True
        }
        
        print("Creando nuovo progetto...")
        new_project = api.create_project(project_data, custom_fields)
        
        if new_project:
            print("Progetto creato con successo!")
            print(f"Nome: {new_project.get('name')}")
            print(f"ID: {new_project.get('id')}")
            print(f"Identifier: {new_project.get('identifier')}")
        else:
            print("Errore nella creazione del progetto")
    else:
        print("Errore nel caricamento della cache")


if __name__ == "__main__":
    main()