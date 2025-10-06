#!/usr/bin/env python3

import pyodbc
import logging
from contextlib import contextmanager
from typing import Generator, List
from ..config.ConfigManager import OpenProjectConfig
from ..models.project import Amministratore, IndirizzoImpianto, OpenProjectProject
import sys

from ..api_connection.requests.post_request import PostRequest
from ..api_connection.requests.get_request import GetRequest
from ..api_connection.requests.patch_request import PatchRequest
from ..utils.connection import Connection
from datetime import datetime, timedelta
import requests
import json
import logging
from typing import Dict, List, Optional, Any


logger = logging.getLogger(__name__)


class OpenProjectInterface:

    def __init__(self, op_config: OpenProjectConfig):
        
        self.config = op_config
        
        self.connection = Connection(
            url= self.config.base_url, 
            apikey= self.config.api_key
            )
        
        self._custom_fields_cache = {}
        self.cache_timestamp = None
        self.cache_duration = timedelta(hours=1)  # Cache valida per 1 ora
        
        self.load_custom_fields_to_cache()

    
    def test_connection(self) -> bool:

        try:
            response = GetRequest(connection=self.connection,
                                   context="/api/v3").execute()
            
            logger.debug("Test connection OpenProject API is PASS")
            return True
        except Exception as e:
            logger.debug(f"Test conneciton OpenProject API is FAILED: {e}")



    def get_projects_schema(self):

        try:
            response = GetRequest(connection=self.connection,
                                   context="/api/v3/projects/schema").execute()
                
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Errore nel parsing del JSON: {e}")
            return None
        
    def get_list_of_projects(self):

        try:
            response = GetRequest(connection=self.connection,
                                   context="/api/v3/projects").execute()
            
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Errore nel parsing del JSON: {e}")
            return None

    def create_project(self, record: OpenProjectProject) -> OpenProjectProject:
        
        try:

            payload = record.to_api_payload()

            response = PostRequest(connection=self.connection,
                                   context="/api/v3/projects",
                                   headers={"Content-Type": "application/json"},
                                   json=payload).execute()
            
            
            # Parse of the response to update the data
            record.id = response.get('id')
            record.created_at = response.get('createdAt')
            record.updated_at = response.get('updatedAt')
            
            logger.info(f"Progetto creato con successo: {response.get('name')} (ID: {response.get('id')})")
            return record
                
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

    def update_project (self, record: OpenProjectProject, id: int) -> OpenProjectProject: 

        try:

            payload = record.to_api_payload(is_for_update=True)

            response = PatchRequest(connection=self.connection,
                                   context=f"/api/v3/projects/{id}",
                                   headers={"Content-Type": "application/json"},
                                   json=payload).execute()
            
            record.updated_at = response.get('updated_at')
            
            logging.info(f"Progetto creato con successo: {response.get('name')} (ID: {response.get('id')})")
                
            return record
                
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
        

    ### CUSTOM FIELD HANDLE ###

    def _is_cache_valid(self) -> bool:
        """Verifica se la cache è ancora valida"""
        if self.cache_timestamp is None:
            return False
        return datetime.now() - self.cache_timestamp < self.cache_duration


    def extract_custom_fields_from_schema(self, schema: Dict) -> Dict[str, str]:
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
                custom_fields[field_info.get('name')] = field_key
        
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
            
            # Aggiorna timestamp cache
            self.cache_timestamp = datetime.now()
            
            logger.info(f"Cache aggiornata con {len(self._custom_fields_cache)} custom fields")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel caricamento della cache: {e}")
            return False