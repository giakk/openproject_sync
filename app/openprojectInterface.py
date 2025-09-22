from api_connection.requests.post_request import PostRequest
from api_connection.requests.get_request import GetRequest
from api_connection.requests.patch_request import PatchRequest
from connection import Connection
from datetime import datetime, timedelta
from syncRecord import SyncRecord
import requests
import json
import logging
from typing import Dict, List, Optional, Any


class openprojectInterface: 

    def __init__(self, api_config):
        
        self._custom_fields_cache = {}
        self.cache_timestamp = None
        self.cache_duration = timedelta(hours=1)  # Cache valida per 1 ora
        self.connection = Connection(
            url= api_config['url'], 
            apikey= api_config['api_key']
            )
        
    def get_projects_schema(self):

        try:
            response = GetRequest(connection=self.connection,
                                   context="/api/v3/projects/schema").execute()
                
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Errore nel parsing del JSON: {e}")
            return None
        
    def get_list_of_projects(self):

        try:
            response = GetRequest(connection=self.connection,
                                   context="/api/v3/projects").execute()
                
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Errore nel parsing del JSON: {e}")
            return None
        
    def post_new_project(self, project_data):
        
        try:

            response = PostRequest(connection=self.connection,
                                   context="/api/v3/projects",
                                   headers={"Content-Type": "application/json"},
                                   json=project_data).execute()
            
            # Log della richiesta per debug
            # logging.debug(f"Request payload: {json.dumps(project_data, indent=2)}")
            # logging.debug(f"Response status: {response.status_code}")
            # logging.debug(f"Response body: {response.text}")
                
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
        

    def update_project (self, project_data, identifier: int): 

        try:

            response = PatchRequest(connection=self.connection,
                                   context=f"/api/v3/projects/{identifier}",
                                   headers={"Content-Type": "application/json"},
                                   json=project_data).execute()
            
            # Log della richiesta per debug
            # logging.debug(f"Request payload: {json.dumps(project_data, indent=2)}")
            # logging.debug(f"Response status: {response.status_code}")
            # logging.debug(f"Response body: {response.text}")
                
            updated_project = response.json()
            logging.info(f"Progetto creato con successo: {updated_project.get('name')} (ID: {updated_project.get('id')})")
                
            return updated_project
                
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

        
    def workflowOP(self, record: SyncRecord, isAdminChanged: bool) -> Optional[Dict]:

        # Load again the custom field into the cache if necessary
        if not self._is_cache_valid():
            logging.info("Cache non valida, ricarico custom fields...")
            if not self.load_custom_fields_to_cache():
                logging.warning("Impossibile caricare la cache dei custom fields")

        
        if isAdminChanged:
            # TODO: verificare che l'utente esista. 
            #   - Se NON esiste, crealo;
            #   - Se ESISTE, aggiornalo;
            print("")


        id = self.find_project
        if  id == None:

            # Project not found in OpenProject, need to create a new one
            # Crea il projetto, inserendo anche il le info dell'id e del link utente restituiti dalle istruzioni precedenti
            return self.create_project(record)
        
        else: 

            # Update the project based on new SyncRecord
            return self.update_existing_project(record, id)
        
    

    def create_project(self, record: SyncRecord) -> Optional[Dict]:

        project_data = {
            'identifier': record.commessa.get_identifier(),
            'name': record.commessa.NrCommessa,
            'description': f"Progetto per commessa {record.commessa.NrCommessa}",
            'public': bool(True),
            self._custom_fields_cache['Numero Impianto']: record.commessa.CodImpianto,
            self._custom_fields_cache['Indirizzo Impianto']: record.indirizzo.IndirizzoImp,
            self._custom_fields_cache['Apertura Commessa']: record.commessa.AperturaCommessa.isoformat(),
            self._custom_fields_cache['Fine Lavori']: record.commessa.FineLavori.isoformat(),
            self._custom_fields_cache['Note']: record.commessa.Note

        }

        result = self.post_new_project(project_data)

        return result.get('id')
    

    def update_existing_project(self, record: SyncRecord, id: int):

        project_data = {
            'description': f"Progetto per commessa {record.commessa.NrCommessa}",
            self._custom_fields_cache['Numero Impianto']: record.commessa.CodImpianto,
            self._custom_fields_cache['Indirizzo Impianto']: record.indirizzo.IndirizzoImp,
            self._custom_fields_cache['Apertura Commessa']: record.commessa.AperturaCommessa.isoformat(),
            self._custom_fields_cache['Fine Lavori']: record.commessa.FineLavori.isoformat(),
            self._custom_fields_cache['Note']: record.commessa.Note

        }

        return self.update_project(project_data, id)


    def find_project(self, identifier):

        projects_data = self.get_list_of_projects()

        # Verifica che i dati siano validi
        if not projects_data or '_embedded' not in projects_data:   # TODO: manage exception for invalid data
            return None
        
        elements = projects_data['_embedded'].get('elements', [])
        
        # Ricerca diretta, si ferma al primo match
        search_result = next((project for project in elements 
                    if project.get('identifier') == identifier), None)

        # Return OpenProject ID if found any 
        return search_result['id']


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
            
            logging.info(f"Cache aggiornata con {len(self._custom_fields_cache)} custom fields")
            return True
            
        except Exception as e:
            logging.error(f"Errore nel caricamento della cache: {e}")
            return False
        