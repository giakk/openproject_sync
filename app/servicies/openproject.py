#!/usr/bin/env python3

import logging
from ..config.ConfigManager import OpenProjectConfig
from ..models.project import OpenProjectProject
from ..models.user import OpenProjectUser


from ..api_connection.requests.post_request import PostRequest
from ..api_connection.requests.get_request import GetRequest
from ..api_connection.requests.patch_request import PatchRequest
from ..utils.connection import Connection
from datetime import datetime, timedelta
import requests
import json
import logging
from typing import Dict
import urllib.parse



logger = logging.getLogger(__name__)


class OpenProjectInterface:

    def __init__(self, op_config: OpenProjectConfig):
        
        self.config = op_config
        
        self.connection = Connection(
            url= self.config.base_url, 
            apikey= self.config.api_key
            )
        
        self._project_custom_fields_cache = {}
        self._user_custom_fields_cache = {}
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
        
    def get_users_schema(self):

        try:
            response = GetRequest(connection=self.connection,
                                   context="/api/v3/users/schema").execute()
                
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Errore nel parsing del JSON: {e}")
            return None
        
    def find_project(self, identifier: str):

        try:

            filter_structure = [
                {
                    "name_and_identifier": {
                        "operator": "=",
                        "values": [identifier]
                    }
                }
            ]

            # Converti in JSON string
            filter_json = json.dumps(filter_structure)
            
            # URL encode
            filter_encoded = urllib.parse.quote(filter_json)

            full_url = f"/api/v3/projects?filters={filter_encoded}"

            response = GetRequest(connection=self.connection,
                                context=full_url).execute()
            
            if response.get('total') == 0 and response.get('count') == 0:
                return None
                
            element = response['_embedded'].get('elements')

            return element[0].get('id')
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Errore nel parsing del JSON: {e}")
            return None
        

    def find_user(self, email:str):

        try:

            filter_structure = [
                {
                    "login": {
                        "operator": "=",
                        "values": [email]
                    }
                }
            ]

            filter_json = json.dumps(filter_structure)
            
            # URL encode
            filter_encoded = urllib.parse.quote(filter_json)

            full_url = f"/api/v3/users?filters={filter_encoded}"

            response = GetRequest(connection=self.connection,
                                context=full_url).execute()
            
            if response.get('total') == 0 and response.get('count') == 0:
                return None
                
            element = response['_embedded'].get('elements')

            return element[0].get('id')
        

        except requests.exceptions.RequestException as e:
            logger.error(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Errore nel parsing del JSON: {e}")
            return None
    

    def create_user(self, record: OpenProjectUser) -> OpenProjectUser:
    
        try:

            payload = record.to_api_payload(is_for_update=False)

            response = PostRequest(connection=self.connection,
                        context="/api/v3/users",
                        headers={"Content-Type": "application/json"},
                        json=payload).execute()
            
            
            record.id = response.get('id')
            record.created_at = response.get('createdAt')
            record.updated_at = response.get('updatedAt')
            
            logger.info(f"Utente creato con successo: {response.get('login')} (ID: {response.get('id')})")

            return record
    
                    
        except requests.exceptions.HTTPError as e:
            logging.error(f"Errore HTTP nella creazione dell'utente: {e}")
            if hasattr(e, 'response') and e.response:
                logging.error(f"Response body: {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Errore di rete nella creazione dell'utente: {e}")
            return None
        except Exception as e:
            logging.error(f"Errore generico nella creazione dell'utente: {e}")
            return None
    

    def create_project(self, record: OpenProjectProject) -> OpenProjectProject:
        
        try:

            payload = record.to_api_payload(is_for_update=False)

            response = PostRequest(connection=self.connection,
                                   context="/api/v3/projects",
                                   headers={"Content-Type": "application/json"},
                                   json=payload).execute()
            
            
            # Parse of the response to update the data
            record.id = response.get('id')
            record.created_at = response.get('createdAt')
            record.updated_at = response.get('updatedAt')
            
            logger.info(f"Progetto creato con successo: {response.get('name')} (ID: {response.get('id')})")


            if self.config.create_template is True and record.name and record.name[0].upper() == 'N':

                self.create_work_packages_template(record.id, datetime.now())


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
            
            record.updated_at = response.get('updatedAt')
            
            if record.id is None:
                record.id = response.get('id')
            
            logging.info(f"Progetto aggiornato con successo: {response.get('name')} (ID: {response.get('id')})")
                
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
        

    def update_user(self, record: OpenProjectUser, id: int) -> OpenProjectUser:

        try:

            payload = record.to_api_payload(is_for_update=True)

            response = PatchRequest(connection=self.connection,
                                   context=f"/api/v3/users/{id}",
                                   headers={"Content-Type": "application/json"},
                                   json=payload).execute()
            
            record.updated_at = response.get('updatedAt')
            
            if record.id is None:
                record.id = response.get('id')
            
            logging.info(f"Progetto aggiornato con successo: {response.get('name')} (ID: {response.get('id')})")
                
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
        
        logging.debug(f"Estratti {len(custom_fields)} custom fields dallo schema")
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
            self._project_custom_fields_cache = self.extract_custom_fields_from_schema(schema)
            
            schema = self.get_users_schema()
            if not schema:
                return False
            
            self._user_custom_fields_cache = self.extract_custom_fields_from_schema(schema)

            # Aggiorna timestamp cache
            self.cache_timestamp = datetime.now()


            logger.info(f"Custom fields aggiornati con {len(self._project_custom_fields_cache)} per progetto e {len(self._user_custom_fields_cache)} per utenti")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel caricamento della cache: {e}")
            return False
        

    # MEMBERSHIP

    def create_membership(self, user_id: int = None, project_id: int = None) -> bool:
        """
        Crea una membership per associare un utente a un progetto con ruolo predefinito.

        Args:
            user_id: ID dell'utente
            project_id: ID del progetto

        Returns:
            True se la membership è stata creata con successo o esiste già, False altrimenti
        """
        try:
            payload = {
                "_links": {
                    "principal": {"href": f"/api/v3/users/{user_id}"},
                    "roles": [{"href": "/api/v3/roles/3"}],
                    "project": {"href": f"/api/v3/projects/{project_id}"}
                }
            }

            response = PostRequest(
                connection=self.connection,
                context="/api/v3/memberships",
                headers={"Content-Type": "application/json"},
                json=payload
            ).execute()

            logger.debug(f"Membership creata con successo per user {user_id} nel progetto {project_id}")
            return True

        except requests.exceptions.HTTPError as e:
            # Check if it's a 422 error indicating membership already exists
            if hasattr(e, 'response') and e.response and e.response.status_code == 422:
                try:
                    error_body = e.response.json()
                    error_message = error_body.get('message', '')

                    # Check if the error is about membership already existing
                    if 'già stato usato' in error_message.lower() or 'already' in error_message.lower():
                        logger.debug(f"Membership già esistente per user {user_id} nel progetto {project_id} (422 error)")
                        return True  # Treat as success since membership exists
                except:
                    pass  # If we can't parse the response, fall through to error handling

            logger.error(f"Errore HTTP nella creazione della membership: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response body: {e.response.text}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Errore di rete nella creazione della membership: {e}")
            return False
        except Exception as e:
            logger.error(f"Errore generico nella creazione della membership: {e}")
            return False


    # WORK PACKAGE

    def create_work_package(self, project_id: int = None, name: str = "") -> int:

        payload = {
            "subject": name
        }

        response = PostRequest(connection=self.connection,
                                context=f"/api/v3/projects/{project_id}/work_packages",
                                headers={"Content-Type": "application/json"},
                                json=payload).execute()
        
        return response.get('id')


    def create_relations(self, first_id: int, second_id: int):

        payload = {
            "type": "follows",
            "_links": {
                "to": {
                    "href": f"/api/v3/work_packages/{first_id}"
                }
            }
        }

        response = PostRequest(connection=self.connection,
                                context=f"/api/v3/work_packages/{second_id}/relations",
                                headers={"Content-Type": "application/json"},
                                json=payload).execute()


    def create_milestone(self, date, project_id):

        payload = {
            "subject": "Fine Lavori",
            "dueDate": date,
            "_links": {
                "type": {
                    "href": "/api/v3/types/2"
                }
            }
        }

        response = PostRequest(connection=self.connection,
                                context=f"/api/v3/projects/{project_id}/work_packages",
                                headers={"Content-Type": "application/json"},
                                json=payload).execute()


    def create_work_packages_template(self, project_id: int, due_date: datetime):

        id_1 = self.create_work_package(project_id, "O.M.")

        id_2 = self.create_work_package(project_id, "STRUTTURA")

        id_3 = self.create_work_package(project_id, "PARTE MECCANICO")

        id_4 = self.create_work_package(project_id, "PARTE ELETTRICO")

        id_5 = self.create_work_package(project_id, "Attività cliente")

        self.create_relations(id_1, id_2)
        self.create_relations(id_2, id_3)
        self.create_relations(id_3, id_4)
