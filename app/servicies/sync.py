#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import List, Dict, Any

from ..models.project import GestionaleProject, OpenProjectProject, CachedProject, ProjectSyncOperation
from ..servicies.openproject import OpenProjectInterface
from ..servicies.gestionaleGimi import GestionaleService
from ..servicies.cacheDatabase import CacheDatabaseService
from ..mappers.project_mappers import ProjectMapper
from ..config import ConfigManager

logger = logging.getLogger(__name__)

class SyncService:

    def __init__(self, config: ConfigManager):
        self.global_config = config
        self.gestionale_service = GestionaleService(self.global_config.gestionale_config)
        self.openproject_service = OpenProjectInterface(self.global_config.openproject_config)
        self.cache_service = CacheDatabaseService(self.global_config.cacheDB_config)
        self.project_mapper = ProjectMapper()

        self.cached_projects = []

        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_projects': 0,
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'created': 0,
            'updated': 0,
            'errors': []
        }


# Main method
    def run_full_sync(self) -> Dict[str, Any]:

        # 1. Test the connections to all the databases
        self._test_connections()

        # 2. Extract entries from Gimi Database
        logger.info("Extracting project entries from Gimi Database")
        gestionale_project = self.gestionale_service.extract_Gimi_projects_entries()
        self.stats['total_projects'] = len(gestionale_project)

        # 3. Extract all data from Cache DB
        self._extract_cache_data()

        # 4. Analyse each of the project extracted from Gimi
        sync_operations = self._identify_sync_operation(gestionale_project)

        # 5. Execute operations
        self._execute_sync_operations(sync_operations)

        #6. Update cached project in db
        self.cache_service.update_cache_db(self.cached_projects)









# Auxiliary Functions

    def _test_connections(self):
        logger.debug("Databases connection test...")

        if not self.gestionale_service.test_gestionale_connection():
            raise Exception("Connection to Gimi Database failed")
        
        if not self.openproject_service.test_connection():
            raise Exception("Connection to OpenProject API failed")
        
        if not self.cache_service.test_cache_connection():
            raise Exception("Connection to Cache Database failed")
        
        logger.debug("All connection OK")



    def _extract_cache_data(self):

        logger.info("Extracting data from Cache database...")

        self.cached_projects = self.cache_service.get_projects_in_cache()

        logger.info(f"Extracted {len(self.cached_projects)} projects from cache")



    def _identify_sync_operation(self, projects_list: List[GestionaleProject]) -> List[ProjectSyncOperation]:
        
        logger.info("Analizing the necessary sync operations")

        operations = []

        for project in projects_list:

            try:

                # Search for the project into the cache
                cached_project = next((gimi_project for gimi_project in self.cached_projects
                                       if gimi_project.gestionale_id == project.get_id()), None)

                if cached_project:
                    # project exist. If needed, will be updated
                    if self.project_mapper.update_gestionale_to_cache(project, cached_project):

                        openproject_project = self.project_mapper.map_gestionale_to_openproject(project)

                        openproject_project.custom_fields_cache = self.openproject_service._custom_fields_cache

                        operation = ProjectSyncOperation(
                            operation_type="update",
                            gestionale_project=project,
                            openproject_project=openproject_project,
                            cached_project=cached_project
                        )

                        operations.append(operation)

                else:
                    # project not existing

                    if project.StatoCommessa != 'Chiusa': 
                    # create new project in cache database
                    
                        new_cache_project = self.project_mapper.map_gestionale_to_cache(project)

                        self.cached_projects.append(new_cache_project)

                        openproject_project = self.project_mapper.map_gestionale_to_openproject(project)

                        openproject_project.custom_fields_cache = self.openproject_service._custom_fields_cache

                        operation = ProjectSyncOperation(
                            operation_type="create",
                            gestionale_project=project,
                            openproject_project=openproject_project,
                            cached_project=new_cache_project
                        )

                        operations.append(operation)

                

            except Exception as e:
                logger.error(f"Error while handling project {project.NrCommessa}: {e}")
                continue

        logger.info("Cache correctly updated")

        return operations

           
    def _execute_sync_operations(self, operations: List[ProjectSyncOperation]):

        logger.info(f"Executing {len(operations)} operations....")

        create_operations = [op for op in operations if op.operation_type == "create"]
        update_operations = [op for op in operations if op.operation_type == "update"]

        self._execute_create_operations(create_operations)
        self._execute_update_operations(update_operations)


    def _execute_create_operations(self, operations: List[ProjectSyncOperation]):

        if not operations:
            return

        logger.info(f"Creation of {len(operations)} projects...")

        for operation in operations:

            try:

                result = self._create_single_project(operation)
                self._handle_operation_success(operation, result)

            except Exception as e:

                self._handle_operation_error(operation, e)
                continue

    
    def _execute_update_operations(self, operations: List[ProjectSyncOperation]):

        if not operations:
            return

        logger.info(f"Update of {len(operations)} projects...")

        for operation in operations:

            try:

                result = self._update_single_project(operation)
                self._handle_operation_success(operation, result)

            except Exception as e:

                self._handle_operation_error(operation, e)
                continue

    
    def _create_single_project(self, operation: ProjectSyncOperation) -> OpenProjectProject:

        """
        This function handle the creation of a single Project.
        1. Verify if a project with the same identifier exist
        2. If it exist, that it update the data based on the values it had
        3. If do not exist, create a new project on OP and saves the openproject_id
        """

        try:

            id = self.openproject_service.find_project(operation.openproject_project.identifier)

            if id is None:

                return self.openproject_service.create_project(operation.openproject_project)
            
            else:

                return self.openproject_service.update_project(operation.openproject_project, id)
            
        except Exception as e:

            logger.error(f"Error during OP creation of {operation.gestionale_project.NrCommessa}: {e}")
            raise


    def _update_single_project(self, operation: ProjectSyncOperation) -> OpenProjectProject:

        """
        This function update the OP project based on the new values.
        It will use the project identifier already saved in cache
        """

        try:

            return self.openproject_service.update_project(
                operation.openproject_project,
                operation.cached_project.openproject_id
            )
            
        except Exception as e:

            logger.error(f"Error during OP update of {operation.gestionale_project.NrCommessa}: {e}")
            raise

    
    def _handle_operation_success(self, operation: ProjectSyncOperation, result: OpenProjectProject):
        """Handle operation success"""
        try:
            # Update cache information
            self.project_mapper.mark_sync_success(
                operation.cached_project,
                result
            )
            
            # Update statistics
            self.stats['processed'] += 1
            
            if operation.operation_type == "create":
                self.stats['created'] += 1
            else:
                self.stats['updated'] += 1
            
            logger.debug(f"Successo {operation.operation_type} utente: {operation.cached_project.gestionale_id}")
            
        except Exception as e:
            logger.error(f"Errore gestione successo per utente {operation.cached_project.gestionale_id}: {e}")
    

    def _handle_operation_error(self, operation: ProjectSyncOperation, error: Exception):
        """Gestisce errore operazione"""
        try:
            # Aggiorna cache con errore
            error_message = str(error)[:500]  # Limita lunghezza messaggio
            self.project_mapper.mark_sync_failed(
                operation.cached_project
            )
            
            # Aggiorna statistiche
            self.stats['failed'] += 1
            self.stats['processed'] += 1
            self.stats['errors'].append(f"Utente {operation.cached_project.gestionale_id}: {error_message}")
            
            logger.error(f"Error {operation.operation_type} for project {operation.cached_project.gestionale_id}: {error}")
            
        except Exception as e:
            logger.error(f"Error while failed handling for project {operation.cached_project.gestionale_id}: {e}")

