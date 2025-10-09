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

        self.cached_projects = [CachedProject]

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



    def _identify_sync_operation(self, projects_list: List[GestionaleProject]):
        
        logger.info("Analizing the necessary sync operations")

        operations = [ProjectSyncOperation]


        for project in projects_list:

            try:

                # Search for the project into the cache
                cached_project = next((gimi_project for gimi_project in self.cached_projects
                                       if gimi_project.gestionale_id == project.NrCommessa), None)

                if cached_project:
                    # project exist. If needed, will be updated
                    if self.project_mapper.update_gestionale_to_cache(project, cached_project):

                        openproject_project = self.project_mapper.map_gestionale_to_openproject(project)

                        operation = ProjectSyncOperation(
                            operation_type="update",
                            gestionale_project=project,
                            openproject_project=openproject_project,
                            cached_projec=cached_project
                        )

                        operations.append(operation)

                else:
                    # project not existing

                    if project.StatoCommessa != 'Chiusa': 
                    # create new project in cache database
                    
                        new_cache_project = self.project_mapper.map_gestionale_to_cache(project)

                        self.cached_projects.append(new_cache_project)

                        openproject_project = self.project_mapper.map_gestionale_to_openproject(project)

                        operation = ProjectSyncOperation(
                            operation_type="create",
                            gestionale_project=project,
                            openproject_project=openproject_project,
                            cached_projec=new_cache_project
                        )

                        operations.append(operation)

            except Exception as e:
                logger.error(f"Error while handling project {project.NrCommessa}: {e}")
                continue

        logger.info("Cache correctly updated")


                




        


