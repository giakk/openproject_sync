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
        self.gestionale_service = GestionaleService()
        self.openproject_service = OpenProjectInterface()
        self.cache_service = CacheDatabaseService()
        self.project_mapper = ProjectMapper()

        self.global_config = config

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

        # 3. Update Cache Databare with data just retrived from Gimi Database
        self._update_cache_from_gestionale(gestionale_project)








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


    def _update_cache_from_gestionale(self, projects_list: List[GestionaleProject]):

        logger.info("Updating Cache Database...")

        for project in projects_list:
            try:

                cached_project = self.cache_service.get_project_by_gestionale_id(project.NrCommessa)

                if cached_project:
                    # project exist, will be updated
                    updated_cached = self.project_mapper.update_gestionale_to_cache(project, cached_project)
                    self.cache_service.update_existing_project(updated_cached)
                else:
                    # create new project in cache database
                    new_cached = self.project_mapper.map_gestionale_to_cache(project)
                    self.cache_service.insert_new_project(new_cached)
            
            except Exception as e:
                logger.error(f"Error during cache update for project {project.NrCommessa}: {e}")
                continue

        


