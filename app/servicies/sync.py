#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dataclasses import dataclass

from ..models.project import GestionaleProject, OpenProjectProject, CachedProject, ProjectSyncOperation
from ..models.user import GestionaleUser, OpenProjectUser, CachedUser, UserSyncOperation
from ..models.membership import MembershipTask, CachedMembership
from ..servicies.openproject import OpenProjectInterface
from ..servicies.gestionaleGimi import GestionaleService
from ..servicies.cacheDatabase import CacheDatabaseService
from ..mappers.project_mappers import ProjectMapper
from ..mappers.user_mapper import UserMapper
from ..config import ConfigManager

logger = logging.getLogger(__name__)


class MembershipStats:
    """Thread-safe statistics tracker for membership creation."""
    def __init__(self):
        self._lock = Lock()
        self.total = 0
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.errors = []

    def increment_success(self):
        with self._lock:
            self.successful += 1

    def increment_failure(self, error_msg: str):
        with self._lock:
            self.failed += 1
            self.errors.append(error_msg)

    def increment_skipped(self):
        with self._lock:
            self.skipped += 1


class SyncService:

    def __init__(self, config: ConfigManager):
        self.global_config = config
        self.gestionale_service = GestionaleService(self.global_config.gestionale_config)
        self.openproject_service = OpenProjectInterface(self.global_config.openproject_config)
        self.cache_service = CacheDatabaseService(self.global_config.cacheDB_config)
        self.project_mapper = ProjectMapper()
        self.user_mapper = UserMapper()

        self.cached_users = []
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

        self.run_user_sync()
        self.run_project_sync()
        self.run_membership_sync()


    def run_project_sync(self) -> Dict[str, Any]:
        
        # Extract entries from Gimi Database
        logger.info("Extracting project entries from Gimi Database")
        gestionale_project = self.gestionale_service.extract_Gimi_projects_entries()

        # Extract all data from Cache DB
        self.cached_projects = self.cache_service.get_projects_in_cache()
        logger.info(f"Extracted {len(self.cached_projects)} projects from cache")

        # Analyse each of the project extracted from Gimi
        project_sync_operations = self._identify_sync_operation_project(gestionale_project)

        # Execute operations
        self._execute_sync_operations(project_sync_operations)

        # Update cached project in db
        self.cache_service.update_cache_db(self.cached_projects)

    def run_user_sync(self) -> Dict[str, Any]:

        gimi_manutentori = self.gestionale_service.extract_Gimi_manutentori_entries()

        self.cached_users = self.cache_service.get_users_in_cache()
        logger.info(f"Extracted {len(self.cached_users)} users from cache")

        users_sync_operation = self._identify_sync_operation_users(gimi_manutentori)

        self._execute_sync_operations_users(users_sync_operation)

        self.cache_service.update_cache_db_for_users(self.cached_users)

    def run_membership_sync(self, max_workers: int = 10):
        """
        Parallelize membership synchronization using ThreadPoolExecutor.
        Only creates memberships that don't exist in cache.

        Args:
            max_workers: Maximum number of concurrent threads (default: 10)
        """
        import time

        # Validation
        if not self.cached_users:
            logger.warning("Nessun utente in cache. Esegui prima run_user_sync()")
            return

        if not self.cached_projects:
            logger.warning("Nessun progetto in cache. Esegui prima run_project_sync()")
            return

        # Load existing memberships from cache into a Set for O(1) lookup
        cached_memberships = self.cache_service.get_memberships_in_cache()
        existing_memberships_set = {
            (m.user_id, m.project_id) for m in cached_memberships
        }
        logger.info(f"Loaded {len(existing_memberships_set)} existing memberships from cache")

        # Prepare tasks - only for memberships that DON'T exist in cache
        tasks = []
        new_memberships = []  # To track what we'll create

        for user in self.cached_users:
            if user.openproject_id is None:
                logger.warning(f"Utente {user.gestionale_id} non ha openproject_id, skip")
                continue

            for project in self.cached_projects:
                if project.openproject_id is None:
                    logger.warning(f"Progetto {project.gestionale_id} non ha openproject_id, skip")
                    continue

                # Check if membership already exists in cache
                membership_key = (user.openproject_id, project.openproject_id)

                if membership_key in existing_memberships_set:
                    # Membership already exists, skip
                    continue

                # New membership - add to tasks
                tasks.append(MembershipTask(
                    user_id=user.openproject_id,
                    project_id=project.openproject_id
                ))

        # Initialize statistics
        stats = MembershipStats()
        stats.total = len(tasks)

        total_possible = len(self.cached_users) * len(self.cached_projects)

        logger.info(
            f"Inizio sincronizzazione membership: {len(self.cached_users)} utenti * "
            f"{len(self.cached_projects)} progetti = {total_possible} totali"
        )
        logger.info(
            f"Membership già esistenti in cache: {len(existing_memberships_set)}"
        )
        logger.info(
            f"Nuove membership da creare: {stats.total}"
        )

        if stats.total == 0:
            logger.info("Tutte le membership sono già sincronizzate. Nessuna operazione necessaria.")
            return

        logger.info(f"Utilizzo {max_workers} worker paralleli")

        start_time = time.time()

        # Execute in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._create_single_membership, task, stats): task
                for task in tasks
            }

            # Process completions with progress tracking
            completed = 0
            for future in as_completed(future_to_task):
                completed += 1

                # Log progress every 10% or every 500 memberships
                if completed % max(1, stats.total // 10) == 0 or completed % 500 == 0:
                    progress_pct = (completed / stats.total) * 100
                    logger.info(
                        f"Progresso: {completed}/{stats.total} ({progress_pct:.1f}%) - "
                        f"Successi: {stats.successful}, Falliti: {stats.failed}"
                    )

                # Check for exceptions in future execution
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Errore inatteso nel worker thread: {e}")

        # Calculate performance metrics
        end_time = time.time()
        duration = end_time - start_time
        throughput = stats.total / duration if duration > 0 else 0

        # Prepare new memberships to save in cache
        for task in tasks:
            # Only save successfully created memberships
            if task.success == True:
                new_memberships.append(CachedMembership(
                    user_id=task.user_id,
                    project_id=task.project_id,
                    sync_status='synced',
                    last_sync_at=datetime.now()
                ))

        # Update cache database with new memberships
        if new_memberships:
            logger.info(f"Salvando {len(new_memberships)} nuove membership nel cache database")
            self.cache_service.update_cache_db_for_memberships(new_memberships)

        # Final summary
        logger.info(
            f"Sincronizzazione membership completata: "
            f"{stats.successful} successi, {stats.failed} fallimenti su {stats.total} nuove membership"
        )
        logger.info(
            f"Metriche performance: Durata: {duration:.2f}s, "
            f"Throughput: {throughput:.2f} membership/sec"
        )

        # Log errors if any
        if stats.errors:
            logger.warning(f"Riscontrati {len(stats.errors)} errori:")
            for error in stats.errors[:10]:  # Log first 10 errors
                logger.warning(f"  - {error}")
            if len(stats.errors) > 10:
                logger.warning(f"  ... e altri {len(stats.errors) - 10} errori")

    def _create_single_membership(self, task: MembershipTask, stats: MembershipStats) -> None:
        """
        Worker function to create a single membership.
        Executed in parallel by ThreadPoolExecutor.
        """
        try:
            result = self.openproject_service.create_membership(
                task.user_id,
                task.project_id
            )

            if result:
                stats.increment_success()
                task.success = True
                logger.debug(
                    f"Membership created: user {task.user_id} "
                    f"-> project {task.project_id}"
                )
            else:
                task.success = False
                stats.increment_failure(
                    f"User {task.user_id} -> "
                    f"Project {task.project_id}: API returned False"
                )

        except Exception as e:
            stats.increment_failure(
                f"User {task.user_id} -> "
                f"Project {task.project_id}: {str(e)}"
            )
            logger.error(
                f"Error creating membership for user {task.user_id} "
                f"in project {task.project_id}: {e}"
            )



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

        self.cached_users = self.cache_service.get_users_in_cache()
        logger.info(f"Extracted {len(self.cached_users)} users from cache")

        self.cached_projects = self.cache_service.get_projects_in_cache()
        logger.info(f"Extracted {len(self.cached_projects)} projects from cache")


    def _identify_sync_operation_project(self, projects_list: List[GestionaleProject]) -> List[ProjectSyncOperation]:
        
        logger.info("Analizing the necessary sync operations...")

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

                        openproject_project.custom_fields_cache = self.openproject_service._project_custom_fields_cache

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
                    
                        # Create new project in cache database
                        new_cache_project = self.project_mapper.map_gestionale_to_cache(project)

                        self.cached_projects.append(new_cache_project)

                        openproject_project = self.project_mapper.map_gestionale_to_openproject(project)

                        openproject_project.custom_fields_cache = self.openproject_service._project_custom_fields_cache

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

        return operations
    
    def _identify_sync_operation_users(self, users_list: List[GestionaleUser]) -> List[UserSyncOperation]:

        logger.info("Analizing the necessary sync operations...")

        operations = []

        for user in users_list:

            try:

                cached_user = next((cached_u for cached_u in self.cached_users
                                       if str(cached_u.gestionale_id).strip() == str(user.GimiId).strip()), None)
                
                if cached_user:

                    if self.user_mapper.update_gestionale_user_to_cache(user, cached_user):

                        openproject_user = self.user_mapper.map_gestionale_to_openproject_user(user)

                        openproject_user.custom_fields_cache = self.openproject_service._user_custom_fields_cache

                        operation = UserSyncOperation(
                            operation_type="update",
                            gestionale_user=user,
                            openproject_user=openproject_user,
                            cached_user= cached_user
                        )

                        operations.append(operation)


                else:

                    new_cache_user = self.user_mapper.map_gestionale_to_cache_user((user))

                    self.cached_users.append(new_cache_user)

                    openproject_user = self.user_mapper.map_gestionale_to_openproject_user(user)

                    #TODO: handle custom fields
                    openproject_user.custom_fields_cache = self.openproject_service._user_custom_fields_cache

                    operation = UserSyncOperation(
                        operation_type="create",
                        gestionale_user=user,
                        openproject_user=openproject_user,
                        cached_user= new_cache_user
                    )

                    operations.append(operation)

            except Exception as e:
                logger.error(f"Error while handling user {user.GimiId}: {e}")
                continue
            
        return operations


           
    def _execute_sync_operations(self, operations: List[ProjectSyncOperation]):

        logger.info(f"Executing {len(operations)} operations....")

        create_operations = [op for op in operations if op.operation_type == "create"]
        update_operations = [op for op in operations if op.operation_type == "update"]

        self._execute_create_operations(create_operations)
        self._execute_update_operations(update_operations)


    def _execute_sync_operations_users(self, operations: List[UserSyncOperation]):

        logger.info(f"Executing {len(operations)} operations....")

        create_operations = [op for op in operations if op.operation_type == "create"]
        update_operations = [op for op in operations if op.operation_type == "update"]

        self._execute_create_operations_users(create_operations)
        self._execute_update_operations_users(update_operations)


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

    
    def _execute_create_operations_users(self, operations: List[UserSyncOperation]):

        if not operations:
            return

        logger.info(f"Creation of {len(operations)} users...")

        for operation in operations:

            try:

                result = self._create_single_user(operation)
                self._handle_operation_success_users(operation, result)

            except Exception as e:

                self._handle_operation_error_users(operation, e)
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

    def _execute_update_operations_users(self, operations: List[UserSyncOperation]):

        if not operations:
            return

        logger.info(f"Update of {len(operations)} users...")

        for operation in operations:

            try:

                result = self._update_single_user(operation)
                self._handle_operation_success_users(operation, result)

            except Exception as e:

                self._handle_operation_error_users(operation, e)
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


    def _create_single_user(self, operation: UserSyncOperation) -> OpenProjectUser:

        try:

            id = self.openproject_service.find_user(operation.cached_user.email)

            if id is None:

                return self.openproject_service.create_user(operation.openproject_user)

            else:

                return self.openproject_service.update_user(operation.openproject_user, id)

        except Exception as e:

            logger.error(f"Error during OP creation of users {operation.gestionale_user.GimiId}: {e}")
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

    
    def _update_single_user(self, operation: UserSyncOperation) -> OpenProjectUser:

        try:

            return self.openproject_service.update_user(
                operation.openproject_user,
                operation.cached_user.openproject_id
            )
        
        except Exception as e:

            logger.error(f"Error during OP update of {operation.gestionale_user.GimiId}: {e}")
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
            
            logger.debug(f"Successo {operation.operation_type} progetto: {operation.cached_project.gestionale_id}")
            
        except Exception as e:
            logger.error(f"Errore gestione successo per progetto {operation.cached_project.gestionale_id}: {e}")
    

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


    def _handle_operation_success_users(self, operation: UserSyncOperation, result: OpenProjectUser):
        """Handle operation success"""
        try:
            # Update cache information
            self.user_mapper.mark_sync_success_user(
                operation.cached_user,
                result
            )
            
            # Update statistics
            self.stats['processed'] += 1
            
            if operation.operation_type == "create":
                self.stats['created'] += 1
            else:
                self.stats['updated'] += 1
            
            logger.debug(f"Successo {operation.operation_type} utente: {operation.cached_user.gestionale_id}")
            
        except Exception as e:
            logger.error(f"Errore gestione successo per utente {operation.cached_user.gestionale_id}: {e}")
    

    def _handle_operation_error_users(self, operation: UserSyncOperation, error: Exception):
        """Gestisce errore operazione"""
        try:
            # Aggiorna cache con errore
            error_message = str(error)[:500]  # Limita lunghezza messaggio
            self.user_mapper.mark_sync_failed_user(
                operation.cached_user
            )
            
            # Aggiorna statistiche
            self.stats['failed'] += 1
            self.stats['processed'] += 1
            self.stats['errors'].append(f"Utente {operation.cached_user.gestionale_id}: {error_message}")
            
            logger.error(f"Error {operation.operation_type} for project {operation.cached_user.gestionale_id}: {error}")
            
        except Exception as e:
            logger.error(f"Error while failed handling for project {operation.cached_user.gestionale_id}: {e}")