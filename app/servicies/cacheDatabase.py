from typing import List, Optional
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from ..config.configManager import CacheDBConfig
from contextlib import contextmanager
from typing import Generator, List
from ..models.project import CachedProject


logger = logging.getLogger(__name__)


class CacheDatabaseService:

    def __init__(self, cache_config: CacheDBConfig):
        self.config = cache_config
        self.connection = None

    @contextmanager
    def get_cache_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Context manager per connessione gestionale (readonly)"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password
            )
            conn.set_session(readonly=False)  # Sicurezza
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Errore connessione gestionale: {e}")
            raise Exception(f"Connessione gestionale fallita: {e}")
        finally:
            if conn:
                conn.close()

    def test_cache_connection(self) -> bool:
        """Testa connessione al database cache"""
        try:
            with self.get_cache_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            logger.debug("Test connessione cache: OK")
            return True
        except Exception as e:
            logger.error(f"Test connessione cache fallito: {e}")
            return False


    def insert_new_project(self, project: CachedProject) -> CachedProject:

        """
        Insert in the table **cached_projects** a row with a new project based in the input data
        
        """

        query = """

        INSERT INTO cached_projects (
            gestionale_id, openproject_id, 
            current_hash, last_sync_hash, 
            last_sync_at, sync_status, created_at, updated_at 
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        ) 
        """

        try: 
            with self.get_cache_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (
                        project.gestionale_id,
                        project.openproject_id,
                        project.current_hash,
                        project.last_sync_hash,
                        project.last_sync_at,
                        project.sync_status,
                        project.created_at,
                        project.updated_at
                    ))

                    conn.commit()

                    logger.info(f"Project {project.gestionale_id} correctly inserted in Cache")

        except psycopg2.Error as e:
            logger.error(f"Error during the creation of project {project.gestionale_id}: {e}")
            raise Exception(f"Error during the creation of project {project.gestionale_id}: {e}")
        

    def update_existing_project(self, project: CachedProject) -> CachedProject:

        """
        Update in the table **cached_projects** an existing project with the new data retrived from the Gimi database
        
        """

        query = """

        UPDATE cached_projects (

            openproject_id = %s,
            current_hash = %s,
            last_sync_hash = %s,
            last_sync_at = %s,
            sync_status = %s,
            updated_at = %s
        
        ) WHERE gestionale_id = %s

        """

        try: 
            with self.get_cache_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (
                        project.openproject_id,
                        project.current_hash,
                        project.last_sync_hash,
                        project.last_sync_at,
                        project.sync_status,
                        project.updated_at,
                        project.gestionale_id
                    ))

                    conn.commit()

                    logger.info(f"Project {project.gestionale_id} correctly updated in Cache")

        except psycopg2.Error as e:
            logger.error(f"Error during the update of project {project.gestionale_id}: {e}")
            raise Exception(f"Error during the update of project {project.gestionale_id}: {e}")
        

    