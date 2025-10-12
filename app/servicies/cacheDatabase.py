from typing import List, Optional
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from ..config.ConfigManager import CacheDBConfig
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
        

    def get_projects_in_cache(self) -> List[CachedProject]:

        query = """
        SELECT *
        FROM cached_projects
        """
        
        projects = []
    
        try: 
            with self.get_cache_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    for row in rows:

                        projects.append(self._row_to_cached_project(row))

                    return projects
                

        except psycopg2.Error as e:
            logger.error(f"Error during data recover of all projecta: {e}")
            raise Exception(f"Error during data recover of all projects: {e}")
        

    def update_existing_project(self, project: CachedProject) -> CachedProject:

        """
        Update in the table **cached_projects** an existing project with the new data retrived from the Gimi database
        
        """

        query = """

        UPDATE cached_projects SET
            openproject_id = %s,
            current_hash = %s,
            last_sync_hash = %s,
            last_sync_at = %s,
            sync_status = %s,
            updated_at = %s
        WHERE gestionale_id = %s

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
        

    def get_project_by_gestionale_id(self, gestionale_id: str) -> Optional[CachedProject]:

        query = """

        SELECT *
        FROM cached_projects
        WHERE gestionale_id = %s
            
        """
    
        try: 
            with self.get_cache_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, (gestionale_id,))
                    row = cursor.fetchone()

                    if not row:
                        return None
                    
                    return self._row_to_cached_project(row)
                

        except psycopg2.Error as e:
            logger.error(f"Error during data recover of project {gestionale_id}: {e}")
            raise Exception(f"Error during data recover of project {gestionale_id}: {e}")
        

    def update_cache_db(self, projects: List[CachedProject]):

        """
        Inserisce o aggiorna tutti i progetti nel database con una singola operazione bulk.
        Usa ON CONFLICT per gestire gli upsert in modo efficiente.
        """
        if not projects:
            return
        
        # Prepara i dati da inserire
        values = [
            (
                p.gestionale_id,
                p.openproject_id,
                p.current_hash,
                p.last_sync_hash,
                p.last_sync_at,
                p.sync_status,
                p.created_at,
                p.updated_at
            )
            for p in projects
        ]
        
        # Query con ON CONFLICT per upsert
        query = """
            INSERT INTO cached_projects (
                gestionale_id,
                openproject_id,
                current_hash,
                last_sync_hash,
                last_sync_at,
                sync_status,
                created_at,
                updated_at
            ) VALUES %s
            ON CONFLICT (gestionale_id) 
            DO UPDATE SET
                openproject_id = EXCLUDED.openproject_id,
                current_hash = EXCLUDED.current_hash,
                last_sync_hash = EXCLUDED.last_sync_hash,
                last_sync_at = EXCLUDED.last_sync_at,
                sync_status = EXCLUDED.sync_status,
                updated_at = EXCLUDED.updated_at
        """
        
        try: 
            with self.get_cache_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, values)
                    
                    conn.commit()
                    
                    logger.info(f"✓ Updated {len(projects)} project in cache db")
        
        except psycopg2.Error as e:
            print(f"✗ Error during cache update: {e}")
            raise


    def _row_to_cached_project(self, row) -> CachedProject:

        return CachedProject(
            gestionale_id=row['gestionale_id'],
            openproject_id=row['openproject_id'],
            current_hash=row['current_hash'],
            last_sync_hash=row['last_sync_hash'],
            last_sync_at=row['last_sync_at'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            sync_status=row['sync_status']
        )
    