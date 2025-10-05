#!/usr/bin/env python3


import pyodbc
import psycopg2
import logging
from contextlib import contextmanager
from typing import Generator
from .ConfigManager import CacheDBConfig, DatabaseConfig
import sys

### è stato rimpiazzado da diverse classi in servicies... andrà in disuso


logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Gestisce le connessioni ai database"""
    
    def __init__(self, cache_config: CacheDBConfig, gestionale_config: DatabaseConfig):
        self.cache_config = cache_config
        self.gestionale_config = gestionale_config
        self._sql_conn = None
        self._pg_pool = None
        self._query = None
           
        
    @contextmanager
    def get_gestionale_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Context manager per connessione gestionale (readonly)"""
        conn = None
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.gestionale_config.host},{self.gestionale_config.port};"
                f"DATABASE={self.gestionale_config.database};"
                f"UID={self.gestionale_config.username};"
                f"PWD={self.gestionale_config.password};"
                f"TrustServerCertificate=yes;"
            )
            
            conn = pyodbc.connect(conn_str)
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Errore connessione gestionale: {e}")
            raise Exception(f"Connessione gestionale fallita: {e}")
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cache_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Context manager per connessione gestionale (readonly)"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.cache_config.host,
                port=self.cache_config.port,
                database=self.cache_config.database,
                user=self.cache_config.username,
                password=self.cache_config.password
            )
            conn.set_session(readonly=False)  # Sicurezza
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Errore connessione gestionale: {e}")
            raise Exception(f"Connessione gestionale fallita: {e}")
        finally:
            if conn:
                conn.close()
                               
    def test_gestionale_connection(self) -> bool:
        """Testa connessione al database gestionale"""
        try:
            with self.get_gestionale_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            logger.debug("Test connessione gestionale: OK")
            return True
        except Exception as e:
            logger.error(f"Test connessione gestionale fallito: {e}")
            return False
    
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
    
    def test_all_connections(self) -> dict:
        """Testa tutte le connessioni"""
        results = {
            'gestionale': self.test_gestionale_connection(),
            'cache': self.test_cache_connection()
        }
        
        all_ok = all(results.values())
        logger.info(f"Test connessioni - Gestionale: {results['gestionale']}, "
                   f"Cache: {results['cache']}, Tutto OK: {all_ok}")
        
        return results
    
    def initialize_cache_database(self) -> bool:

        try:

            # query = self.load_query(self.cache_config.query_path)

            with self.get_cache_connection() as conn:
                with conn.cursor() as cursor:

                    # for statement in query.split(";"):
                    #     stmt = statement.strip()
                    #     if stmt:  # salta righe vuote
                    #         cursor.execute(stmt + ";")


                    cursor.execute("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_name = 'cached_users' AND table_schema = 'public'
                    """)
                    
                    table_exists = cursor.fetchone()[0] > 0
                    
                    if table_exists:
                        logger.info("Database cache già inizializzato")
                        return True
                    
                    # Se non esistono, crea schema
                    logger.info("Inizializzazione database cache...")
                    
                    # Qui potresti eseguire lo script SQL di migrazione
                    # Per semplicità, assumiamo che il DB sia già stato inizializzato manualmente
                    logger.warning("Schema database cache non trovato. "
                                 "Eseguire manualmente migrations/init_sync_db.sql")
                    
                    return False

                logger.info("Migrazione database cache eseguita con successo.")
                return True

        except Exception as e:
            logger.error(f"Errore inizializzazione database cache: {e}")
            return False