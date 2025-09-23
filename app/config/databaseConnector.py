#!/usr/bin/env python3

from app.config.configManager import ConfigManager
import pyodbc
import psycopg2
import logging
from contextlib import contextmanager
from typing import Generator
from configManager import CacheDBConfig, DatabaseConfig

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Gestisce le connessioni ai database"""
    
    def __init__(self, cache_config: CacheDBConfig, gestionale_config: DatabaseConfig):
        self.cache_config = cache_config
        self.gestionale_config = gestionale_config
        self._sql_conn = None
        self._pg_pool = None
    
    def connect_sql_server(self) -> pyodbc.Connection:
        """Connessione a SQL Server"""
        if self._sql_conn is None or not self._test_sql_connection():
            sql_config = self.config.sql_server_config
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={sql_config['host']},{sql_config['port']};"
                f"DATABASE={sql_config['database']};"
                f"UID={sql_config['username']};"
                f"PWD={sql_config['password']};"
                f"TrustServerCertificate=yes;"
            )
            
            try:
                self._sql_conn = pyodbc.connect(conn_str)
                self._sql_conn.timeout = sql_config.get('timeout', 30)
                logging.info("Connessione a SQL Server stabilita")
            except Exception as e:
                logging.error(f"Errore connessione SQL Server: {e}")
                raise
        
        return self._sql_conn
    
    def connect_postgresql(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Connessione pool a PostgreSQL"""
        if self._pg_pool is None:
            pg_config = self.config.postgresql_config
            
            try:
                self._pg_pool =  psycopg2.connect(
                    host=pg_config['host'],
                    port=pg_config['port'],
                    database=pg_config['database'],
                    user=pg_config['username'],
                    password=pg_config['password']
                )
                # logging.info("Pool PostgreSQL creato")
                logging.info("Pool PostgreSQL creato")
            except Exception as e:
                logging.error(f"Errore connessione PostgreSQL: {e}")
                raise
        
        return self._pg_pool
    
    def _test_sql_connection(self) -> bool:
        """Test connessione SQL Server"""
        try:
            cursor = self._sql_conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except:
            return False
    
    def close_connections(self):
        """Chiude tutte le connessioni in modo sicuro"""
        if self._sql_conn:
            try:
                self._sql_conn.close()
            except Exception as e:
                print(f"Errore chiusura SQL Server: {e}")
            finally:
                self._sql_conn = None
        
        # if self._pg_pool:
        #     try:
        #         # Attendi che tutte le connessioni attive si chiudano
        #         io.wait_for(self._pg_pool.close(), timeout=5.0)
        #         print("Pool PostgreSQL chiuso correttamente")
        #     except io.TimeoutError:
        #         print("Timeout nella chiusura del pool PostgreSQL")
        #     except Exception as e:
        #         print(f"Errore chiusura pool PostgreSQL: {e}")
        #     finally:
        #         self._pg_pool = None
        
    # DA QUI NUOVO   
        
    @contextmanager
    def get_gestionale_connection(self) -> pyodbc.Connection:
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