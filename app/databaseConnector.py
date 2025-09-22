#!/usr/bin/env python3

from configManager import ConfigManager
import pyodbc
import psycopg2
import logging


class DatabaseConnector:
    """Gestisce le connessioni ai database"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
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
    
    def connect_postgresql(self):
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
