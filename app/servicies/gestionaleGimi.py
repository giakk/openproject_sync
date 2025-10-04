#!/usr/bin/env python3

import pyodbc
import logging
from contextlib import contextmanager
from typing import Generator, List
from ..config.configManager import DatabaseConfig
from ..models.project import GestionaleProject, Amministratore, IndirizzoImpianto
import sys


logger = logging.getLogger(__name__)

class GestionaleService:

    """
    Service which function as interface with the database (Microsoft SQL) of the Gimi software
    """

    def __init__(self, gestionale_config: DatabaseConfig):
        self.config = gestionale_config
        self.connection = None

    def _get_connection_string(self) -> str:
        return(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.config.host},{self.config.port};"
                f"DATABASE={self.config.database};"
                f"UID={self.config.username};"
                f"PWD={self.config.password};"
                f"TrustServerCertificate=yes;"
            )
    
    @contextmanager
    def get_gestionale_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Context manager per connessione gestionale (readonly)"""
        conn = None
        try:
            
            conn = pyodbc.connect(self._get_connection_string())

            yield conn
        except pyodbc.Error as e:
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
        
    
    def load_query(self, query_path: str) -> None:
        """Carica la query SQL dal file"""
        try:
            with open(query_path, 'r', encoding='utf-8') as f:
                self._query = f.read().strip()
            logging.info(f"Query SQL caricata da {query_path}")
        except Exception as e:
            logging.error(f"Errore nel caricamento della query: {e}")
            sys.exit(1)
        

    def get_Gimi_data(self) -> List[GestionaleProject]:

        query = self.load_query(self.config.extract_projects_query)

        try:
            with self.get_gestionale_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    projects = []

                    for row in rows:
                        project = GestionaleProject(
                            NrCommessa=str(row.NrCommessa), 
                            CodImpianto=str(row.CodImpianto),
                            AperturaCommessa=row.AperturaCommessa,
                            FineLavori=row.FineLavori,
                            StatoCommessa=str(row.StatoCommessa or ""),
                            StatoFatturazione=str(row.StatoFatturaz or ""),
                            Note=str(row.Note or ""),
                            Ammin=Amministratore(
                                Name=str(row.Amm_nominativo or ""),
                                Tel=str(row.Amm_tel_ufficio or ""),
                                Cell=str(row.Amm_cellulare or ""),
                                Mail=str(row.Amm_email or ""),
                                Pac=str("")
                            ),
                            Indirizzo=IndirizzoImpianto(
                                NominativoImp=str(row.Imp_nominativo or ""),
                                IndirizzoImp=str(row.Imp_Indirizzo or ""),
                                LocazioneImp=str(row.Imp_locazione or ""),
                                CapImp=str(row.Imp_cap or ""),
                                LocalitaImp=str(row.Imp_localita or ""),
                                ProvImp=str(row.Imp_prov or "")
                            )
                        )

                        projects.append(project)
                    
                    logger.info(f"Estratti {len(projects)} progetti dal database di Gimi")
                    return projects

        except pyodbc.Error as e:
            logger.error(f"Errore durante l'esecuzione della query {self.self.config.extract_projects_query}: {e}")
            # TODO: Gestisci il raise degli errori
        