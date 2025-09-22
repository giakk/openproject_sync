#!/usr/bin/env python3

import io
import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import aiofiles
import aiohttp
import requests
from requests.auth import HTTPBasicAuth
import asyncpg
import pyodbc
import yaml
from dataclasses import dataclass
from configManager import ConfigManager
from databaseConnector import DatabaseConnector

@dataclass
class IndirizzoImpianto:
    NominativoImp: str
    IndirizzoImp: str
    LocazioneImp: str 
    CapImp: str
    LocalitaImp: str
    ProvImp: str

    def to_string(self) -> str:
        """Concatena tutte le variabili della classe come stringa per l'hash"""
        return f"{self.NominativoImp}_{self.IndirizzoImp}_{self.LocazioneImp}_{self.CapImp}_{self.LocalitaImp}_{self.ProvImp}"


@dataclass    
class User:
    UserName: str
    UserTel: str
    UserCell: str
    UserMail: str
    UserPac: str

    def to_string(self) -> str:
        """Concatena tutte le variabili della classe come stringa per l'hash"""
        return f"{self.UserName}_{self.UserTel}_{self.UserCell}_{self.UserMail}_{self.UserPac}"


@dataclass    
class Commessa:
    NrCommessa: str
    CodImpianto: str
    AperturaCommessa: datetime
    FineLavori: datetime
    StatoCommessa: str
    StatoFatturazione: str
    Note: str

    def get_identifier(self) -> str:
        return self.NrCommessa.lower().replace('/', '-')

    def to_string(self) -> str:
        """Concatena tutte le variabili della classe come stringa per l'hash"""
        # Converte datetime in stringa ISO format per consistency
        apertura_str = self.AperturaCommessa.isoformat() if self.AperturaCommessa else ""
        fine_str = self.FineLavori.isoformat() if self.FineLavori else ""
        
        return f"{self.NrCommessa}_{self.CodImpianto}_{apertura_str}_{fine_str}_{self.StatoCommessa}_{self.StatoFatturazione}_{self.Note}"


@dataclass
class SyncRecord:
    """Rappresenta un record da sincronizzare"""
    commessa: Commessa
    indirizzo: IndirizzoImpianto
    amministratoreImpianto: User
    data_modifica: datetime
    project_hash: str
    admin_hash: str
    




class DataExtractor:
    """Estrae dati dal database SQL Server"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector
        self._query = None
        self.load_query()
    
    def load_query(self) -> None:
        """Carica la query SQL dal file"""
        query_file = self.db_connector.config.sql_server_config['query_path']
        try:
            with open(query_file, 'r', encoding='utf-8') as f:
                self._query = f.read().strip()
            logging.info(f"Query SQL caricata da {query_file}")
        except Exception as e:
            logging.error(f"Errore nel caricamento della query: {e}")
            sys.exit(1)
    
    def extract_records(self) -> List[SyncRecord]:
        """Estrae i record dalla vista SQL Server"""
        conn =  self.db_connector.connect_sql_server()
        records = []
        
        try:
            cursor = conn.cursor()
            
            cursor.execute(self._query)
            
            rows = cursor.fetchall()
            
            for row in rows:
                # Crea oggetti dalle classi definite
                commessa = Commessa(
                    NrCommessa=str(row.NrCommessa), 
                    CodImpianto=str(row.CodImpianto),
                    AperturaCommessa=row.AperturaCommessa,
                    FineLavori=row.FineLavori,
                    StatoCommessa=str(row.StatoCommessa),
                    StatoFatturazione=str(row.StatoFatturaz),
                    Note=str(row.Note or "")
                )
                
                indirizzo = IndirizzoImpianto(
                    NominativoImp=str(r""),
                    IndirizzoImp=str(row.Imp_Indirizzo or ""),
                    LocazioneImp=str(""),
                    CapImp=str(""),
                    LocalitaImp=str(row.Imp_localita or ""),
                    ProvImp=str(row.Imp_prov or "")
                )
                
                amministratore = User(
                    UserName=str(row.Amm_nominativo or ""),
                    UserTel=str(""),
                    UserCell=str(row.Amm_cellulare or ""),
                    UserMail=str(row.Amm_email or ""),
                    UserPac=str("")
                )
                
                project_hash_data = f"{commessa.to_string()}|{indirizzo.to_string()}"
                admin_hash_data = f"{amministratore.to_string()}"
                
                # Crea il record finale
                record = SyncRecord(
                    commessa=commessa,
                    indirizzo=indirizzo,
                    amministratoreImpianto=amministratore,
                    data_modifica=row.data_modifica if hasattr(row, 'data_modifica') else datetime.now(),
                    project_hash=hashlib.md5(project_hash_data.encode()).hexdigest(),
                    admin_hash=hashlib.md5(admin_hash_data.encode()).hexdigest()
                )
                records.append(record)
            
            logging.info(f"Estratti {len(records)} record da SQL Server")
            print(f"Estratti {len(records)} record da SQL Server")
            
        except Exception as e:
            logging.error(f"Errore nell'estrazione dati: {e}")
            raise
        
        return records
    

class SyncStateManager:
    """Gestisce lo stato della sincronizzazione"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector
        self.project_table = "sync_commesse"
        # self.load_query()


    def load_query(self) -> None:
        """Carica la query SQL dal file"""
        query_file = self.db_connector.config.postgresql_config['query_path']
        try:
            with open(query_file, 'r', encoding='utf-8') as f:
                self._query = f.read().strip()
            logging.info(f"Query SQL caricata da {query_file}")
        except Exception as e:
            logging.error(f"Errore nel caricamento della query: {e}")
            sys.exit(1)
       
    def init_tables(self):
        """Inizializza la tabella di stato"""
        pool =  self.db_connector.connect_postgresql()
        
        with pool.acquire() as conn:
             conn.execute(self._query)
    
    def get_last_sync_time(self) -> Optional[datetime]:
        """Ottiene l'ultimo timestamp di sincronizzazione"""
        pool =  self.db_connector.connect_postgresql()
        
        with pool.acquire() as conn:
            result =  conn.fetchval(
                f"SELECT MAX(last_sync) FROM {self.project_table}"
            )
        
        return result

    
    def get_commessa_sync_state(self, commessa_id: str) -> Optional[Dict]:
        """Ottiene lo stato di sincronizzazione per una commessa"""


        try:    
            conn = self.db_connector.connect_postgresql()
            
            # Usa %s invece di $1 per parametri con psycopg2
            query = f"""SELECT NrCommessa, project_hash, admin_hash, last_sync 
                       FROM {self.project_table} 
                       WHERE NrCommessa = %s"""
            
            with conn.cursor() as cursor:
                cursor.execute(query, (commessa_id,))
                row = cursor.fetchone()
                
                if row:
                    return {
                        'NrCommessa': row[0],
                        'project_hash': row[1],
                        'admin_hash': row[2],
                        'last_sync': row[3]
                    }
                return None
                
        except Exception as e:
            logging.error(f"Errore nel recupero stato commessa {commessa_id}: {e}")
            return None
    
    def update_sync_state(self, record: SyncRecord):
        """Aggiorna lo stato di sincronizzazione"""
        pool =  self.db_connector.connect_postgresql()
        
        with pool.acquire() as conn:
             conn.execute(
                f"""INSERT INTO {self.project_table} 
                    (NrCommessa, project_hash, admin_hash, last_sync)
                    VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                    ON CONFLICT (NrCommessa) 
                    DO UPDATE SET 
                        project_hash = $2,
                        admin_hash = $3,
                        last_sync = CURRENT_TIMESTAMP""",
                record.commessa.NrCommessa, record.project_hash, record.admin_hash
            )


class SyncService:
    """Servizio principale di sincronizzazione"""
    
    def __init__(self, config_path: str = "/home/riccardo/syncer/config/config.yaml"):
        self.config = ConfigManager(config_path)
        self.db_connector = DatabaseConnector(self.config)
        self.data_extractor = DataExtractor(self.db_connector)
        self.openproject_api = OpenProjectAPI(self.config)
        self.state_manager = SyncStateManager(self.db_connector)
        
        self._running = False
        self._sync_in_progress = False
        
        # Setup logging
        self._setup_logging()
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _setup_logging(self):
        """Configura il logging"""
        log_config = self.config.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        log_file = log_config.get('file', 'sync.log')
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        # Riduci verbosità di aiohttp
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
    
    def _signal_handler(self, signum, frame):
        """Gestisce i segnali di terminazione"""
        logging.info(f"Ricevuto segnale {signum}, arresto in corso...")
        self._running = False

    def sync_data(self) -> Dict[str, int]:
        """Esegue la sincronizzazione dei dati"""
        if self._sync_in_progress:
            logging.warning("Sincronizzazione già in corso, salto")
            return {"status": "skipped", "reason": "sync_in_progress"}
        
        self._sync_in_progress = True
        stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}
        
        try:
            logging.info("Inizio sincronizzazione")
            start_time = time.time()
            
            # Estrai dati da SQL Server
            records =  self.data_extractor.extract_records()
            
            # Processa ogni record
            for record in records:
                try:
                     self._process_record(record, stats)
                except Exception as e:
                    logging.error(f"Errore processing record {record.commessa.NrCommessa}: {e}")
                    stats["errors"] += 1
            
            duration = time.time() - start_time
            logging.info(
                f"Sincronizzazione completata in {duration:.2f}s - "
                f"Created: {stats['created']}, Updated: {stats['updated']}, "
                f"Skipped: {stats['skipped']}, Errors: {stats['errors']}"
            )
            
        except Exception as e:
            logging.error(f"Errore durante la sincronizzazione: {e}")
            stats["errors"] += 1
        finally:
            self._sync_in_progress = False
        
        return stats
    
    def _process_record(self, record: SyncRecord, stats: Dict[str, int]):
        """Processa un singolo record"""
        # Controlla se il progetto esiste già
        sync_state =  self.state_manager.get_commessa_sync_state(record.commessa.NrCommessa)
        
        if sync_state:
            # Progetto esistente - controlla se è cambiato
            if sync_state['project_hash'] == record.project_hash:
                stats["skipped"] += 1
                return
            
            # Aggiorna progetto esistente
            # success =  self.openproject_api.update_project(
            #     sync_state['project_identifier'], record
            # )

            success = True
            
            if success:
                self.state_manager.update_sync_state(record)
                stats["updated"] += 1
            else:
                stats["errors"] += 1
        
        else:
            # Nuovo progetto
            # # Controlla se il progetto esiste in OpenProject ma non nel nostro stato
            # existing_project =  self.openproject_api.get_project_by_identifier(
            #     sync_state["openproject_id"]
            # )
            
            # if existing_project:
            #     # Progetto esiste, aggiorna stato e contenuto
            #     success =  self.openproject_api.update_project(
            #         sync_state["openproject_id"], record
            #     )
            #     if success:
            #          self.state_manager.update_sync_state(record, sync_state["openproject_id"])
            #         stats["updated"] += 1
            #     else:
            #         stats["errors"] += 1
            # else:
                # Crea nuovo progetto
            success =  self.openproject_api.create_project(record)
            if success:
                self.state_manager.update_sync_state(record, sync_state["openproject_id"])
                stats["created"] += 1
            else:
                stats["errors"] += 1
    
    def initialize(self):
        """Inizializza il servizio"""
        logging.info("Inizializzazione servizio di sincronizzazione")
        #  self.state_manager.init_tables()
        # Da inserire una verifica che sia connesso al db Microsoft
        logging.info("Servizio inizializzato")


    def run_scheduler(self):
        """Esegue il loop di sincronizzazione schedulata"""
        self._running = True
        interval = self.config.sync_config.get('interval_minutes', 3) * 60
        
        logging.info(f"Avvio scheduler con intervallo di {interval/60} minuti")
        
        while self._running:
            try:
                self.sync_data()
                
                # Attendi prossima esecuzione o interruzione
                for _ in range(int(interval)):
                    if not self._running:
                        break
                    time.sleep(1)  # CORREZIONE: time.sleep invece di io.sleep
                    
            except Exception as e:
                logging.error(f"Errore nel loop principale: {e}")
                time.sleep(60)


    def shutdown(self):
        """Chiude il servizio"""
        logging.info("Arresto servizio")
        self._running = False
        
        if self._sync_in_progress:
            logging.info("Attendo completamento sincronizzazione in corso...")
            while self._sync_in_progress:
                time.sleep(1)  # CORREZIONE: time.sleep invece di io.sleep
        
        # self.openproject_api.close()
        self.db_connector.close_connections()
        logging.info("Servizio arrestato")


# class OpenProjectAPI:
    # """Gestisce le chiamate API a OpenProject"""
    
    # def __init__(self, config: ConfigManager):
    #     self.config = config
    #     self.base_url = config.openproject_config['url'].rstrip('/')
    #     self.api_key = config.openproject_config['api_key']
    #     self._session = None
    #     self._custom_fields_cache = {}
    #     self.cache_timestamp = None
    #     self.cache_duration = timedelta(hours=1)  # Cache valida per 1 ora

    
    #  def _get_session(self) -> aiohttp.ClientSession:
    #     """Ottiene la sessione HTTP"""
    #     if self._session is None:
    #         auto = aiohttp.BasicAuth("apikey", {self.api_key})
    #         headers = {
    #             'Authorization': f'Basic {self.api_key}',
    #             'Content-Type': 'application/json'
    #         }
    #         timeout = aiohttp.ClientTimeout(total=30)
    #         self._session = aiohttp.ClientSession(
    #             headers=headers,
    #             timeout=timeout
    #         )
    #     return self._session


    # #  def get_project_form(self):

    # ### CUSTOM FIELD ###

    # def _is_cache_valid(self) -> bool:
    #     """Verifica se la cache è ancora valida"""
    #     if self.cache_timestamp is None:
    #         return False
    #     return datetime.now() - self.cache_timestamp < self.cache_duration
    
    #  def get_project_schema(self) -> Optional[Dict]:
    #     """
    #     Recupera lo schema del progetto da OpenProject
    #     """
    #     session =  self._get_session()
        
    #     try:
    #          with session.get(f"{self.base_url}/api/v3/projects/schema") as resp:
    #             if resp.status == 200:
    #                 return  resp.json()
    #             elif resp.status == 404:
    #                 return None
    #             else:
    #                 resp.raise_for_status()
    #     except Exception as e:
    #         logging.warning(f"Errore richiesta schema progetto: {e}")
    #         return None


    # def extract_custom_fields_from_schema(self, schema: Dict) -> Dict[str, Dict]:
    #     """
    #     Estrae i custom fields dallo schema del progetto
    #     Returns: Dict con structure {field_key: field_info}
    #     """
    #     custom_fields = {}
        
    #     if not schema:
    #         return custom_fields
        
    #     # Cerca tutti i campi che iniziano con "customField"
    #     for field_key, field_info in schema.items():
    #         if field_key.startswith('customField'):
    #             custom_fields[field_key] = {
    #                 'id': field_key,
    #                 'name': field_info.get('name', ''),
    #                 'type': field_info.get('type', ''),
    #                 'required': field_info.get('required', False),
    #                 'writable': field_info.get('writable', False),
    #                 'location': field_info.get('location', ''),
    #                 'options': field_info.get('options', {})
    #             }
        
    #     logging.info(f"Estratti {len(custom_fields)} custom fields dallo schema")
    #     return custom_fields
    
    #  def load_custom_fields_to_cache(self) -> bool:
    #     """
    #     Carica i custom fields nella cache
    #     """
    #     try:
    #         # Ottieni lo schema
    #         schema =  self.get_project_schema()
    #         if not schema:
    #             return False
            
    #         # Aggiorna la cache
    #         self._custom_fields_cache = self.extract_custom_fields_from_schema(schema)
            
    #         # Aggiorna timestamp cache
    #         self.cache_timestamp = datetime.now()
            
    #         logging.info(f"Cache aggiornata con {len(self._custom_fields_cache)} custom fields")
    #         return True
            
    #     except Exception as e:
    #         logging.error(f"Errore nel caricamento della cache: {e}")
    #         return False
    
    #  def ensure_custom_fields(self) -> Dict[str, int]:
    #     """Assicura che i custom fields esistano"""
    #     if self._custom_fields_cache:
    #         return self._custom_fields_cache
        
    #     session =  self._get_session()

    #     #
        
    #     # Campi custom richiesti
    #     required_fields = [
    #         {'key': 'numero_cliente', 'name': 'Numero Cliente', 'type': 'string'},
    #         {'key': 'indirizzo', 'name': 'Indirizzo', 'type': 'text'},
    #         {'key': 'telefono', 'name': 'Telefono', 'type': 'string'},
    #         {'key': 'numero_commessa', 'name': 'Numero Commessa', 'type': 'string'}
    #     ]
        
    #     for field in required_fields:
    #         field_id =  self._get_or_create_custom_field(session, field)
    #         self._custom_fields_cache[field['key']] = field_id
        
    #     return self._custom_fields_cache
    
    #  def _get_or_create_custom_field(self, session: aiohttp.ClientSession, field: Dict) -> int:
    #     """Ottiene o crea un custom field"""
    #     # Prima controlla se esiste
    #      with session.get(f"{self.base_url}/api/v3/custom_fields") as resp:
    #         if resp.status == 200:
    #             data =  resp.json()
    #             for cf in data.get('_embedded', {}).get('elements', []):
    #                 if cf['name'] == field['name']:
    #                     return cf['id']
        
    #     # Se non esiste, lo crea
    #     field_data = {
    #         'name': field['name'],
    #         'fieldFormat': field['type'],
    #         'isRequired': False,
    #         'isForAll': True
    #     }
        
    #      with session.post(f"{self.base_url}/api/v3/custom_fields", 
    #                            json=field_data) as resp:
    #         if resp.status == 201:
    #             data =  resp.json()
    #             return data['id']
    #         else:
    #             logging.error(f"Errore creazione custom field {field['name']}: {resp.status}")
    #             raise Exception(f"Impossibile creare custom field {field['name']}")
    
    #  def get_project_by_identifier(self, identifier: str) -> Optional[Dict]:
    #     """Cerca un progetto per identificatore"""
    #     session =  self._get_session()
        
    #     try:
    #          with session.get(f"{self.base_url}/api/v3/projects/{identifier}") as resp:
    #             if resp.status == 200:
    #                 return  resp.json()
    #             elif resp.status == 404:
    #                 return None
    #             else:
    #                 resp.raise_for_status()
    #     except Exception as e:
    #         logging.warning(f"Errore ricerca progetto {identifier}: {e}")
    #         return None
    
    #  def create_project(self, record: SyncRecord) -> bool:
    #     """Crea un nuovo progetto"""
    #     session =  self._get_session()
    #     custom_fields =  self.ensure_custom_fields()
        
    #     project_data = {
    #         'identifier': f"commessa-{record.commessa_id}",
    #         'name': record.project_name,
    #         'description': f"Progetto per commessa {record.commessa_id}",
    #         'public': False,
    #         'customField1': {
    #             custom_fields['numero_cliente']: record.cliente_numero,
    #             custom_fields['indirizzo']: record.indirizzo,
    #             custom_fields['telefono']: record.telefono,
    #             custom_fields['numero_commessa']: record.commessa_id
    #         }
    #     }
        
    #     try:
    #          with session.post(f"{self.base_url}/api/v3/projects", 
    #                                json=project_data) as resp:
    #             if resp.status == 201:
    #                 logging.info(f"Progetto creato: {record.project_name}")
    #                 return True
    #             else:
    #                 error_data =  resp.text()
    #                 logging.error(f"Errore creazione progetto {record.project_name}: {error_data}")
    #                 return False
    #     except Exception as e:
    #         logging.error(f"Eccezione creazione progetto: {e}")
    #         return False
    
    #  def update_project(self, project_id: str, record: SyncRecord) -> bool:
    #     """Aggiorna un progetto esistente"""
    #     session =  self._get_session()
    #     custom_fields =  self.ensure_custom_fields()
        
    #     # Aggiorna solo i campi necessari
    #     update_data = {
    #         'name': record.project_name
    #     }
        
    #     try:
    #         # Prima aggiorna i dati base del progetto
    #          with session.patch(f"{self.base_url}/api/v3/projects/{project_id}", 
    #                                 json=update_data) as resp:
    #             if resp.status != 200:
    #                 error_data =  resp.text()
    #                 logging.error(f"Errore aggiornamento progetto base: {error_data}")
    #                 return False
            
    #         # Poi aggiorna i custom fields
    #         success =  self._update_project_custom_fields(
    #             session, project_id, record, custom_fields
    #         )
            
    #         if success:
    #             logging.info(f"Progetto aggiornato: {record.project_name}")
    #             return True
    #         else:
    #             logging.error(f"Errore aggiornamento custom fields")
    #             return False
                
    #     except Exception as e:
    #         logging.error(f"Eccezione aggiornamento progetto: {e}")
    #         return False
    
    #  def _update_project_custom_fields(self, session: aiohttp.ClientSession, 
    #                                       project_id: str, record: SyncRecord, 
    #                                       custom_fields: Dict[str, int]) -> bool:
    #     """Aggiorna i custom fields di un progetto"""
    #     try:
    #         # Costruisce il payload per i custom fields
    #         # Formato corretto per OpenProject API v3
    #         custom_field_updates = {}
            
    #         for field_key, field_id in custom_fields.items():
    #             field_name = f"customField{field_id}"
                
    #             if field_key == 'numero_cliente':
    #                 custom_field_updates[field_name] = record.cliente_numero
    #             elif field_key == 'indirizzo':
    #                 custom_field_updates[field_name] = record.indirizzo
    #             elif field_key == 'telefono':
    #                 custom_field_updates[field_name] = record.telefono
    #             elif field_key == 'numero_commessa':
    #                 custom_field_updates[field_name] = record.commessa_id
            
    #         # Invia PATCH per i custom fields
    #          with session.patch(f"{self.base_url}/api/v3/projects/{project_id}",
    #                                json=custom_field_updates) as resp:
    #             if resp.status == 200:
    #                 return True
    #             else:
    #                 error_data =  resp.text()
    #                 logging.error(f"Errore aggiornamento custom fields: {resp.status} - {error_data}")
                    
    #                 # Prova approccio alternativo se il primo non funziona
    #                 return  self._update_custom_fields_alternative(
    #                     session, project_id, record, custom_fields
    #                 )
                    
    #     except Exception as e:
    #         logging.error(f"Eccezione aggiornamento custom fields: {e}")
    #         return False
    
    #  def _update_custom_fields_alternative(self, session: aiohttp.ClientSession,
    #                                           project_id: str, record: SyncRecord,
    #                                           custom_fields: Dict[str, int]) -> bool:
    #     """Approccio alternativo per aggiornare custom fields"""
    #     try:
    #         # Approccio alternativo usando project attributes (v14+)
    #         attributes_data = {
    #             'projectAttributes': {
    #                 str(custom_fields['numero_cliente']): {'raw': record.cliente_numero},
    #                 str(custom_fields['indirizzo']): {'raw': record.indirizzo},
    #                 str(custom_fields['telefono']): {'raw': record.telefono},
    #                 str(custom_fields['numero_commessa']): {'raw': record.commessa_id}
    #             }
    #         }
            
    #          with session.patch(f"{self.base_url}/api/v3/projects/{project_id}",
    #                                json=attributes_data) as resp:
    #             if resp.status == 200:
    #                 logging.info("Custom fields aggiornati con approccio alternativo")
    #                 return True
    #             else:
    #                 error_data =  resp.text()
    #                 logging.warning(f"Anche approccio alternativo fallito: {resp.status} - {error_data}")
    #                 return False
                    
    #     except Exception as e:
    #         logging.warning(f"Approccio alternativo custom fields fallito: {e}")
    #         return False
    
    #  def close(self):
    #     """Chiude la sessione HTTP"""
    #     if self._session:
    #          self._session.close()

class OpenProjectAPI:
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.base_url = config.openproject_config['url'].rstrip('/')
        self.api_key = config.openproject_config['api_key']
        self._session = None
        self._custom_fields_cache = {}
        self.cache_timestamp = None
        self.cache_duration = timedelta(hours=1)  # Cache valida per 1 ora

    def _is_cache_valid(self) -> bool:
        """Verifica se la cache è ancora valida"""
        if self.cache_timestamp is None:
            return False
        return datetime.now() - self.cache_timestamp < self.cache_duration

    def get_projects_schema(self):
        """
        Effettua una chiamata GET all'endpoint /api/v3/projects/schema
        
        Returns:
            dict: Risposta JSON dell'API o None in caso di errore
        """
        try:
            if self._session is None:
                with requests.Session() as s:
                    s.auth = HTTPBasicAuth("apikey", self.api_key)
                    s.headers.update({"Content-Type": "application/hal+json"})
                    response = s.get(f"{self.base_url}/api/v3/projects/schema")
                    response.raise_for_status()
                
                return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Errore nel parsing del JSON: {e}")
            return None
        
    def extract_custom_fields_from_schema(self, schema: Dict) -> Dict[str, str]:
        """
        Estrae i custom fields dallo schema del progetto
        Returns: Dict con structure {field_key: field_info}
        """
        custom_fields = {}
        
        if not schema:
            return custom_fields
        
        # Cerca tutti i campi che iniziano con "customField"
        for field_key, field_info in schema.items():
            if field_key.startswith('customField'):
                custom_fields[field_info.get('name')] = field_key
        
        logging.info(f"Estratti {len(custom_fields)} custom fields dallo schema")
        return custom_fields
    
    def load_custom_fields_to_cache(self) -> bool:
        """
        Carica i custom fields nella cache
        """
        try:
            # Ottieni lo schema
            schema = self.get_projects_schema()
            if not schema:
                return False
            
            # Aggiorna la cache
            self._custom_fields_cache = self.extract_custom_fields_from_schema(schema)
            
            # Aggiorna timestamp cache
            self.cache_timestamp = datetime.now()
            
            logging.info(f"Cache aggiornata con {len(self._custom_fields_cache)} custom fields")
            return True
            
        except Exception as e:
            logging.error(f"Errore nel caricamento della cache: {e}")
            return False

    def get_projects_form(self):
        """
        Effettua una chiamata GET all'endpoint /api/v3/projects/form
        
        Returns:
            dict: Risposta JSON dell'API o None in caso di errore
        """
        try:
            if self._session is None:
                with requests.Session() as s:
                    s.auth = HTTPBasicAuth("apikey", self.api_key)
                    s.headers.update({"Content-Type": "application/hal+json"})
                    response = s.get(f"{self.base_url}/api/v3/projects/form")
                    response.raise_for_status()
                
                return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Errore durante la chiamata API: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Errore nel parsing del JSON: {e}")
            return None



    def create_project(self, record: SyncRecord) -> Optional[Dict]:

        try:
            # Verifica che la cache sia valida
            if not self._is_cache_valid():
                logging.info("Cache non valida, ricarico custom fields...")
                if not self.load_custom_fields_to_cache():
                    logging.warning("Impossibile caricare la cache dei custom fields")
            
            project_data = {
                'identifier': record.commessa.get_identifier(),
                'name': record.commessa.NrCommessa,
                'description': f"Progetto per commessa {record.commessa.NrCommessa}",
                'public': bool(True),
                self._custom_fields_cache['Numero Impianto']: record.commessa.CodImpianto,
                self._custom_fields_cache['Indirizzo Impianto']: record.indirizzo.IndirizzoImp,
                self._custom_fields_cache['Apertura Commessa']: record.commessa.AperturaCommessa.isoformat(),
                self._custom_fields_cache['Fine Lavori']: record.commessa.FineLavori.isoformat(),
                self._custom_fields_cache['Note']: record.commessa.Note

            }

            # Effettua la chiamata API
            with requests.Session() as s:
                s.auth = HTTPBasicAuth("apikey", self.api_key)
                s.headers.update({
                    "Content-Type": "application/json",
                    "Accept": "application/hal+json"
                })
                
                response = s.post(
                    f"{self.base_url}/api/v3/projects",
                    json=project_data
                )
                
                # Log della richiesta per debug
                logging.debug(f"Request payload: {json.dumps(project_data, indent=2)}")
                logging.debug(f"Response status: {response.status_code}")
                logging.debug(f"Response body: {response.text}")
                
                response.raise_for_status()
                
                created_project = response.json()
                logging.info(f"Progetto creato con successo: {created_project.get('name')} (ID: {created_project.get('id')})")
                
                return created_project
                
        except requests.exceptions.HTTPError as e:
            logging.error(f"Errore HTTP nella creazione del progetto: {e}")
            if hasattr(e, 'response') and e.response:
                logging.error(f"Response body: {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Errore di rete nella creazione del progetto: {e}")
            return None
        except Exception as e:
            logging.error(f"Errore generico nella creazione del progetto: {e}")
            return None



# Entry point
def main():

    service = SyncService()
    
    try:
        service.initialize()

        service.run_scheduler()
    finally:
        service.shutdown()


if __name__ == "__main__":
    main()