from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import hashlib
import json
from .base import OpenProjectStatus

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
    
    def format(self) -> str:
        return(f"{self.NominativoImp}\n"
               f"{self.IndirizzoImp}, {self.CapImp}, {self.ProvImp}")
    

@dataclass    
class Amministratore:
    Name: str
    Tel: str
    Cell: str
    Mail: str
    Pec: str

    def to_string(self) -> str:
        """Concatena tutte le variabili della classe come stringa per l'hash"""
        return f"{self.Name}_{self.Tel}_{self.Cell}_{self.Mail}_{self.Pec}"
    
    def format(self) -> str:
        return(f"{self.Name}\n"
               f"{self.Tel}\n"
               f"{self.Cell}\n"
               f"{self.Mail}")
    

@dataclass
class GestionaleProject:

    GimiID: str
    NrCommessa: str
    CodImpianto: str
    AperturaCommessa: datetime
    FineLavori: datetime
    StatoCommessa: str
    StatoFatturazione: str
    Note: str
    Ammin: Amministratore
    Indirizzo: IndirizzoImpianto

    def get_id(self) -> str:
        return f"{self.GimiID}-{self.CodImpianto}"
    
    def to_string(self) -> str:
        """Concatena tutte le variabili della classe come stringa per l'hash"""
        # Converte datetime in stringa ISO format per consistency
        apertura_str = self.AperturaCommessa.isoformat() if self.AperturaCommessa else ""
        fine_str = self.FineLavori.isoformat() if self.FineLavori else ""
        
        return f"{self.NrCommessa}_{self.CodImpianto}_{apertura_str}_{fine_str}_{self.StatoCommessa}_{self.StatoFatturazione}_{self.Note}"

    def concatenate_data(self) -> str:
        return f"{self.to_string()}_{self.Indirizzo.to_string()}_{self.Ammin.to_string()}"

    def calculate_hash(self) -> str:
        return hashlib.sha256(self.concatenate_data().encode()).hexdigest()

    def get_AperturaCommessa_as_str(self):
        if self.AperturaCommessa is None:
            return ""
        return self.AperturaCommessa.isoformat()

    def get_FineLavori_as_str(self):
        if self.FineLavori is None:
            return ""
        return self.FineLavori.isoformat()

@dataclass
class OpenProjectProject:

    id: int = None
    identifier: str = None
    name: str = ""
    active: bool = True
    public: bool = True
    codImpianto: str = None
    indirizzo: str = None
    apertura: str = None
    fineLavori: str = None
    note: str = None
    fatturazione: str = None
    amministratore: str = None
    stato: str = OpenProjectStatus.ON_TRACK
    custom_fields_cache: Dict[str, str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


    def to_api_payload(self, is_for_update: bool) -> Dict[str, Any]:

        status_value = self.stato.value if isinstance(self.stato, OpenProjectStatus) else self.stato

        if is_for_update == True:

            return {
                'status': status_value,
                'active': self.active,
                self.custom_fields_cache['Numero Impianto']: self.codImpianto,
                self.custom_fields_cache['Indirizzo Impianto']: self.indirizzo,
                self.custom_fields_cache['Apertura Commessa']: self.apertura,
                self.custom_fields_cache['Fine Lavori']: self.fineLavori,
                self.custom_fields_cache['Note']: self.note,
                self.custom_fields_cache['Stato Fatturazione']: self.fatturazione,
                self.custom_fields_cache['Administrator']: self.amministratore
            }
        

        return {
            'identifier': self.identifier,
            'name': self.name,
            'description': f"Progetto per commessa {self.name}",
            'public': bool(True),
            'status': status_value,
            'active': self.active,
            self.custom_fields_cache['Numero Impianto']: self.codImpianto,
            self.custom_fields_cache['Indirizzo Impianto']: self.indirizzo,
            self.custom_fields_cache['Apertura Commessa']: self.apertura,
            self.custom_fields_cache['Fine Lavori']: self.fineLavori,
            self.custom_fields_cache['Note']: self.note,
            self.custom_fields_cache['Stato Fatturazione']: self.fatturazione,
            self.custom_fields_cache['Administrator']: self.amministratore
        }


@dataclass
class CachedProject:

    gestionale_id: str
    openproject_id: int = None
    current_hash: str = ""
    last_sync_hash: Optional[str] = None
    last_sync_at: Optional[datetime] = None
    sync_status: str = "pending"  # pending, synced, error
    # sync_attempts: int = 0
    # last_error: Optional[str] = None
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = field(default_factory=datetime.now)

    def needs_sync(self) -> bool:
        """Determina se l'utente necessita sincronizzazione"""
        return (
            self.current_hash != self.last_sync_hash or
            self.sync_status == "error" or
            self.openproject_id is None
        )
    

    # def is_sync_failed(self) -> bool:
    #     """Determina se la sincronizzazione è fallita troppe volte"""
    #     return self.sync_attempts >= 3 and self.sync_status == "error"
    

@dataclass
class ProjectSyncOperation:
    """Represent the syncronization operation"""
    operation_type: str  # create, update, delete
    gestionale_project: GestionaleProject
    openproject_project: OpenProjectProject
    cached_project: CachedProject
    validation_errors: list = field(default_factory=list)
    
    def is_valid(self) -> bool:
        """Verifica se l'operazione è valida"""
        return len(self.validation_errors) == 0