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
    Description: str
    OrdineDiLavoro: str
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
    description: str = None
    fatturazione: str = None
    amministratore: str = None
    ordine_di_lavoro: str = None
    stato: str = OpenProjectStatus.ON_TRACK
    custom_fields_cache: Dict[str, str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


    def to_api_payload(self, is_for_update: bool) -> Dict[str, Any]:
        
        status_value = self.stato.value if isinstance(self.stato, OpenProjectStatus) else self.stato

        note = {
            "format": "markdown",
            "raw": self.note,
            "html": ""
        }

        description = self.format_project_description()

        # Custom fields common to creation and update
        custom_fields = {
            self.custom_fields_cache['Numero Impianto']: self.codImpianto,
            self.custom_fields_cache['Indirizzo Impianto']: self.indirizzo,
            self.custom_fields_cache['Apertura Commessa']: self.apertura,
            self.custom_fields_cache['Fine Lavori']: self.fineLavori,
            self.custom_fields_cache['Note']: note,
            self.custom_fields_cache['Stato Fatturazione']: self.fatturazione,
            self.custom_fields_cache['Administrator']: self.amministratore
        }

        # Other common fields between creation and update
        payload = {
            'status': status_value,
            'active': self.active,
            'description': description,
            **custom_fields
        }

        # Specific fields for creation of a project
        if not is_for_update:
            payload.update({
                'identifier': self.identifier,
                'name': self.name,
                'public': True
            })

        return payload
    
    def format_project_description(self):

        COSTANT_SPACE = "\n\n<br style=\"page-break-after:always;\">\n\n### Ordine di lavoro\n\n"

        # List of all the work orders divided
        work_orders = []
        if self.ordine_di_lavoro:
            work_orders = [wo.strip() for wo in self.ordine_di_lavoro.split(';') if wo.strip()]

        # Create markdown list with all the work orders
        markdown_list = "\n".join([f"* {wo}" for wo in work_orders]) if work_orders else ""

        # Combine description and work orders
        description_text = (self.description or "") + (COSTANT_SPACE + markdown_list if markdown_list else "")

        return {
            "format": "markdown",
            "raw": description_text,
            "html": ""
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
