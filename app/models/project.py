from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import hashlib
import json
from .user import OpenProjectUser
from .base import ApiComplexStructure


class StatoFatturazione(Enum):
    FATTURATO = "Fatturato"
    DAFATTURARE = "Da Fatturare"
    ACCONTO = "Acconto"

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
class gestionaleProject:

    NrCommessa: str
    CodImpianto: str
    AperturaCommessa: datetime
    FineLavori: datetime
    StatoCommessa: str
    StatoFatturazione: str
    Note: str
    Indirizzo: IndirizzoImpianto
    
    def to_string(self) -> str:
        """Concatena tutte le variabili della classe come stringa per l'hash"""
        # Converte datetime in stringa ISO format per consistency
        apertura_str = self.AperturaCommessa.isoformat() if self.AperturaCommessa else ""
        fine_str = self.FineLavori.isoformat() if self.FineLavori else ""
        
        return f"{self.NrCommessa}_{self.CodImpianto}_{apertura_str}_{fine_str}_{self.StatoCommessa}_{self.StatoFatturazione}_{self.Note}_{self.Indirizzo.to_string}"

    def calculate_hash(self) -> str:
        return hashlib.sha256(self.to_string.encode()).hexdigest()


@dataclass
class OpenProjectProject:

    id: int = None
    name: str
    active: bool = True
    public: bool = True
    codImpianto: str = None
    indirizzo: str = None
    apertura: datetime = None
    fineLavori: datetime = None
    note: str = None
    fatturazione: StatoFatturazione = None
    admin: OpenProjectUser = None
    custom_fields_cache: Dict[str, str]


    def get_identifier(self) -> str:
        return self.name.lower().replace('/', '-')
    
    def 


    def to_api_payload(self) -> Dict[str, Any]:

        return {
            'identifier': self.get_identifier(),
            'name': self.name,
            'description': f"Progetto per commessa {self.name}",
            'public': bool(True),
            self.custom_fields_cache['Numero Impianto']: self.codImpianto,
            self.custom_fields_cache['Indirizzo Impianto']: self.indirizzo,
            self.custom_fields_cache['Apertura Commessa']: self.apertura.isoformat(),
            self.custom_fields_cache['Fine Lavori']: self.fineLavori.isoformat(),
            self.custom_fields_cache['Note']: self.note,
            self.custom_fields_cache['Stato Fatturazione']: self.fatturazione,
            self.custom_fields_cache['Administrator']: self.admin.ref
        }


