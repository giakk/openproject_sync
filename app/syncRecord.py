#!/usr/bin/env python3

import hashlib
import json
import logging
import os
from datetime import datetime
from dataclasses import dataclass



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
    