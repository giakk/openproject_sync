from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import hashlib
import json

class UserStatus(Enum):
    ACTIVE = "active"
    LOCKED = "locked"
    REGISTERED = "registered"


@dataclass
class GestionaleUser:
    """Modello utente dal database gestionale"""
    id: str
    email: str
    firstName: str
    lastName: str
    phoneNumber: Optional[str] = None
    isActive: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte in dizionario per hashing e serializzazione"""
        return {
            'email': self.email,
            'firstname': self.firstName,
            'lastname': self.lastName,
            'phone': self.phoneNumber,
            'active': self.isActive
        }
    
    def calculate_hash(self) -> str:
        """Calcola hash per rilevare cambiamenti"""
        data_str = json.dumps(self.to_dict(), sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()



@dataclass
class OpenProjectUser:
    """Modello utente per OpenProject"""
    id: int = None
    firstName: str = ""
    lastName: str = ""
    email: str = ""
    admin: bool = False
    password: str = "Open_Project@2025!"
    status: UserStatus = UserStatus.ACTIVE
    phone: str = None
    custom_fields_cache: Dict[str, str]
    ref: str

    def to_api_payload(self) -> Dict[str, Any]:
        """Converte in payload per API OpenProject"""
        
        
        payload = {

            "login": self.email,
            "password": self.password,
            "firstName": self.firstName,
            "lastName": self.lastName,
            "email": self.email,
            "admin": False,
            "status": self.status.value
        }


        # Aggiungi custom fields se presenti
        if self.phone:
            payload[self.custom_fields_cache["Numero"]] = self.phone

            
        return payload
    
    



@dataclass
class CachedUser:
    """Modello utente nel database di appoggio"""
    gestionale_id: str
    openproject_id: int = None
    email: str = ""
    current_hash: str = ""
    last_sync_hash: Optional[str] = None
    last_sync_at: Optional[datetime] = None
    sync_status: str = "pending"  # pending, synced, error
    sync_attempts: int = 0
    last_error: Optional[str] = None
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = field(default_factory=datetime.now)
    
    def needs_sync(self) -> bool:
        """Determina se l'utente necessita sincronizzazione"""
        return (
            self.current_hash != self.last_sync_hash or
            self.sync_status == "error" or
            self.openproject_id is None
        )
    
    def is_sync_failed(self) -> bool:
        """Determina se la sincronizzazione è fallita troppe volte"""
        return self.sync_attempts >= 3 and self.sync_status == "error"
    

class OperationType(Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class UserSyncOperation:
    """Rappresenta un'operazione di sincronizzazione"""
    operation_type: str  # create, update, delete
    gestionale_user: GestionaleUser
    openproject_user: OpenProjectUser
    cached_user: CachedUser
    validation_errors: list = field(default_factory=list)
    auto_corrections: list = field(default_factory=list)
    
    def is_valid(self) -> bool:
        """Verifica se l'operazione è valida"""
        return len(self.validation_errors) == 0
    