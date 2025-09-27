from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from enum import Enum
import hashlib
import json

@dataclass
class ApiComplexStructure:

    href: str
    title: str 

    def get_payload(self) -> str:
        return {
            'href': self.href,
            'title': self.title
        }
    


@dataclass
class CustomFields:

    custom_fields_cache: Dict
    cache_timestamp: datetime = None
    cache_duration = timedelta(hours=1)  # Cache valida per 1 ora

    def _is_cache_valid(self) -> bool:
        """Verifica se la cache è ancora valida"""
        if self.cache_timestamp is None:
            return False
        return datetime.now() - self.cache_timestamp < self.cache_duration
    
    def extract_custom_fields_from_schema(self, schema: Dict) -> Dict[str, str]:
        """
        Estrae i custom fields dallo schema del progetto
        Returns: Dict con structure {field_name: field_key}
        """
        custom_fields = {}
        
        if not schema:
            return custom_fields
        
        # Cerca tutti i campi che iniziano con "customField"
        for field_key, field_info in schema.items():
            if field_key.startswith('customField'):
                custom_fields[field_info.get('name')] = field_key
        
        return custom_fields