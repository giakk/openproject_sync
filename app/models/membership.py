from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import hashlib
import json

@dataclass
class MembershipTask:
    user_id: int
    project_id: int
    success: bool


@dataclass
class CachedMembership:
    """Cached membership nel database di appoggio"""
    user_id: int        # OpenProject user ID
    project_id: int     # OpenProject project ID
    sync_status: str = "synced"  # synced, error (default: synced)
    last_sync_at: Optional[datetime] = None
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = field(default_factory=datetime.now)

    def to_tuple(self) -> tuple:
        """Converte in tupla per confronti veloci"""
        return (self.user_id, self.project_id)
