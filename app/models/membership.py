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

