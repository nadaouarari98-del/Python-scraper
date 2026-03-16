from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

@dataclass
class APIResult:
    contact_number: Optional[str]
    email: Optional[str]
    source: str
    confidence: float
    credits_used: int = 1
    raw_response: dict = None

class PaidAPIClient(ABC):
    api_id: str = ""
    config_key: str = ""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.calls_made = 0
        self.credits_used = 0

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_key.strip())

    @abstractmethod
    def search(self, record: dict) -> Optional[APIResult]:
        pass
