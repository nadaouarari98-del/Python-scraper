from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class ContactResult:
    contact_number: Optional[str]
    email: Optional[str]
    source: str
    confidence: float
    raw_data: dict = None


class PublicSource(ABC):
    source_id: str = ""
    domain: str = ""
    rate_limit_seconds: float = 1.0

    @abstractmethod
    def search(self, record: dict) -> Optional[ContactResult]:
        pass

    def is_available(self) -> bool:
        return True
