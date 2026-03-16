import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.enrichment.sources.base import PublicSource, ContactResult
from src.enrichment.rate_limiter import GLOBAL_RATE_LIMITER

logger = logging.getLogger(__name__)


class DataGovSource(PublicSource):
    source_id = "data_gov"
    domain = "data.gov.in"
    rate_limit_seconds = 2.0
    
    _robots_cache = {}

    def is_available(self) -> bool:
        return True

    def _check_robots_txt(self) -> bool:
        """Check if robots.txt allows scraping (cached per domain)."""
        if self.domain in self._robots_cache:
            return self._robots_cache[self.domain]
        
        try:
            resp = requests.get(f"https://{self.domain}/robots.txt", timeout=5)
            allowed = resp.status_code == 200
            self._robots_cache[self.domain] = allowed
            logger.info(f"{self.source_id}: robots.txt check - allowed={allowed}")
            return allowed
        except Exception as e:
            logger.warning(f"{self.source_id}: robots.txt check failed: {e}")
            self._robots_cache[self.domain] = False
            return False

    @retry(
        stop=stop_after_attempt(1),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, requests.Timeout))
    )
    def _search_data_gov(self, company_name: str) -> Optional[dict]:
        """Search Data.gov.in for company contact information."""
        GLOBAL_RATE_LIMITER.wait(self.domain, self.rate_limit_seconds)
        
        try:
            url = "https://data.gov.in/api/3/action/package_search"
            params = {"q": company_name, "rows": 1}
            
            logger.debug(f"{self.source_id}: searching for '{company_name}'")
            resp = requests.get(url, params=params, timeout=5)
            
            if resp.status_code != 200:
                logger.warning(f"{self.source_id}: HTTP {resp.status_code} for '{company_name}'")
                return None
            
            try:
                data = resp.json()
                if data.get('success') and data.get('result', {}).get('results'):
                    result = data['result']['results'][0]
                    contact_info = {
                        'name': result.get('name'),
                        'organization': result.get('organization', {}).get('name')
                    }
                    logger.info(f"{self.source_id}: found data for '{company_name}'")
                    return contact_info
            except Exception as e:
                logger.debug(f"{self.source_id}: JSON parse error for '{company_name}': {e}")
            
            logger.debug(f"{self.source_id}: no contact found for '{company_name}'")
            return None
                
        except requests.Timeout:
            logger.warning(f"{self.source_id}: timeout searching for '{company_name}'")
            raise
        except ConnectionError as e:
            logger.warning(f"{self.source_id}: connection error for '{company_name}': {e}")
            raise
        except Exception as e:
            logger.error(f"{self.source_id}: unexpected error searching for '{company_name}': {e}")
            return None

    def search(self, record: dict) -> Optional[ContactResult]:
        """Search for shareholder contact using Data.gov.in."""
        if not self._check_robots_txt():
            logger.warning(f"{self.source_id}: robots.txt disallows scraping")
            return None
        
        company_name = record.get('company_name', '')
        if not company_name:
            logger.debug(f"{self.source_id}: no company_name in record")
            return None
        
        try:
            result = self._search_data_gov(company_name)
            
            if result:
                # Data.gov.in doesn't directly provide phone/email, just metadata
                return ContactResult(
                    contact_number=None,
                    email=None,
                    source=self.source_id,
                    confidence=0.5,
                    raw_data=result
                )
            
            return None
            
        except Exception as e:
            logger.error(f"{self.source_id}: search failed for record {record.get('id', '?')}: {e}")
            return None
