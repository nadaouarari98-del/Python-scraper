import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.enrichment.sources.base import PublicSource, ContactResult
from src.enrichment.rate_limiter import GLOBAL_RATE_LIMITER

logger = logging.getLogger(__name__)


class BSESource(PublicSource):
    source_id = "bse"
    domain = "www.bseindia.com"
    rate_limit_seconds = 1.0
    
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
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, requests.Timeout))
    )
    def _search_bse(self, company_name: str) -> Optional[dict]:
        """Search BSE for listed company contact information."""
        GLOBAL_RATE_LIMITER.wait(self.domain, self.rate_limit_seconds)
        
        try:
            url = f"https://{self.domain}/corporates/Default.aspx"
            params = {"name": company_name}
            
            logger.debug(f"{self.source_id}: searching for '{company_name}'")
            resp = requests.get(url, params=params, timeout=10)
            
            if resp.status_code != 200:
                logger.warning(f"{self.source_id}: HTTP {resp.status_code} for '{company_name}'")
                return None
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Try to extract contact phone/email from page
            contact_info = {}
            phone_elem = soup.select_one('a[href^="tel:"]')
            if phone_elem:
                contact_info['contact_number'] = phone_elem.text.strip()
                logger.info(f"{self.source_id}: found phone for '{company_name}': {contact_info['contact_number']}")
            
            email_elem = soup.select_one('a[href^="mailto:"]')
            if email_elem:
                contact_info['email'] = email_elem.text.strip()
                logger.info(f"{self.source_id}: found email for '{company_name}': {contact_info['email']}")
            
            if contact_info:
                return contact_info
            else:
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
        """Search for shareholder contact using BSE database."""
        if not self._check_robots_txt():
            logger.warning(f"{self.source_id}: robots.txt disallows scraping")
            return None
        
        company_name = record.get('company_name', '')
        if not company_name:
            logger.debug(f"{self.source_id}: no company_name in record")
            return None
        
        try:
            result = self._search_bse(company_name)
            
            if result:
                contact_number = result.get('contact_number')
                email = result.get('email')
                
                if contact_number or email:
                    return ContactResult(
                        contact_number=contact_number,
                        email=email,
                        source=self.source_id,
                        confidence=0.8,
                        raw_data=result
                    )
            
            return None
            
        except Exception as e:
            logger.error(f"{self.source_id}: search failed for record {record.get('id', '?')}: {e}")
            return None
