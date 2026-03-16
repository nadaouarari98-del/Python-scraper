import logging
import requests
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.enrichment.sources.base import PublicSource, ContactResult
from src.enrichment.rate_limiter import GLOBAL_RATE_LIMITER

logger = logging.getLogger(__name__)


class TruecallerSource(PublicSource):
    source_id = "truecaller"
    domain = "www.truecaller.com"
    rate_limit_seconds = 3.0  # 1 request per 3 seconds
    
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
    def _search_truecaller(self, phone_number: str) -> Optional[dict]:
        """Search Truecaller for phone number details."""
        GLOBAL_RATE_LIMITER.wait(self.domain, self.rate_limit_seconds)
        
        try:
            url = "https://www.truecaller.com/search/in/"
            params = {"q": phone_number}
            headers = {"User-Agent": "Mozilla/5.0"}
            
            logger.debug(f"{self.source_id}: searching for '{phone_number}'")
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            
            # Check for CAPTCHA
            if 'captcha' in resp.text.lower() or 'verify you are human' in resp.text.lower():
                logger.warning(f"{self.source_id}: CAPTCHA detected for phone '{phone_number}'")
                return None
            
            if resp.status_code != 200:
                logger.warning(f"{self.source_id}: HTTP {resp.status_code} for phone '{phone_number}'")
                return None
            
            try:
                data = resp.json()
                if data.get('data'):
                    result = data['data'][0]
                    contact_info = {
                        'contact_number': result.get('phone'),
                        'name': result.get('name'),
                        'email': result.get('email')
                    }
                    logger.info(f"{self.source_id}: found contact for phone '{phone_number}'")
                    return contact_info
            except Exception as e:
                logger.debug(f"{self.source_id}: JSON parse error for phone '{phone_number}': {e}")
            
            logger.debug(f"{self.source_id}: no contact found for phone '{phone_number}'")
            return None
                
        except requests.Timeout:
            logger.warning(f"{self.source_id}: timeout searching for phone '{phone_number}'")
            raise
        except ConnectionError as e:
            logger.warning(f"{self.source_id}: connection error for phone '{phone_number}': {e}")
            raise
        except Exception as e:
            logger.error(f"{self.source_id}: unexpected error searching for phone '{phone_number}': {e}")
            return None

    def search(self, record: dict) -> Optional[ContactResult]:
        """Search for shareholder contact using Truecaller API."""
        if not self._check_robots_txt():
            logger.warning(f"{self.source_id}: robots.txt disallows scraping")
            return None
        
        contact_number = record.get('contact_number', '')
        if not contact_number:
            logger.debug(f"{self.source_id}: no contact_number in record")
            return None
        
        try:
            result = self._search_truecaller(contact_number)
            
            if result:
                phone = result.get('contact_number')
                email = result.get('email')
                
                if phone or email:
                    return ContactResult(
                        contact_number=phone,
                        email=email,
                        source=self.source_id,
                        confidence=0.9,
                        raw_data=result
                    )
            
            return None
            
        except Exception as e:
            logger.error(f"{self.source_id}: search failed for record {record.get('id', '?')}: {e}")
            return None
