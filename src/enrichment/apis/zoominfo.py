import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from typing import Optional
from .base_api import PaidAPIClient, APIResult

api_id = "zoominfo"
config_key = "zoominfo_api_key"

class ZoomInfoClient(PaidAPIClient):
    api_id = api_id
    config_key = config_key
    endpoint = "https://api.zoominfo.com/lookup/contact"

    def __init__(self, api_key: str):
        super().__init__(api_key)

    @retry(
        retry=retry_if_exception_type((requests.exceptions.RequestException, RuntimeError)),
        stop=stop_after_attempt(2),
        wait=wait_fixed(60)
    )
    def search(self, record: dict) -> Optional[APIResult]:
        if not self.is_configured():
            return None
        full_name = record.get("full_name")
        company_name = record.get("company_name")
        payload = {
            "fullName": full_name,
            "companyName": company_name
        }
        headers = {"Api-Key": self.api_key}
        try:
            resp = requests.post(self.endpoint, json=payload, headers=headers, timeout=30)
            self.calls_made += 1
            if resp.status_code == 200:
                data = resp.json()
                person = data.get("person", {})
                phone = None
                if person.get("phone_numbers"):
                    phone = person["phone_numbers"][0].get("sanitized_number")
                email = person.get("email")
                return APIResult(
                    contact_number=phone,
                    email=email,
                    source="zoominfo",
                    confidence=1.0 if phone or email else 0.0,
                    credits_used=1,
                    raw_response=data
                )
            elif resp.status_code == 404:
                return None
            elif resp.status_code == 401:
                raise ValueError("ZoomInfo API key invalid")
            elif resp.status_code == 429:
                raise RuntimeError("Rate limited (429)")
            elif 500 <= resp.status_code < 600:
                raise RuntimeError(f"ZoomInfo 5xx error: {resp.status_code}")
            else:
                return None
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"ZoomInfo request failed: {e}")
