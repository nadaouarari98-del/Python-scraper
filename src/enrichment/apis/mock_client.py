from time import sleep
from typing import Optional
from faker import Faker
from .base_api import PaidAPIClient, APIResult

api_id = "mock"

class MockClient(PaidAPIClient):
    api_id = api_id
    config_key = ""

    def __init__(self, api_key: str = ""):
        super().__init__(api_key)
        self.faker = Faker("en_IN")

    def search(self, record: dict) -> Optional[APIResult]:
        sleep(0.2)  # 200ms delay
        phone = self.faker.phone_number()
        # Format to +91-XXXXX-XXXXX
        phone = "+91-{}-{}".format(phone[-10:-5], phone[-5:])
        email = self.faker.email()
        result = APIResult(
            contact_number=phone,
            email=email,
            source="mock",
            confidence=0.0,
            credits_used=0,
            raw_response={"is_mock": True}
        )
        return result
