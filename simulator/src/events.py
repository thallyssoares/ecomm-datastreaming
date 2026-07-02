from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    VIEW_ITEM = "view_item"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"


class DeviceType(str, Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


class EcommerceEvent(BaseModel):
    """Evento bruto de clickstream - schema compatível com Bronze BigQuery"""

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: str
    session_id: str
    event_type: EventType
    event_timestamp: datetime = Field(default_factory=datetime.utcnow)
    product_id: Optional[str] = None
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    brand: Optional[str] = None
    price: Optional[float] = None
    currency: str = "BRL"
    page_url: Optional[str] = None
    referrer_url: Optional[str] = None
    user_agent: Optional[str] = None
    ip_country: str = "BR"
    device_type: DeviceType = DeviceType.MOBILE
    os: Optional[str] = None
    browser: Optional[str] = None

    def to_kafka_message(self) -> dict:
        """Serializa para JSON compatível com Kafka producer"""
        return self.model_dump(mode="json")


class UserSession(BaseModel):
    """Estado da sessão do usuário para funil realista"""

    user_id: str
    session_id: str
    started_at: datetime
    last_event_at: datetime
    current_step: int = 0  # 0=page_view, 1=view_item, 2=add_to_cart, 3=purchase
    viewed_products: list[str] = []
    cart: list[str] = []
    device_type: DeviceType = DeviceType.MOBILE
    user_agent: str = ""
    ip_country: str = "BR"
