import uuid
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class OrderEvent:
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "ORDER_PLACED"
    order_id: str = field(default_factory=lambda: f"ORD-{uuid.uuid4().hex[:8].upper()}")
    customer_id: str = field(
        default_factory=lambda: f"CUST-{uuid.uuid4().hex[:6].upper()}"
    )
    occurred_at: int = field(
        default_factory=lambda: int(datetime.utcnow().timestamp() * 1000)
    )
    ingested_at: int = field(
        default_factory=lambda: int(datetime.utcnow().timestamp() * 1000)
    )
    schema_version: str = "1.0"
    source_system: str = "order-service"
    payload: dict = field(default_factory=dict)

    def to_dict(self):
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "occurred_at": self.occurred_at,
            "ingested_at": self.ingested_at,
            "schema_version": self.schema_version,
            "source_system": self.source_system,
            "payload": {k: str(v) for k, v in self.payload.items()},
        }


@dataclass
class PaymentEvent:
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "PAYMENT_INITIATED"
    order_id: str = ""
    customer_id: str = ""
    amount: float = 0.0
    currency: str = "INR"
    occurred_at: int = field(
        default_factory=lambda: int(datetime.utcnow().timestamp() * 1000)
    )
    ingested_at: int = field(
        default_factory=lambda: int(datetime.utcnow().timestamp() * 1000)
    )
    schema_version: str = "1.0"
    gateway: str = "razorpay"
    status: str = "PENDING"

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items()}
