import random
import uuid
import time
from datetime import datetime, timedelta
from faker import Faker
from models import OrderEvent, PaymentEvent

fake = Faker("en_IN")  # Indian locale — realistic for your market

CITIES         = ["Mumbai","Delhi","Bengaluru","Hyderabad","Chennai","Pune","Kolkata"]
GATEWAYS       = ["razorpay","paytm","phonepe","upi_direct"]
PRODUCT_CATS   = ["electronics","fashion","grocery","books","home","sports"]
EVENT_SEQUENCE = [
    "ORDER_PLACED","ORDER_CONFIRMED","PAYMENT_INITIATED",
    "PAYMENT_SUCCESS","ORDER_SHIPPED","ORDER_DELIVERED",
]

class EventGenerator:
    def __init__(self, chaos_rate: float = 0.05):
        """
        chaos_rate: fraction of events that are intentionally malformed.
        0.05 = 5% chaos — realistic for production systems.
        """
        self.chaos_rate = chaos_rate
        self._recent_events: list[dict] = []   # used to generate duplicates

    # ------------------------------------------------------------------ #
    # Core generators                                                      #
    # ------------------------------------------------------------------ #

    def generate_order_event(self, event_type: str = "ORDER_PLACED") -> dict:
        order_id    = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        customer_id = f"CUST-{random.randint(1000, 99999):05d}"
        city        = random.choice(CITIES)
        category    = random.choice(PRODUCT_CATS)
        amount      = round(random.uniform(199, 49999), 2)

        event = OrderEvent(
            event_type=event_type,
            order_id=order_id,
            customer_id=customer_id,
            payload={
                "city":          city,
                "category":      category,
                "amount":        amount,
                "item_count":    random.randint(1, 5),
                "discount_pct":  random.choice([0, 5, 10, 15, 20]),
                "pincode":       fake.postcode(),
                "device":        random.choice(["android","ios","web"]),
            }
        ).to_dict()

        self._recent_events.append(event)
        if len(self._recent_events) > 500:
            self._recent_events.pop(0)

        return event

    def generate_payment_event(self, order_id: str,
                               customer_id: str, amount: float) -> dict:
        status = random.choices(
            ["SUCCESS","FAILED","PENDING"],
            weights=[75, 15, 10]
        )[0]

        return PaymentEvent(
            event_type=f"PAYMENT_{status}",
            order_id=order_id,
            customer_id=customer_id,
            amount=amount,
            gateway=random.choice(GATEWAYS),
            status=status,
        ).to_dict()

    # ------------------------------------------------------------------ #
    # Chaos generators — these simulate real-world production problems     #
    # ------------------------------------------------------------------ #

    def generate_duplicate_event(self) -> dict | None:
        """Resend a recent event with the same event_id — tests dedup logic."""
        if not self._recent_events:
            return None
        event = random.choice(self._recent_events).copy()
        # same event_id, slightly different ingested_at (simulates retry)
        event["ingested_at"] = int(datetime.utcnow().timestamp() * 1000)
        return event

    def generate_late_event(self) -> dict:
        """Event with occurred_at 2–6 hours in the past — tests watermarking."""
        hours_late = random.uniform(2, 6)
        past_ts    = datetime.utcnow() - timedelta(hours=hours_late)
        event      = self.generate_order_event("ORDER_PLACED")
        event["occurred_at"] = int(past_ts.timestamp() * 1000)
        return event

    def generate_malformed_event(self) -> dict:
        """Missing required fields — should route to DLQ."""
        return {
            "event_id":   str(uuid.uuid4()),
            "event_type": "ORDER_PLACED",
            # order_id and customer_id intentionally missing
            "occurred_at": int(datetime.utcnow().timestamp() * 1000),
            "payload": {},
        }

    def generate_schema_evolved_event(self) -> dict:
        """Schema v2 adds a new field — tests forward compatibility."""
        event = self.generate_order_event("ORDER_PLACED")
        event["schema_version"] = "2.0"
        event["payload"]["loyalty_points"] = str(random.randint(0, 500))  # new field
        return event

    # ------------------------------------------------------------------ #
    # Main generation logic with chaos injection                           #
    # ------------------------------------------------------------------ #

    def next_event(self) -> tuple[str, dict]:
        """
        Returns (topic_name, event_dict).
        Injects chaos at self.chaos_rate probability.
        """
        roll = random.random()

        if roll < self.chaos_rate * 0.4:
            # duplicate event — same topic as original
            event = self.generate_duplicate_event()
            if event:
                return ("orders-raw", event)

        if roll < self.chaos_rate * 0.7:
            # late-arriving event
            return ("orders-raw", self.generate_late_event())

        if roll < self.chaos_rate:
            # malformed event — will hit DLQ
            return ("orders-raw", self.generate_malformed_event())

        if roll < self.chaos_rate + 0.02:
            # schema evolved event
            return ("orders-raw", self.generate_schema_evolved_event())

        # Normal events (95%+ of the time)
        event_type = random.choice(EVENT_SEQUENCE)
        return ("orders-raw", self.generate_order_event(event_type))