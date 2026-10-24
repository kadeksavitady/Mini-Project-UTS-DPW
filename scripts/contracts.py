"""Shared order contract and record validation helpers."""

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]

    @field_validator(
        "order_id", "customer_id", "product_category", "payment_method", mode="before"
    )
    @classmethod
    def strip_required_text(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("order_ts")
    @classmethod
    def normalize_timestamp_to_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def validate_record(payload: dict[str, object]) -> tuple[dict[str, object] | None, list[str]]:
    """Return a JSON-compatible normalized order or human-readable validation errors."""
    try:
        order = Order.model_validate(payload)
    except ValidationError as exc:
        reasons = [
            f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
            for error in exc.errors()
        ]
        return None, reasons
    return order.model_dump(mode="json"), []

