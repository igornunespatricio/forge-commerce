"""
Pydantic models for API response validation and data structure definitions.
"""

from datetime import date, datetime
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, validator


class CustomerResponse(BaseModel):
    """Customer data model for API responses."""
    customer_id: int
    customer_uuid: str
    first_name: str
    last_name: str
    email: str
    phone: str
    date_of_birth: date
    registration_date: date
    country: str
    country_code: str
    city: str
    address: str
    postal_code: str
    customer_segment: str
    customer_lifetime_value: float
    preferred_payment_method: str
    is_active: bool
    last_login_date: date
    total_orders: int
    total_spent: float
    created_at: datetime
    updated_at: datetime

    class Config:
        json_encoders = {
            date: lambda v: v.strftime('%Y-%m-%d'),
            datetime: lambda v: v.strftime('%Y-%m-%d %H:%M:%S')
        }


class ProductResponse(BaseModel):
    """Product data model for API responses."""
    product_id: int
    product_uuid: str
    product_name: str
    sku: str
    upc: str
    category: str
    subcategory: str
    brand: str
    description: str
    price: float
    cost_price: float
    margin: float
    inventory_level: int
    total_value: float
    supplier_name: str
    supplier_country: str
    weight: float
    dimensions: str
    color: str
    material: str
    product_rating: float
    review_count: int
    is_active: bool
    is_discontinued: bool
    created_at: date
    last_updated: date
    warranty_months: int
    return_policy_days: int

    class Config:
        json_encoders = {
            date: lambda v: v.strftime('%Y-%m-%d')
        }


class OrderItemResponse(BaseModel):
    """Order item data model for API responses."""
    product_id: int
    product_name: str
    category: str
    subcategory: str
    brand: str
    unit_price: float
    quantity: int
    discount_percentage: float
    line_total: float


class OrderResponse(BaseModel):
    """Order data model for API responses."""
    order_id: int
    order_uuid: str
    customer_id: int
    customer_segment: str
    order_date: date
    order_status: str
    payment_status: str
    payment_method: str
    shipping_method: str
    shipping_cost: float
    discount_percentage: float
    discount_amount: float
    tax_rate: float
    tax_amount: float
    subtotal: float
    total_amount: float
    fulfillment_date: Optional[date]
    delivery_date: Optional[date]
    tracking_number: str
    delivery_address: str
    delivery_city: str
    delivery_country: str
    delivery_postal_code: str
    order_items: List[OrderItemResponse]
    created_at: datetime
    updated_at: datetime

    class Config:
        json_encoders = {
            date: lambda v: v.strftime('%Y-%m-%d'),
            datetime: lambda v: v.strftime('%Y-%m-%d %H:%M:%S')
        }


class PaymentMetadata(BaseModel):
    """Payment metadata model for API responses."""
    ip_address: str
    user_agent: str
    device_type: str
    browser: str


class PaymentResponse(BaseModel):
    """Payment data model for API responses."""
    payment_id: int
    payment_uuid: str
    order_id: int
    customer_id: int
    customer_segment: str
    payment_method: str
    payment_status: str
    payment_gateway: str
    payment_date: date
    payment_time: str
    currency_code: str
    order_amount: float
    transaction_fee_rate: float
    transaction_fee: float
    net_amount: float
    fraud_score: float
    chargeback_amount: Optional[float]
    chargeback_date: Optional[date]
    chargeback_reason: Optional[str]
    payment_reference: str
    payment_metadata: PaymentMetadata
    created_at: datetime
    updated_at: datetime

    class Config:
        json_encoders = {
            date: lambda v: v.strftime('%Y-%m-%d'),
            datetime: lambda v: v.strftime('%Y-%m-%d %H:%M:%S')
        }


class ApiResponse(BaseModel):
    """Generic API response wrapper."""
    data: Dict
    metadata: Dict = Field(default_factory=dict)

    @validator('metadata', pre=True, always=True)
    def set_metadata_defaults(cls, v):
        if v is None:
            v = {}
        v.setdefault('generated_at', datetime.now().isoformat())
        v.setdefault('version', '1.0')
        return v