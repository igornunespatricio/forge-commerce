"""
Configuration module for the E-Commerce Data Generator API.

Provides configuration settings for API behavior, generator parameters,
and environment-specific settings.
"""

import os
from datetime import datetime
from typing import Dict, Any, Optional


class APIConfig:
    """Configuration class for the API."""
    
    # Server configuration
    HOST = os.getenv("API_HOST", "0.0.0.0")
    PORT = int(os.getenv("API_PORT", "8000"))
    RELOAD = os.getenv("API_RELOAD", "true").lower() == "true"
    
    # CORS configuration
    ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "*").split(",")
    ALLOW_CREDENTIALS = os.getenv("ALLOW_CREDENTIALS", "true").lower() == "true"
    ALLOW_METHODS = ["*"]
    ALLOW_HEADERS = ["*"]
    
    # Generator configuration
    GENERATOR_CONFIG = {
        'start_date': datetime(2020, 1, 1),
        'end_date': datetime(2024, 12, 31),
        'start_id': 1,
        'endpoint_url': 'http://localhost:9000',
        'aws_access_key_id': 'admin',
        'aws_secret_access_key': 'password',
        'bucket_name': 'raw',
    }
    
    # Validation settings
    MAX_CUSTOMER_ID = 999999999
    MAX_PRODUCT_ID = 999999999
    MAX_ORDER_ID = 999999999
    MAX_PAYMENT_ID = 999999999
    
    # Business rules validation
    MIN_ORDER_AMOUNT = 0.01
    MIN_PRODUCT_PRICE = 0.01
    MIN_PRODUCT_INVENTORY = 0
    
    # API metadata
    API_TITLE = "E-Commerce Data Generator API"
    API_DESCRIPTION = "API for generating synthetic e-commerce data for data warehouse testing"
    API_VERSION = "1.0.0"
    
    # Logging configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    @classmethod
    def get_generator_config(cls) -> Dict[str, Any]:
        """Get configuration for generator initialization."""
        return cls.GENERATOR_CONFIG.copy()
    
    @classmethod
    def get_cors_config(cls) -> Dict[str, Any]:
        """Get CORS middleware configuration."""
        return {
            "allow_origins": cls.ALLOW_ORIGINS,
            "allow_credentials": cls.ALLOW_CREDENTIALS,
            "allow_methods": cls.ALLOW_METHODS,
            "allow_headers": cls.ALLOW_HEADERS,
        }
    
    @classmethod
    def validate_customer_id(cls, customer_id: Optional[int]) -> Optional[int]:
        """Validate customer ID."""
        if customer_id is not None:
            if not (1 <= customer_id <= cls.MAX_CUSTOMER_ID):
                raise ValueError(f"Customer ID must be between 1 and {cls.MAX_CUSTOMER_ID}")
        return customer_id
    
    @classmethod
    def validate_product_id(cls, product_id: Optional[int]) -> Optional[int]:
        """Validate product ID."""
        if product_id is not None:
            if not (1 <= product_id <= cls.MAX_PRODUCT_ID):
                raise ValueError(f"Product ID must be between 1 and {cls.MAX_PRODUCT_ID}")
        return product_id
    
    @classmethod
    def validate_order_id(cls, order_id: Optional[int]) -> Optional[int]:
        """Validate order ID."""
        if order_id is not None:
            if not (1 <= order_id <= cls.MAX_ORDER_ID):
                raise ValueError(f"Order ID must be between 1 and {cls.MAX_ORDER_ID}")
        return order_id
    
    @classmethod
    def validate_payment_id(cls, payment_id: Optional[int]) -> Optional[int]:
        """Validate payment ID."""
        if payment_id is not None:
            if not (1 <= payment_id <= cls.MAX_PAYMENT_ID):
                raise ValueError(f"Payment ID must be between 1 and {cls.MAX_PAYMENT_ID}")
        return payment_id
    
    @classmethod
    def validate_order_amount(cls, amount: float) -> float:
        """Validate order amount."""
        if amount < cls.MIN_ORDER_AMOUNT:
            raise ValueError(f"Order amount must be greater than {cls.MIN_ORDER_AMOUNT}")
        return amount
    
    @classmethod
    def validate_product_price(cls, price: float) -> float:
        """Validate product price."""
        if price < cls.MIN_PRODUCT_PRICE:
            raise ValueError(f"Product price must be greater than {cls.MIN_PRODUCT_PRICE}")
        return price
    
    @classmethod
    def validate_product_inventory(cls, inventory: int) -> int:
        """Validate product inventory."""
        if inventory < cls.MIN_PRODUCT_INVENTORY:
            raise ValueError(f"Product inventory must be greater than or equal to {cls.MIN_PRODUCT_INVENTORY}")
        return inventory


class DevelopmentConfig(APIConfig):
    """Development environment configuration."""
    RELOAD = True
    LOG_LEVEL = "DEBUG"
    ALLOW_ORIGINS = ["*"]  # Allow all origins in development


class ProductionConfig(APIConfig):
    """Production environment configuration."""
    RELOAD = False
    LOG_LEVEL = "INFO"
    # In production, specify exact origins
    ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "").split(",") if os.getenv("ALLOW_ORIGINS") else ["localhost"]


class TestingConfig(APIConfig):
    """Testing environment configuration."""
    RELOAD = False
    LOG_LEVEL = "WARNING"
    ALLOW_ORIGINS = ["*"]


# Environment-based configuration selection
def get_config():
    """Get configuration based on environment."""
    env = os.getenv("ENVIRONMENT", "development").lower()
    
    if env == "production":
        return ProductionConfig()
    elif env == "testing":
        return TestingConfig()
    else:
        return DevelopmentConfig()


# Get current configuration
config = get_config()