"""
FastAPI application for generating synthetic e-commerce data.

Provides REST API endpoints for generating single records of customers, products, orders, and payments.
Reuses existing generator classes with single-record methods for consistency and performance.

Author: Data Engineering Team
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
import uvicorn

# Add the parent directory to the path to import generator modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    CustomerResponse, ProductResponse, OrderResponse, 
    PaymentResponse
)
from config import config
from src.generate_customers import CustomerGenerator
from src.generate_products import ProductGenerator
from src.generate_orders import OrderGenerator
from src.generate_payments import PaymentGenerator


# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('api.log')
    ]
)
logger = logging.getLogger(__name__)


# Initialize FastAPI app
app = FastAPI(
    title=config.API_TITLE,
    description=config.API_DESCRIPTION,
    version=config.API_VERSION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    **config.get_cors_config()
)

# Initialize generators with configuration
customer_generator = CustomerGenerator(config.get_generator_config())
product_generator = ProductGenerator(config.get_generator_config())
order_generator = OrderGenerator(config.get_generator_config())
payment_generator = PaymentGenerator(config.get_generator_config())


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests."""
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response: {response.status_code}")
    return response


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle all uncaught exceptions."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred",
            "request_path": str(request.url)
        }
    )


# Validation error handler
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors."""
    logger.warning(f"Validation error: {exc}")
    return JSONResponse(
        status_code=422,
        content={
            "error": "Validation error",
            "message": "Invalid request parameters",
            "details": exc.errors()
        }
    )


@app.get("/", response_model=Dict[str, Any])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "E-Commerce Data Generator API",
        "version": "1.0.0",
        "endpoints": {
            "customers": "/api/generate/customer",
            "products": "/api/generate/product", 
            "orders": "/api/generate/order",
            "payments": "/api/generate/payment"
        },
        "documentation": "/docs",
        "status": "healthy"
    }


@app.get("/api/generate/customer", response_model=CustomerResponse)
async def generate_customer(
    customer_id: Optional[int] = Query(None, description="Specific customer ID (optional)")
):
    """
    Generate a single customer record.
    
    Returns a complete customer record with realistic data including:
    - Personal information (name, email, address)
    - Geographic distribution
    - Customer segment and lifetime value
    - Payment preferences
    - Activity metrics
    """
    try:
        # Generate customer data using existing generator
        customer_data = customer_generator.generate_single_customer(customer_id)
        
        # Validate required fields
        required_fields = ['customer_id', 'email', 'country', 'registration_date']
        for field in required_fields:
            if not customer_data.get(field):
                raise HTTPException(
                    status_code=500, 
                    detail=f"Missing required field: {field}"
                )
        
        # Return structured response
        return CustomerResponse(**customer_data)
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to generate customer: {str(e)}"
        )


@app.get("/api/generate/product", response_model=ProductResponse)
async def generate_product(
    product_id: Optional[int] = Query(None, description="Specific product ID (optional)")
):
    """
    Generate a single product record.
    
    Returns a complete product record with realistic data including:
    - Product details (name, category, brand)
    - Pricing and cost information
    - Inventory and supplier details
    - Product specifications
    - Quality metrics
    """
    try:
        # Generate product data using existing generator
        product_data = product_generator.generate_single_product(product_id)
        
        # Validate required fields
        required_fields = ['product_id', 'product_name', 'category', 'price', 'inventory_level']
        for field in required_fields:
            if not product_data.get(field):
                raise HTTPException(
                    status_code=500, 
                    detail=f"Missing required field: {field}"
                )
        
        # Return structured response
        return ProductResponse(**product_data)
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to generate product: {str(e)}"
        )


@app.get("/api/generate/order", response_model=OrderResponse)
async def generate_order(
    order_id: Optional[int] = Query(None, description="Specific order ID (optional)"),
    customer_id: Optional[int] = Query(None, description="Specific customer ID (optional)")
):
    """
    Generate a single order record.
    
    Returns a complete order record with realistic data including:
    - Order details (status, dates, amounts)
    - Customer segment information
    - Order items with pricing
    - Shipping and fulfillment details
    - Payment method preferences
    """
    try:
        # Generate order data using existing generator
        order_data = order_generator.generate_single_order(order_id, customer_id)
        
        # Validate required fields
        required_fields = ['order_id', 'customer_id', 'order_date', 'total_amount']
        for field in required_fields:
            if not order_data.get(field):
                raise HTTPException(
                    status_code=500, 
                    detail=f"Missing required field: {field}"
                )
        
        # Validate order items
        if not order_data.get('order_items') or len(order_data['order_items']) == 0:
            raise HTTPException(
                status_code=500, 
                detail="Order must contain at least one item"
            )
        
        # Return structured response
        return OrderResponse(**order_data)
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to generate order: {str(e)}"
        )


@app.get("/api/generate/payment", response_model=PaymentResponse)
async def generate_payment(
    payment_id: Optional[int] = Query(None, description="Specific payment ID (optional)")
):
    """
    Generate a single payment record.
    
    Returns a complete payment record with realistic data including:
    - Payment details (method, status, gateway)
    - Financial calculations (fees, net amounts)
    - Fraud detection scores
    - Chargeback information
    - Payment metadata (IP, device, browser)
    """
    try:
        # Generate payment data using existing generator
        payment_data = payment_generator.generate_single_payment(payment_id)
        
        # Validate required fields
        required_fields = ['payment_id', 'order_id', 'payment_method', 'payment_status', 'order_amount']
        for field in required_fields:
            if not payment_data.get(field):
                raise HTTPException(
                    status_code=500, 
                    detail=f"Missing required field: {field}"
                )
        
        # Validate amounts
        if payment_data['order_amount'] <= 0:
            raise HTTPException(
                status_code=500, 
                detail="Order amount must be greater than 0"
            )
        
        # Return structured response
        return PaymentResponse(**payment_data)
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to generate payment: {str(e)}"
        )


@app.get("/api/health", response_model=Dict[str, Any])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
        "services": {
            "customer_generator": "available",
            "product_generator": "available", 
            "order_generator": "available",
            "payment_generator": "available"
        }
    }


@app.get("/api/status", response_model=Dict[str, Any])
async def api_status():
    """API status and statistics endpoint."""
    return {
        "api_info": {
            "name": "E-Commerce Data Generator API",
            "version": "1.0.0",
            "description": "Generates synthetic e-commerce data for data warehouse testing",
            "uptime": "running"
        },
        "endpoints": {
            "customer_generation": {
                "path": "/api/generate/customer",
                "method": "GET",
                "description": "Generate single customer record"
            },
            "product_generation": {
                "path": "/api/generate/product", 
                "method": "GET",
                "description": "Generate single product record"
            },
            "order_generation": {
                "path": "/api/generate/order",
                "method": "GET", 
                "description": "Generate single order record"
            },
            "payment_generation": {
                "path": "/api/generate/payment",
                "method": "GET",
                "description": "Generate single payment record"
            }
        },
        "features": [
            "Reuses existing batch generation logic",
            "Consistent data patterns and distributions",
            "Realistic e-commerce data relationships",
            "Comprehensive validation and error handling",
            "RESTful API design",
            "OpenAPI documentation available at /docs"
        ]
    }


def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = True):
    """Run the FastAPI server."""
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )


if __name__ == "__main__":
    run_server()