#!/usr/bin/env python3
"""
Order Data Generation Script

Generates synthetic order data for the e-commerce data warehouse.
Follows the .clinerules for data generation best practices.

Target: 20M-50M orders with realistic customer behavior patterns,
seasonal trends, product preferences, and order fulfillment workflows.

Author: Data Engineering Team
"""

import os
import sys
import argparse
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import pandas as pd
from faker import Faker
from faker.providers import BaseProvider
import numpy as np

# Add the parent directory to Python path to import tools module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tools.storage_client import StorageClient, StorageClientFactory, upload_csv, upload_json, upload_parquet

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/generate_orders.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class OrderProvider(BaseProvider):
    """Custom Faker provider for e-commerce order data."""
    
    def order_status(self) -> str:
        """Generate order status with realistic distribution."""
        statuses = [
            ('completed', 0.85),      # 85% completed orders
            ('cancelled', 0.08),      # 8% cancelled orders
            ('refunded', 0.05),       # 5% refunded orders
            ('pending', 0.02)         # 2% pending orders
        ]
        return random.choices(population=[s[0] for s in statuses], 
                            weights=[s[1] for s in statuses], k=1)[0]
    
    def payment_status(self, order_status: str) -> str:
        """Generate payment status based on order status."""
        if order_status == 'completed':
            return random.choices(
                population=['paid', 'pending', 'failed'],
                weights=[0.95, 0.03, 0.02],
                k=1
            )[0]
        elif order_status == 'cancelled':
            return random.choices(
                population=['refunded', 'pending', 'failed'],
                weights=[0.60, 0.30, 0.10],
                k=1
            )[0]
        elif order_status == 'refunded':
            return 'refunded'
        else:
            return random.choices(
                population=['pending', 'failed'],
                weights=[0.80, 0.20],
                k=1
            )[0]
    
    def shipping_method(self) -> str:
        """Generate shipping method with realistic distribution."""
        methods = [
            ('standard', 0.60),      # 60% standard shipping
            ('express', 0.25),       # 25% express shipping
            ('overnight', 0.10),     # 10% overnight shipping
            ('pickup', 0.05)         # 5% in-store pickup
        ]
        return random.choices(population=[m[0] for m in methods], 
                            weights=[m[1] for m in methods], k=1)[0]
    
    def discount_percentage(self, customer_segment: str) -> float:
        """Generate discount percentage based on customer segment."""
        if customer_segment == 'premium':
            # Premium customers get better discounts
            return round(random.choices([0, 5, 10, 15], weights=[40, 30, 20, 10])[0] / 100, 2)
        elif customer_segment == 'regular':
            # Regular customers get moderate discounts
            return round(random.choices([0, 5, 10], weights=[60, 30, 10])[0] / 100, 2)
        else:
            # Occasional customers get minimal discounts
            return round(random.choices([0, 5], weights=[80, 20])[0] / 100, 2)
    
    def order_date(self, start_date: datetime, end_date: datetime, customer_segment: str) -> datetime:
        """Generate order date with seasonal and customer behavior patterns."""
        # Add seasonal patterns (more orders in Q4, Q1)
        date_range = end_date - start_date
        random_days = random.randint(0, date_range.days)
        
        # Base date
        order_date = start_date + timedelta(days=random_days)
        
        # Customer behavior patterns
        if customer_segment == 'premium':
            # Premium customers shop more consistently
            season_factor = 1.0
        elif customer_segment == 'regular':
            # Regular customers follow seasonal trends more
            if order_date.month in [11, 12, 1]:  # Holiday season
                season_factor = 1.3
            elif order_date.month in [6, 7, 8]:  # Summer lull
                season_factor = 0.8
            else:
                season_factor = 1.0
        else:
            # Occasional customers shop mostly during sales
            if order_date.month in [1, 6, 11, 12]:  # Sales periods
                season_factor = 1.5
            else:
                season_factor = 0.6
        
        # Weekend shopping patterns
        if order_date.weekday() >= 5:  # Weekend
            season_factor *= 1.2
        
        # Adjust probability based on factors
        if random.random() < (season_factor / 2.0):
            return order_date
        else:
            return self.order_date(start_date, end_date, customer_segment)
    
    def order_quantity(self, customer_segment: str) -> int:
        """Generate order quantity based on customer segment."""
        if customer_segment == 'premium':
            # Premium customers buy more items per order
            return random.choices([1, 2, 3, 4, 5], weights=[20, 30, 25, 15, 10])[0]
        elif customer_segment == 'regular':
            # Regular customers have moderate quantities
            return random.choices([1, 2, 3], weights=[40, 40, 20])[0]
        else:
            # Occasional customers buy fewer items
            return random.choices([1, 2], weights=[70, 30])[0]


class OrderGenerator:
    """Generates synthetic order data with realistic patterns."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.faker = Faker('en_US')
        self.faker.add_provider(OrderProvider)

        # MinIO configuration
        self.storage_client = StorageClientFactory.create_minio_client(config)
        
        # Shipping costs by method
        self.shipping_costs = {
            'standard': (5.0, 15.0),
            'express': (15.0, 35.0),
            'overnight': (25.0, 50.0),
            'pickup': (0.0, 0.0)
        }
        
        # Product category preferences by customer segment
        self.category_preferences = {
            'premium': {
                'electronics': 0.35,
                'clothing': 0.20,
                'home_garden': 0.15,
                'beauty_health': 0.10,
                'sports_outdoors': 0.10,
                'books_media': 0.05,
                'toys_games': 0.05
            },
            'regular': {
                'clothing': 0.30,
                'home_garden': 0.25,
                'electronics': 0.20,
                'beauty_health': 0.10,
                'books_media': 0.08,
                'sports_outdoors': 0.05,
                'toys_games': 0.02
            },
            'occasional': {
                'books_media': 0.35,
                'clothing': 0.25,
                'toys_games': 0.20,
                'beauty_health': 0.10,
                'electronics': 0.05,
                'home_garden': 0.03,
                'sports_outdoors': 0.02
            }
        }
        
        # Order fulfillment times by shipping method (days)
        self.fulfillment_times = {
            'standard': (3, 7),
            'express': (1, 3),
            'overnight': (0, 1),
            'pickup': (0, 0)
        }
    
    def generate_batch(self, batch_size: int, batch_num: int, customer_data: List[Dict], product_data: List[Dict]) -> List[Dict]:
        """Generate a batch of order records."""
        batch_data = []
        
        # Create lookup dictionaries for faster access
        customer_lookup = {c['customer_id']: c for c in customer_data}
        product_lookup = {p['product_id']: p for p in product_data}
        
        # Get ID ranges
        min_customer_id = min(customer_lookup.keys())
        max_customer_id = max(customer_lookup.keys())
        min_product_id = min(product_lookup.keys())
        max_product_id = max(product_lookup.keys())
        
        for i in range(batch_size):
            order_id = self.config['start_id'] + (batch_num * self.config['batch_size']) + i
            
            # Select customer with realistic distribution
            customer_id = random.randint(min_customer_id, max_customer_id)
            customer = customer_lookup.get(customer_id)
            
            if not customer:
                # Fallback to random selection if customer not found
                customer = random.choice(customer_data)
                customer_id = customer['customer_id']
            
            customer_segment = customer['customer_segment']
            
            # Generate order data
            order_status = self.faker.order_status()
            payment_status = self.faker.payment_status(order_status)
            shipping_method = self.faker.shipping_method()
            
            # Generate order date with customer behavior patterns
            order_date = self.faker.order_date(
                self.config['start_date'], 
                self.config['end_date'],
                customer_segment
            )
            
            # Generate order items
            order_quantity = self.faker.order_quantity(customer_segment)
            order_items = self._generate_order_items(
                order_quantity, 
                customer_segment, 
                product_lookup,
                min_product_id,
                max_product_id
            )
            
            # Calculate order totals
            subtotal = sum(item['line_total'] for item in order_items)
            shipping_cost = self._calculate_shipping_cost(shipping_method, subtotal)
            discount_percentage = self.faker.discount_percentage(customer_segment)
            discount_amount = subtotal * discount_percentage
            tax_rate = 0.08  # 8% average tax rate
            tax_amount = (subtotal - discount_amount) * tax_rate
            total_amount = subtotal - discount_amount + shipping_cost + tax_amount
            
            # Generate fulfillment dates
            fulfillment_date = self._calculate_fulfillment_date(order_date, shipping_method)
            delivery_date = self._calculate_delivery_date(fulfillment_date, shipping_method)
            
            # Generate tracking number
            tracking_number = self.faker.bothify(text='TRK-########')
            
            order_data = {
                'order_id': order_id,
                'order_uuid': self.faker.uuid4(),
                'customer_id': customer_id,
                'customer_segment': customer_segment,
                'order_date': order_date.strftime('%Y-%m-%d'),
                'order_status': order_status,
                'payment_status': payment_status,
                'payment_method': customer['preferred_payment_method'],
                'shipping_method': shipping_method,
                'shipping_cost': round(shipping_cost, 2),
                'discount_percentage': discount_percentage,
                'discount_amount': round(discount_amount, 2),
                'tax_rate': tax_rate,
                'tax_amount': round(tax_amount, 2),
                'subtotal': round(subtotal, 2),
                'total_amount': round(total_amount, 2),
                'fulfillment_date': fulfillment_date.strftime('%Y-%m-%d') if fulfillment_date else None,
                'delivery_date': delivery_date.strftime('%Y-%m-%d') if delivery_date else None,
                'tracking_number': tracking_number,
                'delivery_address': customer['address'],
                'delivery_city': customer['city'],
                'delivery_country': customer['country'],
                'delivery_postal_code': str(customer['postal_code']),  # Convert to string for API compatibility
                'order_items': order_items,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            batch_data.append(order_data)
        
        return batch_data
    
    def _generate_order_items(self, quantity: int, customer_segment: str, product_lookup: Dict, min_product_id: int, max_product_id: int) -> List[Dict]:
        """Generate order items with realistic product selection."""
        order_items = []
        
        # Get category preferences for this customer segment
        preferences = self.category_preferences[customer_segment]
        
        for _ in range(quantity):
            # Select product category based on customer preferences
            category = random.choices(
                population=list(preferences.keys()),
                weights=list(preferences.values()),
                k=1
            )[0]
            
            # Find products in this category
            category_products = [
                p for p in product_lookup.values() 
                if p['category'] == category and p['is_active'] and p['inventory_level'] > 0
            ]
            
            if not category_products:
                # Fallback to any available product
                category_products = [p for p in product_lookup.values() if p['is_active'] and p['inventory_level'] > 0]
            
            if category_products:
                product = random.choice(category_products)
                
                # Determine quantity (usually 1, sometimes more for certain categories)
                item_quantity = 1
                if category in ['books_media', 'toys_games'] and random.random() < 0.3:
                    item_quantity = random.randint(1, 3)
                
                # Apply potential discount for the item
                item_discount = 0.0
                if random.random() < 0.2:  # 20% chance of item-specific discount
                    item_discount = round(random.uniform(0.05, 0.25), 2)
                
                line_total = product['price'] * item_quantity * (1 - item_discount)
                
                order_item = {
                    'product_id': product['product_id'],
                    'product_name': product['product_name'],
                    'category': product['category'],
                    'subcategory': product['subcategory'],
                    'brand': product['brand'],
                    'unit_price': product['price'],
                    'quantity': item_quantity,
                    'discount_percentage': item_discount,
                    'line_total': round(line_total, 2)
                }
                
                order_items.append(order_item)
        
        return order_items
    
    def _calculate_shipping_cost(self, shipping_method: str, subtotal: float) -> float:
        """Calculate shipping cost based on method and order value."""
        base_min, base_max = self.shipping_costs[shipping_method]
        
        # Free shipping for orders over $100 with standard shipping
        if shipping_method == 'standard' and subtotal >= 100.0:
            return 0.0
        
        # Weight-based calculation (simplified)
        weight_factor = random.uniform(0.5, 2.0)
        shipping_cost = base_min + (base_max - base_min) * weight_factor
        
        return round(shipping_cost, 2)
    
    def _calculate_fulfillment_date(self, order_date: datetime, shipping_method: str) -> datetime:
        """Calculate order fulfillment date."""
        if shipping_method == 'pickup':
            # Pickup orders are fulfilled immediately
            return order_date
        
        # Add 1-2 days for processing
        processing_days = random.randint(1, 2)
        fulfillment_date = order_date + timedelta(days=processing_days)
        
        # Don't fulfill on weekends
        while fulfillment_date.weekday() >= 5:
            fulfillment_date += timedelta(days=1)
        
        return fulfillment_date
    
    def _calculate_delivery_date(self, fulfillment_date: datetime, shipping_method: str) -> datetime:
        """Calculate expected delivery date."""
        if not fulfillment_date or shipping_method == 'pickup':
            return None
        
        min_days, max_days = self.fulfillment_times[shipping_method]
        delivery_days = random.randint(min_days, max_days)
        
        delivery_date = fulfillment_date + timedelta(days=delivery_days)
        
        # Don't deliver on weekends for most methods
        if shipping_method != 'pickup':
            while delivery_date.weekday() >= 5:
                delivery_date += timedelta(days=1)
        
        return delivery_date
    
    def validate_data(self, data: List[Dict]) -> bool:
        """Validate generated order data."""
        if not data:
            return False
        
        # Check for required fields
        required_fields = ['order_id', 'customer_id', 'order_date', 'total_amount']
        for record in data:
            for field in required_fields:
                if not record.get(field):
                    logger.error(f"Missing required field: {field} in record {record.get('order_id')}")
                    return False
            
            # Validate amounts
            if record['total_amount'] <= 0:
                logger.error(f"Invalid total amount: {record['total_amount']} in record {record.get('order_id')}")
                return False
        
        return True
    
    def generate_single_order(self, order_id: int = None, customer_id: int = None) -> Dict:
        """Generate a single order record for API use using generate_batch method."""
        if order_id is None:
            order_id = self.faker.random_int(min=1, max=999999999)
        
        # Temporarily set batch_size to 1 for single order generation
        original_batch_size = self.config.get('batch_size', 1)
        self.config['batch_size'] = 1
        
        try:
            # Load customer and product data from storage using storage client
            customer_data, product_data = self._load_reference_data_from_storage()
            
            # Use generate_batch method with batch size 1
            batch_data = self.generate_batch(batch_size=1, batch_num=0, customer_data=customer_data, product_data=product_data)
            
            # Extract the single order record from the batch
            order_data = batch_data[0]
            
            # Override the order_id if provided
            if order_id is not None:
                order_data['order_id'] = order_id
            
            return order_data
        finally:
            # Restore original batch_size
            self.config['batch_size'] = original_batch_size
    
    def _load_reference_data_from_storage(self) -> Tuple[List[Dict], List[Dict]]:
        """Load customer and product data from storage using storage client."""
        customer_data: List[Dict] = []
        product_data: List[Dict] = []
        customer_data_list_objects = self.storage_client.list_objects(self.config.get('bucket_name'),'customers')
        for customer_data_object in customer_data_list_objects:
                customer_data_list = self.storage_client.download_object_as_json(self.config.get('bucket_name'), customer_data_object)
                customer_data.extend(customer_data_list)
        product_data_list_objects = self.storage_client.list_objects(self.config.get('bucket_name'),'products')
        for product_data_object in product_data_list_objects:
                product_data_list = self.storage_client.download_object_as_json(self.config.get('bucket_name'), product_data_object)
                product_data.extend(product_data_list)
        # product_data = self.storage_client.download_object_as_string('products')
        return customer_data, product_data

    def save_batch(self, data: List[Dict], batch_num: int, output_format: str = 'csv') -> str:
        """Save a batch of order data to file."""
        if not self.validate_data(data):
            raise ValueError("Data validation failed")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"orders_batch_{batch_num:04d}_{timestamp}"
        filename_with_prefix = f"{self.config.get('filepath_prefix')}/{filename}.json"
        filepath = f"{self.config.get('endpoint_url')}/{filename_with_prefix}"
        try:
            if output_format.lower() == 'csv':
                raise NotImplementedError("CSV upload is not implemented yet")
            elif output_format.lower() == 'json':
                df = pd.DataFrame(data)
                success = upload_json(
                    bucket_name=self.config.get('bucket_name'),
                    key=filename_with_prefix,
                    data=df.to_dict(orient='records'),
                    storage_client=self.storage_client
                )
                if not success:
                    raise Exception(f"Failed to upload JSON batch {batch_num}")
                logger.info(f"Uploaded batch {batch_num} to MinIO as {filename}.json")
                return filepath
            elif output_format.lower() == 'parquet':
                raise NotImplementedError("Parquet upload is not implemented yet")
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
        except Exception as e:
            logger.error(f"Error uploading batch {batch_num} to MinIO: {str(e)}")
            raise
    
    def generate_orders(self, customer_data: List[Dict], product_data: List[Dict]) -> Dict[str, int]:
        """Generate all order data in batches."""
        total_records = self.config['total_records']
        batch_size = self.config['batch_size']
        output_format = self.config['output_format']
        
        total_batches = (total_records + batch_size - 1) // batch_size
        files_created = []
        total_records_generated = 0
        
        logger.info(f"Starting order generation: {total_records:,} records in {total_batches} batches")
        logger.info(f"Using {len(customer_data):,} customers and {len(product_data):,} products")
        
        for batch_num in range(total_batches):
            current_batch_size = min(batch_size, total_records - total_records_generated)
            
            logger.info(f"Generating batch {batch_num + 1}/{total_batches} ({current_batch_size:,} records)")
            
            try:
                batch_data = self.generate_batch(current_batch_size, batch_num, customer_data, product_data)
                filepath = self.save_batch(batch_data, batch_num, output_format)
                
                files_created.append(filepath)
                total_records_generated += len(batch_data)
                
                # Log progress
                progress = (total_records_generated / total_records) * 100
                logger.info(f"Progress: {progress:.1f}% ({total_records_generated:,}/{total_records:,})")
                
            except Exception as e:
                logger.error(f"Error generating batch {batch_num}: {str(e)}")
                raise
        
        logger.info(f"Order generation completed: {total_records_generated:,} records in {len(files_created)} files")
        
        return {
            'total_records': total_records_generated,
            'files_created': len(files_created),
            'files': files_created
        }


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate synthetic order data')
    parser.add_argument('--total-records', type=int, default=500000, 
                       help='Total number of order records to generate (default: 500,000)')
    parser.add_argument('--batch-size', type=int, default=25000,
                       help='Number of records per batch (default: 25,000)'),
    parser.add_argument('--output-format', type=str, choices=['csv', 'json', 'parquet'], 
                       default='csv', help='Output file format (default: csv)')
    parser.add_argument('--start-date', type=str, default='2020-01-01',
                       help='Start date for order dates (default: 2020-01-01)')
    parser.add_argument('--end-date', type=str, default='2024-12-31',
                       help='End date for order dates (default: 2024-12-31)')
    parser.add_argument('--start-id', type=int, default=1,
                       help='Starting order ID (default: 1)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducible results (default: 42)')
    parser.add_argument('--bucket-name', type=str, default='forge-commerce', help='Bucket Name (default: forge-commerce)')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:9000', help='Endpoint URL (default: http://localhost:9000)')
    parser.add_argument('--filepath-prefix', type=str, default='orders', help='Filepath prefix (default: orders)')
    return parser.parse_args()




def main():
    """Main execution function."""
    args = parse_arguments()
    
    # Set random seed for reproducibility
    random.seed(args.seed)
    np.random.seed(args.seed)
    
    # Configuration
    config = {
        'total_records': args.total_records,
        'batch_size': args.batch_size,
        'output_format': args.output_format,
        'start_date': datetime.strptime(args.start_date, '%Y-%m-%d'),
        'end_date': datetime.strptime(args.end_date, '%Y-%m-%d'),
        'start_id': args.start_id,
        'endpoint_url': 'http://localhost:9000',
        'aws_access_key_id': 'admin',
        'aws_secret_access_key': 'password',
        'bucket_name': args.bucket_name,
        'filepath_prefix': args.filepath_prefix
    }
    
    logger.info(f"Order generation configuration: {config}")
    
    # Create generator and run
    generator = OrderGenerator(config)
    
    try:
        # Load reference data from storage
        customer_data, product_data = generator._load_reference_data_from_storage()
        
        results = generator.generate_orders(customer_data, product_data)
        logger.info(f"Generation completed successfully:")
        logger.info(f"  Total records: {results['total_records']:,}")
        logger.info(f"  Files created: {results['files_created']}")
        
    except Exception as e:
        logger.error(f"Order generation failed: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()