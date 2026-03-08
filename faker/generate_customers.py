#!/usr/bin/env python3
"""
Customer Data Generation Script

Generates synthetic customer data for the e-commerce data warehouse.
Follows the .clinerules for data generation best practices.

Target: 5M-10M customers with realistic geographic distribution,
customer lifetime value patterns, and temporal registration patterns.

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('generate_customers.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CustomerProvider(BaseProvider):
    """Custom Faker provider for e-commerce customer data."""
    
    def customer_segment(self) -> str:
        """Generate customer segment based on spending patterns."""
        segments = [
            ('premium', 0.10),    # 10% premium customers
            ('regular', 0.60),    # 60% regular customers  
            ('occasional', 0.30)  # 30% occasional customers
        ]
        return random.choices(population=[s[0] for s in segments], 
                            weights=[s[1] for s in segments], k=1)[0]
    
    def registration_date(self, start_date: datetime, end_date: datetime) -> datetime:
        """Generate registration date with seasonal patterns."""
        # Add seasonal patterns (more registrations in Q1 and Q4)
        date_range = end_date - start_date
        random_days = random.randint(0, date_range.days)
        
        # Apply seasonal weighting
        season_factor = 1.0
        test_date = start_date + timedelta(days=random_days)
        month = test_date.month
        
        if month in [1, 2, 11, 12]:  # Q1 and Q4 peaks
            season_factor = 1.5
        elif month in [6, 7, 8]:  # Summer dip
            season_factor = 0.8
            
        # Adjust probability based on season
        if random.random() < (season_factor / 2.0):
            return test_date
        else:
            return self.registration_date(start_date, end_date)
    
    def customer_lifetime_value(self, segment: str) -> float:
        """Generate customer lifetime value based on segment."""
        if segment == 'premium':
            # High spenders: $1000-$10000
            return round(random.lognormvariate(7.5, 1.5), 2)
        elif segment == 'regular':
            # Medium spenders: $100-$2000  
            return round(random.lognormvariate(5.0, 1.2), 2)
        else:
            # Occasional spenders: $10-$500
            return round(random.lognormvariate(3.5, 1.0), 2)


class CustomerGenerator:
    """Generates synthetic customer data with realistic patterns."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.faker = Faker('en_US')
        self.faker.add_provider(CustomerProvider)
        
        # Geographic distribution (realistic US distribution)
        self.geographic_distribution = [
            ('United States', 'US', 0.60),
            ('Canada', 'CA', 0.10), 
            ('United Kingdom', 'GB', 0.08),
            ('Germany', 'DE', 0.07),
            ('France', 'FR', 0.05),
            ('Australia', 'AU', 0.04),
            ('Other', 'XX', 0.06)
        ]
        
        # Payment method preferences by country
        self.payment_methods = {
            'US': [('credit_card', 0.50), ('paypal', 0.30), ('apple_pay', 0.15), ('google_pay', 0.05)],
            'CA': [('credit_card', 0.45), ('paypal', 0.35), ('interac', 0.15), ('google_pay', 0.05)],
            'GB': [('credit_card', 0.40), ('paypal', 0.35), ('apple_pay', 0.20), ('google_pay', 0.05)],
            'DE': [('credit_card', 0.55), ('paypal', 0.25), ('giropay', 0.15), ('sofort', 0.05)],
            'FR': [('credit_card', 0.50), ('paypal', 0.30), ('apple_pay', 0.15), ('google_pay', 0.05)],
            'AU': [('credit_card', 0.45), ('paypal', 0.35), ('apple_pay', 0.15), ('google_pay', 0.05)],
            'XX': [('credit_card', 0.60), ('paypal', 0.25), ('other', 0.15)]
        }
    
    def generate_batch(self, batch_size: int, batch_num: int) -> List[Dict]:
        """Generate a batch of customer records."""
        batch_data = []
        
        for i in range(batch_size):
            customer_id = self.config['start_id'] + (batch_num * self.config['batch_size']) + i
            
            # Select country with realistic distribution
            country_info = random.choices(
                population=[g[:2] for g in self.geographic_distribution],
                weights=[g[2] for g in self.geographic_distribution],
                k=1
            )[0]
            country_name, country_code = country_info
            
            # Generate customer data
            segment = self.faker.customer_segment()
            registration_date = self.faker.registration_date(
                self.config['start_date'], 
                self.config['end_date']
            )
            
            # Generate realistic age distribution
            age = self.faker.random_int(min=18, max=80)
            birth_date = registration_date - timedelta(days=age * 365 + self.faker.random_int(min=0, max=365))
            
            # Generate payment methods
            preferred_methods = self.payment_methods.get(country_code, self.payment_methods['XX'])
            payment_method = random.choices(
                population=[p[0] for p in preferred_methods],
                weights=[p[1] for p in preferred_methods],
                k=1
            )[0]
            
            customer_data = {
                'customer_id': customer_id,
                'customer_uuid': self.faker.uuid4(),
                'first_name': self.faker.first_name(),
                'last_name': self.faker.last_name(),
                'email': self.faker.email(),
                'phone': self.faker.phone_number(),
                'date_of_birth': birth_date.strftime('%Y-%m-%d'),
                'registration_date': registration_date.strftime('%Y-%m-%d'),
                'country': country_name,
                'country_code': country_code,
                'city': self.faker.city(),
                'address': self.faker.address().replace('\n', ', '),
                'postal_code': self.faker.postcode(),
                'customer_segment': segment,
                'customer_lifetime_value': self.faker.customer_lifetime_value(segment),
                'preferred_payment_method': payment_method,
                'is_active': self.faker.boolean(chance_of_getting_true=85),
                'last_login_date': self.faker.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
                'total_orders': self.faker.random_int(min=0, max=100),
                'total_spent': round(random.uniform(0, 5000), 2),
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            batch_data.append(customer_data)
        
        return batch_data
    
    def validate_data(self, data: List[Dict]) -> bool:
        """Validate generated customer data."""
        if not data:
            return False
        
        # Check for required fields
        required_fields = ['customer_id', 'email', 'country', 'registration_date']
        for record in data:
            for field in required_fields:
                if not record.get(field):
                    logger.error(f"Missing required field: {field} in record {record.get('customer_id')}")
                    return False
            
            # Validate email format
            if '@' not in record['email']:
                logger.error(f"Invalid email format: {record['email']} in record {record.get('customer_id')}")
                return False
        
        return True
    
    def save_batch(self, data: List[Dict], batch_num: int, output_format: str = 'csv') -> str:
        """Save a batch of customer data to file."""
        if not self.validate_data(data):
            raise ValueError("Data validation failed")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"customers_batch_{batch_num:04d}_{timestamp}"
        
        output_path = Path(self.config['output_dir'])
        output_path.mkdir(parents=True, exist_ok=True)
        
        if output_format.lower() == 'csv':
            filepath = output_path / f"{filename}.csv"
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False, encoding='utf-8')
        elif output_format.lower() == 'json':
            filepath = output_path / f"{filename}.json"
            pd.DataFrame(data).to_json(filepath, orient='records', date_format='iso')
        elif output_format.lower() == 'parquet':
            filepath = output_path / f"{filename}.parquet"
            df = pd.DataFrame(data)
            df.to_parquet(filepath, index=False)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        logger.info(f"Saved batch {batch_num} to {filepath}")
        return str(filepath)
    
    def generate_single_customer(self, customer_id: int = None) -> Dict:
        """Generate a single customer record for API use using generate_batch method."""
        if customer_id is None:
            customer_id = self.faker.random_int(min=1, max=999999999)
        
        # Temporarily set batch_size to 1 for single customer generation
        original_batch_size = self.config.get('batch_size', 1)
        self.config['batch_size'] = 1
        
        try:
            # Use generate_batch method with batch size 1
            batch_data = self.generate_batch(batch_size=1, batch_num=0)
            
            # Extract the single customer record from the batch
            customer_data = batch_data[0]
            
            # Override the customer_id if provided
            if customer_id is not None:
                customer_data['customer_id'] = customer_id
            
            return customer_data
        finally:
            # Restore original batch_size
            self.config['batch_size'] = original_batch_size

    def generate_customers(self) -> Dict[str, int]:
        """Generate all customer data in batches."""
        total_records = self.config['total_records']
        batch_size = self.config['batch_size']
        output_format = self.config['output_format']
        
        total_batches = (total_records + batch_size - 1) // batch_size
        files_created = []
        total_records_generated = 0
        
        logger.info(f"Starting customer generation: {total_records:,} records in {total_batches} batches")
        
        for batch_num in range(total_batches):
            current_batch_size = min(batch_size, total_records - total_records_generated)
            
            logger.info(f"Generating batch {batch_num + 1}/{total_batches} ({current_batch_size:,} records)")
            
            try:
                batch_data = self.generate_batch(current_batch_size, batch_num)
                filepath = self.save_batch(batch_data, batch_num, output_format)
                
                files_created.append(filepath)
                total_records_generated += len(batch_data)
                
                # Log progress
                progress = (total_records_generated / total_records) * 100
                logger.info(f"Progress: {progress:.1f}% ({total_records_generated:,}/{total_records:,})")
                
            except Exception as e:
                logger.error(f"Error generating batch {batch_num}: {str(e)}")
                raise
        
        logger.info(f"Customer generation completed: {total_records_generated:,} records in {len(files_created)} files")
        
        return {
            'total_records': total_records_generated,
            'files_created': len(files_created),
            'files': files_created
        }


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate synthetic customer data')
    parser.add_argument('--total-records', type=int, default=1000000, 
                       help='Total number of customer records to generate (default: 1,000,000)')
    parser.add_argument('--batch-size', type=int, default=50000,
                       help='Number of records per batch (default: 50,000)')
    parser.add_argument('--output-dir', type=str, default='data/raw/customers',
                       help='Output directory for generated files (default: data/raw/customers)')
    parser.add_argument('--output-format', type=str, choices=['csv', 'json', 'parquet'], 
                       default='csv', help='Output file format (default: csv)')
    parser.add_argument('--start-date', type=str, default='2020-01-01',
                       help='Start date for registration dates (default: 2020-01-01)')
    parser.add_argument('--end-date', type=str, default='2024-12-31',
                       help='End date for registration dates (default: 2024-12-31)')
    parser.add_argument('--start-id', type=int, default=1,
                       help='Starting customer ID (default: 1)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducible results (default: 42)')
    
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
        'output_dir': args.output_dir,
        'output_format': args.output_format,
        'start_date': datetime.strptime(args.start_date, '%Y-%m-%d'),
        'end_date': datetime.strptime(args.end_date, '%Y-%m-%d'),
        'start_id': args.start_id
    }
    
    logger.info(f"Customer generation configuration: {config}")
    
    # Create generator and run
    generator = CustomerGenerator(config)
    
    try:
        results = generator.generate_customers()
        logger.info(f"Generation completed successfully:")
        logger.info(f"  Total records: {results['total_records']:,}")
        logger.info(f"  Files created: {results['files_created']}")
        
    except Exception as e:
        logger.error(f"Customer generation failed: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()