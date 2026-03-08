#!/usr/bin/env python3
"""
Payment Data Generation Script

Generates synthetic payment data for the e-commerce data warehouse.
Follows the .clinerules for data generation best practices.

Target: 20M-50M payment records with realistic payment patterns,
fraud detection scenarios, and payment method distributions.

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('generate_payments.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PaymentProvider(BaseProvider):
    """Custom Faker provider for e-commerce payment data."""
    
    def payment_method(self, customer_segment: str) -> str:
        """Generate payment method based on customer segment."""
        if customer_segment == 'premium':
            # Premium customers prefer credit cards and digital wallets
            methods = [
                ('credit_card', 0.45),
                ('paypal', 0.25),
                ('apple_pay', 0.15),
                ('google_pay', 0.10),
                ('debit_card', 0.05)
            ]
        elif customer_segment == 'regular':
            # Regular customers use a mix of methods
            methods = [
                ('debit_card', 0.35),
                ('credit_card', 0.30),
                ('paypal', 0.20),
                ('google_pay', 0.10),
                ('apple_pay', 0.05)
            ]
        else:
            # Occasional customers prefer simpler methods
            methods = [
                ('debit_card', 0.40),
                ('paypal', 0.30),
                ('credit_card', 0.20),
                ('google_pay', 0.07),
                ('apple_pay', 0.03)
            ]
        
        return random.choices(population=[m[0] for m in methods], 
                            weights=[m[1] for m in methods], k=1)[0]
    
    def payment_status(self, payment_method: str, order_status: str) -> str:
        """Generate payment status based on method and order status."""
        if order_status == 'completed':
            if payment_method in ['credit_card', 'debit_card']:
                return random.choices(
                    population=['success', 'pending', 'failed'],
                    weights=[0.96, 0.03, 0.01],
                    k=1
                )[0]
            elif payment_method in ['paypal', 'apple_pay', 'google_pay']:
                return random.choices(
                    population=['success', 'pending', 'failed'],
                    weights=[0.98, 0.015, 0.005],
                    k=1
                )[0]
        elif order_status == 'cancelled':
            return random.choices(
                population=['refunded', 'pending', 'failed'],
                weights=[0.70, 0.25, 0.05],
                k=1
            )[0]
        elif order_status == 'refunded':
            return 'refunded'
        else:
            return random.choices(
                population=['pending', 'failed'],
                weights=[0.85, 0.15],
                k=1
            )[0]
    
    def fraud_score(self, payment_method: str, customer_segment: str, order_amount: float) -> float:
        """Generate fraud score based on payment method, customer segment, and order amount."""
        base_score = 0.0
        
        # Payment method risk factors
        if payment_method == 'credit_card':
            base_score += 0.15
        elif payment_method == 'debit_card':
            base_score += 0.10
        elif payment_method in ['paypal', 'apple_pay', 'google_pay']:
            base_score += 0.05
        
        # Customer segment risk factors
        if customer_segment == 'premium':
            base_score -= 0.05  # Lower risk
        elif customer_segment == 'occasional':
            base_score += 0.05  # Higher risk
        
        # Order amount risk factors
        if order_amount > 1000:
            base_score += 0.20
        elif order_amount > 500:
            base_score += 0.10
        elif order_amount > 200:
            base_score += 0.05
        
        # Add random variation
        fraud_score = base_score + random.uniform(-0.05, 0.15)
        fraud_score = max(0.0, min(1.0, fraud_score))  # Clamp between 0 and 1
        
        return round(fraud_score, 3)
    
    def payment_gateway(self, payment_method: str) -> str:
        """Generate payment gateway based on payment method."""
        if payment_method == 'credit_card':
            return random.choice(['stripe', 'paypal', 'square', 'adyen'])
        elif payment_method == 'debit_card':
            return random.choice(['stripe', 'square', 'worldpay'])
        elif payment_method == 'paypal':
            return 'paypal'
        elif payment_method == 'apple_pay':
            return 'stripe'
        elif payment_method == 'google_pay':
            return 'stripe'
        else:
            return 'stripe'
    
    def currency_code(self) -> str:
        """Generate currency code with realistic distribution."""
        currencies = [
            ('USD', 0.60),  # US Dollar
            ('EUR', 0.20),  # Euro
            ('GBP', 0.10),  # British Pound
            ('CAD', 0.05),  # Canadian Dollar
            ('AUD', 0.05)   # Australian Dollar
        ]
        return random.choices(population=[c[0] for c in currencies], 
                            weights=[c[1] for c in currencies], k=1)[0]
    
    def payment_date(self, order_date: datetime, payment_method: str) -> datetime:
        """Generate payment date based on order date and payment method."""
        # Most payments happen immediately, but some take time
        if payment_method in ['paypal', 'apple_pay', 'google_pay']:
            # Digital payments are usually instant
            payment_delay = random.choices([0, 0, 0, 1], weights=[80, 10, 5, 5])[0]
        else:
            # Card payments might take 1-3 days
            payment_delay = random.choices([0, 1, 2, 3], weights=[50, 30, 15, 5])[0]
        
        payment_date = order_date + timedelta(days=payment_delay)
        
        # Don't process payments on weekends for some methods
        if payment_method in ['credit_card', 'debit_card']:
            while payment_date.weekday() >= 5:
                payment_date += timedelta(days=1)
        
        return payment_date


class PaymentGenerator:
    """Generates synthetic payment data with realistic patterns."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.faker = Faker('en_US')
        self.faker.add_provider(PaymentProvider)
        
        # Transaction fees by payment method
        self.transaction_fees = {
            'credit_card': 0.029,  # 2.9%
            'debit_card': 0.015,   # 1.5%
            'paypal': 0.034,       # 3.4%
            'apple_pay': 0.026,    # 2.6%
            'google_pay': 0.026    # 2.6%
        }
        
        # Chargeback rates by payment method
        self.chargeback_rates = {
            'credit_card': 0.015,  # 1.5%
            'debit_card': 0.008,   # 0.8%
            'paypal': 0.005,       # 0.5%
            'apple_pay': 0.002,    # 0.2%
            'google_pay': 0.002    # 0.2%
        }
        
        # Payment attempt probabilities
        self.payment_attempt_probabilities = {
            'completed': 0.95,      # 95% of completed orders have payment attempts
            'pending': 0.60,        # 60% of pending orders have payment attempts
            'cancelled': 0.40,      # 40% of cancelled orders have payment attempts
            'refunded': 0.90        # 90% of refunded orders have payment attempts
        }
    
    def generate_batch(self, batch_size: int, batch_num: int, order_data: List[Dict]) -> List[Dict]:
        """Generate a batch of payment records with realistic scenarios."""
        batch_data = []
        
        # Create lookup dictionary for faster access
        order_lookup = {o['order_id']: o for o in order_data}
        
        # Track payment attempts per order to prevent duplicate successes
        order_payment_history = {}
        
        for i in range(batch_size):
            payment_id = self.config['start_id'] + (batch_num * self.config['batch_size']) + i
            
            # Select order for payment with realistic probability
            order_id = self._select_order_for_payment(order_data, order_payment_history)
            
            if order_id is None:
                # No orders need payments, skip this iteration
                continue
                
            order = order_lookup[order_id]
            payment_history = order_payment_history.get(order_id, [])
            
            # Generate payment based on order status and payment history
            payment_data = self._generate_payment_for_order(
                order, payment_history, payment_id, i, batch_num
            )
            
            # Update payment history
            order_payment_history[order_id] = payment_history + [payment_data['payment_status']]
            
            batch_data.append(payment_data)
        
        return batch_data
    
    def _select_order_for_payment(self, order_data: List[Dict], order_payment_history: Dict) -> Optional[int]:
        """Select order for payment based on realistic probabilities."""
        eligible_orders = []
        
        for order in order_data:
            order_id = order['order_id']
            order_status = order['order_status']
            payment_history = order_payment_history.get(order_id, [])
            
            # Check if order already has successful payment
            if 'success' in payment_history:
                continue  # Skip orders that already have successful payments
            
            # Determine probability of payment attempt based on order status
            attempt_probability = self.payment_attempt_probabilities.get(order_status, 0.5)
            
            # Check if this order should have a payment attempt
            if random.random() < attempt_probability:
                eligible_orders.append(order_id)
        
        if not eligible_orders:
            return None  # No orders need payments
        
        return random.choice(eligible_orders)
    
    def _generate_payment_for_order(self, order: Dict, payment_history: List[str], 
                                   payment_id: int, payment_index: int, batch_num: int) -> Dict:
        """Generate payment record for a specific order with realistic retry logic."""
        
        customer_segment = order['customer_segment']
        order_status = order['order_status']
        order_amount = order['total_amount']
        order_date = datetime.strptime(order['order_date'], '%Y-%m-%d')
        
        # Determine payment attempt type based on history
        if not payment_history:
            # First payment attempt
            payment_data = self._generate_first_payment_attempt(
                order, payment_id, payment_index, batch_num
            )
        else:
            # Retry payment attempt
            payment_data = self._generate_retry_payment_attempt(
                order, payment_history, payment_id, payment_index, batch_num
            )
        
        return payment_data
    
    def _generate_first_payment_attempt(self, order: Dict, payment_id: int, 
                                       payment_index: int, batch_num: int) -> Dict:
        """Generate first payment attempt for an order."""
        
        customer_segment = order['customer_segment']
        order_status = order['order_status']
        order_amount = order['total_amount']
        order_date = datetime.strptime(order['order_date'], '%Y-%m-%d')
        
        # Generate payment method based on customer segment
        payment_method = self.faker.payment_method(customer_segment)
        
        # Determine success probability based on order status and customer segment
        if order_status == 'completed':
            # High success rate for completed orders
            success_prob = 0.85 if customer_segment == 'premium' else 0.75
        elif order_status == 'pending':
            # Lower success rate for pending orders
            success_prob = 0.60
        elif order_status == 'cancelled':
            # Very low success rate for cancelled orders
            success_prob = 0.20
        else:  # refunded
            success_prob = 0.90
        
        # Generate payment status
        if random.random() < success_prob:
            payment_status = 'success'
        else:
            payment_status = random.choices(
                population=['failed', 'pending'],
                weights=[0.8, 0.2],
                k=1
            )[0]
        
        # Generate other payment data
        payment_gateway = self.faker.payment_gateway(payment_method)
        currency_code = self.faker.currency_code()
        payment_date = self.faker.payment_date(order_date, payment_method)
        
        # Calculate transaction fee
        transaction_fee_rate = self.transaction_fees[payment_method]
        transaction_fee = order_amount * transaction_fee_rate
        
        # Generate fraud score
        fraud_score = self.faker.fraud_score(payment_method, customer_segment, order_amount)
        
        # Determine if this payment should have a chargeback (only for successful payments)
        chargeback_amount = 0.0
        chargeback_date = None
        chargeback_reason = None
        
        if payment_status == 'success' and random.random() < self.chargeback_rates[payment_method]:
            chargeback_amount = order_amount
            chargeback_date = payment_date + timedelta(days=random.randint(1, 90))
            chargeback_reasons = [
                ('fraudulent', 0.40),
                ('not_received', 0.30),
                ('defective', 0.20),
                ('cancelled', 0.10)
            ]
            chargeback_reason = random.choices(
                population=[r[0] for r in chargeback_reasons],
                weights=[r[1] for r in chargeback_reasons],
                k=1
            )[0]
        
        # Generate payment reference
        payment_reference = self.faker.bothify(text='PAY-########')
        
        return {
            'payment_id': payment_id,
            'payment_uuid': self.faker.uuid4(),
            'order_id': order['order_id'],
            'customer_id': order['customer_id'],
            'customer_segment': customer_segment,
            'payment_method': payment_method,
            'payment_status': payment_status,
            'payment_gateway': payment_gateway,
            'payment_date': payment_date.strftime('%Y-%m-%d'),
            'payment_time': payment_date.strftime('%H:%M:%S'),
            'currency_code': currency_code,
            'order_amount': round(order_amount, 2),
            'transaction_fee_rate': transaction_fee_rate,
            'transaction_fee': round(transaction_fee, 2),
            'net_amount': round(order_amount - transaction_fee, 2),
            'fraud_score': fraud_score,
            'chargeback_amount': round(chargeback_amount, 2),
            'chargeback_date': chargeback_date.strftime('%Y-%m-%d') if chargeback_date else None,
            'chargeback_reason': chargeback_reason,
            'payment_reference': payment_reference,
            'payment_metadata': {
                'ip_address': self.faker.ipv4(),
                'user_agent': self.faker.user_agent(),
                'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                'browser': random.choice(['chrome', 'firefox', 'safari', 'edge'])
            },
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def _generate_retry_payment_attempt(self, order: Dict, payment_history: List[str],
                                       payment_id: int, payment_index: int, batch_num: int) -> Dict:
        """Generate retry payment attempt for an order."""
        
        customer_segment = order['customer_segment']
        order_status = order['order_status']
        order_amount = order['total_amount']
        order_date = datetime.strptime(order['order_date'], '%Y-%m-%d')
        
        # Generate payment method (might be different from previous attempts)
        payment_method = self.faker.payment_method(customer_segment)
        
        # Retry logic: failed payments have higher chance of success on retry
        # but decreasing with each attempt
        retry_count = len(payment_history)
        base_success_prob = 0.60
        
        # Adjust success probability based on retry count
        success_prob = base_success_prob * (0.9 ** retry_count)
        
        # Minimum success probability
        success_prob = max(success_prob, 0.20)
        
        # Generate payment status
        if random.random() < success_prob:
            payment_status = 'success'
        else:
            payment_status = random.choices(
                population=['failed', 'pending'],
                weights=[0.9, 0.1],
                k=1
            )[0]
        
        # Generate other payment data (similar to first attempt)
        payment_gateway = self.faker.payment_gateway(payment_method)
        currency_code = self.faker.currency_code()
        payment_date = self.faker.payment_date(order_date, payment_method)
        
        # Calculate transaction fee
        transaction_fee_rate = self.transaction_fees[payment_method]
        transaction_fee = order_amount * transaction_fee_rate
        
        # Generate fraud score (slightly higher for retry attempts)
        fraud_score = self.faker.fraud_score(payment_method, customer_segment, order_amount)
        fraud_score = min(fraud_score + 0.05, 1.0)  # Slightly higher risk for retries
        
        # Determine if this payment should have a chargeback
        chargeback_amount = 0.0
        chargeback_date = None
        chargeback_reason = None
        
        if payment_status == 'success' and random.random() < self.chargeback_rates[payment_method]:
            chargeback_amount = order_amount
            chargeback_date = payment_date + timedelta(days=random.randint(1, 90))
            chargeback_reasons = [
                ('fraudulent', 0.40),
                ('not_received', 0.30),
                ('defective', 0.20),
                ('cancelled', 0.10)
            ]
            chargeback_reason = random.choices(
                population=[r[0] for r in chargeback_reasons],
                weights=[r[1] for r in chargeback_reasons],
                k=1
            )[0]
        
        # Generate payment reference
        payment_reference = self.faker.bothify(text='PAY-########')
        
        return {
            'payment_id': payment_id,
            'payment_uuid': self.faker.uuid4(),
            'order_id': order['order_id'],
            'customer_id': order['customer_id'],
            'customer_segment': customer_segment,
            'payment_method': payment_method,
            'payment_status': payment_status,
            'payment_gateway': payment_gateway,
            'payment_date': payment_date.strftime('%Y-%m-%d'),
            'payment_time': payment_date.strftime('%H:%M:%S'),
            'currency_code': currency_code,
            'order_amount': round(order_amount, 2),
            'transaction_fee_rate': transaction_fee_rate,
            'transaction_fee': round(transaction_fee, 2),
            'net_amount': round(order_amount - transaction_fee, 2),
            'fraud_score': round(fraud_score, 3),
            'chargeback_amount': round(chargeback_amount, 2),
            'chargeback_date': chargeback_date.strftime('%Y-%m-%d') if chargeback_date else None,
            'chargeback_reason': chargeback_reason,
            'payment_reference': payment_reference,
            'payment_metadata': {
                'ip_address': self.faker.ipv4(),
                'user_agent': self.faker.user_agent(),
                'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                'browser': random.choice(['chrome', 'firefox', 'safari', 'edge'])
            },
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def validate_data(self, data: List[Dict]) -> bool:
        """Validate generated payment data."""
        if not data:
            return False
        
        # Check for required fields
        required_fields = ['payment_id', 'order_id', 'payment_method', 'payment_status', 'order_amount']
        for record in data:
            for field in required_fields:
                if not record.get(field):
                    logger.error(f"Missing required field: {field} in record {record.get('payment_id')}")
                    return False
            
            # Validate amounts
            if record['order_amount'] <= 0:
                logger.error(f"Invalid order amount: {record['order_amount']} in record {record.get('payment_id')}")
                return False
        
        # Validate realistic payment scenarios
        if not self._validate_payment_scenarios(data):
            return False
        
        return True
    
    def _validate_payment_scenarios(self, payment_data: List[Dict]) -> bool:
        """Validate realistic payment scenarios and business rules."""
        
        # Group payments by order
        order_payments = {}
        for payment in payment_data:
            order_id = payment['order_id']
            if order_id not in order_payments:
                order_payments[order_id] = []
            order_payments[order_id].append(payment['payment_status'])
        
        # Validate scenarios
        for order_id, payment_statuses in order_payments.items():
            # Rule 1: Maximum one successful payment per order
            success_count = payment_statuses.count('success')
            if success_count > 1:
                logger.error(f"Order {order_id} has {success_count} successful payments (should be max 1)")
                return False
            
            # Rule 2: Failed payments should eventually succeed or be abandoned
            if 'failed' in payment_statuses and 'success' not in payment_statuses:
                # This is valid - order was abandoned after failed payment
                pass
            
            # Rule 3: Pending payments should eventually succeed, fail, or remain pending
            if 'pending' in payment_statuses:
                # This is valid - payment is still processing
                pass
            
            # Rule 4: Retry patterns should be realistic (decreasing success probability)
            failed_count = payment_statuses.count('failed')
            if failed_count > 3:
                # Too many failed attempts is unrealistic
                logger.warning(f"Order {order_id} has {failed_count} failed payments (unrealistic retry pattern)")
        
        logger.info(f"Payment scenario validation passed: {len(order_payments)} orders processed")
        return True
    
    def save_batch(self, data: List[Dict], batch_num: int, output_format: str = 'csv') -> str:
        """Save a batch of payment data to file."""
        if not self.validate_data(data):
            raise ValueError("Data validation failed")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"payments_batch_{batch_num:04d}_{timestamp}"
        
        output_path = Path(self.config['output_dir'])
        output_path.mkdir(parents=True, exist_ok=True)
        
        if output_format.lower() == 'csv':
            # Flatten payment metadata for CSV output
            flattened_data = []
            for payment in data:
                payment_copy = payment.copy()
                metadata = payment_copy.pop('payment_metadata', {})
                
                # Add payment-level data
                flattened_data.append({**payment_copy, **metadata})
            
            filepath = output_path / f"{filename}.csv"
            df = pd.DataFrame(flattened_data)
            df.to_csv(filepath, index=False, encoding='utf-8')
            
        elif output_format.lower() == 'json':
            filepath = output_path / f"{filename}.json"
            pd.DataFrame(data).to_json(filepath, orient='records', date_format='iso')
            
        elif output_format.lower() == 'parquet':
            # Flatten for Parquet format
            flattened_data = []
            for payment in data:
                payment_copy = payment.copy()
                metadata = payment_copy.pop('payment_metadata', {})
                
                flattened_data.append({**payment_copy, **metadata})
            
            filepath = output_path / f"{filename}.parquet"
            df = pd.DataFrame(flattened_data)
            df.to_parquet(filepath, index=False)
            
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        logger.info(f"Saved batch {batch_num} to {filepath}")
        return str(filepath)
    
    def generate_single_payment(self, payment_id: int = None, order_id: int = None) -> Dict:
        """Generate a single payment record for API use using generate_batch method."""
        if payment_id is None:
            payment_id = self.faker.random_int(min=1, max=999999999)
        
        # Temporarily set batch_size to 1 for single payment generation
        original_batch_size = self.config.get('batch_size', 1)
        self.config['batch_size'] = 1
        
        try:
            # Load real order data using the existing function
            order_data = load_order_data(
                self.config.get('order_data', '../../data/raw/orders'),
                self.config.get('order_format', 'json')
            )
            
            # Use generate_batch method with batch size 1
            batch_data = self.generate_batch(batch_size=1, batch_num=0, order_data=order_data)
            
            # Extract the single payment record from the batch
            payment_data = batch_data[0]
            
            # Override the payment_id if provided
            if payment_id is not None:
                payment_data['payment_id'] = payment_id
            
            return payment_data
        finally:
            # Restore original batch_size
            self.config['batch_size'] = original_batch_size

    def generate_payments(self, order_data: List[Dict]) -> Dict[str, int]:
        """Generate all payment data in batches."""
        total_records = self.config['total_records']
        batch_size = self.config['batch_size']
        output_format = self.config['output_format']
        
        total_batches = (total_records + batch_size - 1) // batch_size
        files_created = []
        total_records_generated = 0
        
        logger.info(f"Starting payment generation: {total_records:,} records in {total_batches} batches")
        logger.info(f"Using {len(order_data):,} orders")
        
        for batch_num in range(total_batches):
            current_batch_size = min(batch_size, total_records - total_records_generated)
            
            logger.info(f"Generating batch {batch_num + 1}/{total_batches} ({current_batch_size:,} records)")
            
            try:
                batch_data = self.generate_batch(current_batch_size, batch_num, order_data)
                filepath = self.save_batch(batch_data, batch_num, output_format)
                
                files_created.append(filepath)
                total_records_generated += len(batch_data)
                
                # Log progress
                progress = (total_records_generated / total_records) * 100
                logger.info(f"Progress: {progress:.1f}% ({total_records_generated:,}/{total_records:,})")
                
            except Exception as e:
                logger.error(f"Error generating batch {batch_num}: {str(e)}")
                raise
        
        logger.info(f"Payment generation completed: {total_records_generated:,} records in {len(files_created)} files")
        
        return {
            'total_records': total_records_generated,
            'files_created': len(files_created),
            'files': files_created
        }


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate synthetic payment data')
    parser.add_argument('--total-records', type=int, default=500000, 
                       help='Total number of payment records to generate (default: 500,000)')
    parser.add_argument('--batch-size', type=int, default=25000,
                       help='Number of records per batch (default: 25,000)')
    parser.add_argument('--output-dir', type=str, default='data/raw/payments',
                       help='Output directory for generated files (default: data/raw/payments)')
    parser.add_argument('--output-format', type=str, choices=['csv', 'json', 'parquet'], 
                       default='csv', help='Output file format (default: csv)')
    parser.add_argument('--start-id', type=int, default=1,
                       help='Starting payment ID (default: 1)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducible results (default: 42)')
    parser.add_argument('--order-data', type=str, default='data/raw/orders',
                       help='Path to order data directory (default: data/raw/orders)')
    parser.add_argument('--order-format', type=str, choices=['csv', 'json', 'parquet'], 
                       default='csv', help='Order data file format (default: csv)')
    
    return parser.parse_args()


def load_order_data(order_dir: str, order_format: str = 'csv') -> List[Dict]:
    """Load order data for payment generation."""
    logger.info(f"Loading order data from {order_dir} (format: {order_format})")
    
    # Load order data
    order_pattern = f"*.{order_format}"
    order_files = list(Path(order_dir).glob(order_pattern))
    if not order_files:
        raise ValueError(f"No order data files found in {order_dir} with pattern {order_pattern}")
    
    order_data = []
    for file_path in order_files:
        if order_format == 'csv':
            df = pd.read_csv(file_path)
        elif order_format == 'json':
            df = pd.read_json(file_path, orient='records')
        elif order_format == 'parquet':
            df = pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported order data format: {order_format}")
        order_data.extend(df.to_dict('records'))
    
    logger.info(f"Loaded {len(order_data):,} orders")
    
    return order_data


def main():
    """Main execution function."""
    args = parse_arguments()
    
    # Set random seed for reproducibility
    random.seed(args.seed)
    
    # Load order data
    try:
        order_data = load_order_data(args.order_data, args.order_format)
    except Exception as e:
        logger.error(f"Failed to load order data: {str(e)}")
        sys.exit(1)
    
    # Configuration
    config = {
        'total_records': args.total_records,
        'batch_size': args.batch_size,
        'output_dir': args.output_dir,
        'output_format': args.output_format,
        'start_id': args.start_id
    }
    
    logger.info(f"Payment generation configuration: {config}")
    
    # Create generator and run
    generator = PaymentGenerator(config)
    
    try:
        results = generator.generate_payments(order_data)
        logger.info(f"Generation completed successfully:")
        logger.info(f"  Total records: {results['total_records']:,}")
        logger.info(f"  Files created: {results['files_created']}")
        
    except Exception as e:
        logger.error(f"Payment generation failed: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()