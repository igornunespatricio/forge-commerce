#!/usr/bin/env python3
"""
Product Data Generation Script

Generates synthetic product data for the e-commerce data warehouse.
Follows the .clinerules for data generation best practices.

Target: 100K-1M products with realistic categories, pricing,
inventory levels, and supplier relationships.

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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tools.storage_client import StorageClient, StorageClientFactory, upload_csv, upload_json, upload_parquet

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/generate_products.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ProductProvider(BaseProvider):
    """Custom Faker provider for e-commerce product data."""
    
    def product_category(self) -> str:
        """Generate product category with realistic distribution."""
        categories = [
            ('electronics', 0.25),      # 25% electronics
            ('clothing', 0.20),         # 20% clothing
            ('home_garden', 0.18),      # 18% home & garden
            ('books_media', 0.12),      # 12% books & media
            ('sports_outdoors', 0.10),  # 10% sports & outdoors
            ('beauty_health', 0.08),    # 8% beauty & health
            ('toys_games', 0.07)        # 7% toys & games
        ]
        return random.choices(population=[c[0] for c in categories], 
                            weights=[c[1] for c in categories], k=1)[0]
    
    def product_subcategory(self, category: str) -> str:
        """Generate product subcategory based on main category."""
        subcategories = {
            'electronics': [
                'smartphones', 'laptops', 'headphones', 'tablets', 'smartwatches',
                'cameras', 'gaming_consoles', 'tv_audio', 'accessories'
            ],
            'clothing': [
                'men_clothing', 'women_clothing', 'shoes', 'accessories',
                'activewear', 'undergarments', 'outerwear'
            ],
            'home_garden': [
                'furniture', 'decor', 'kitchen', 'bedding', 'bath',
                'tools', 'garden', 'appliances'
            ],
            'books_media': [
                'books', 'ebooks', 'movies', 'music', 'video_games',
                'software', 'magazines'
            ],
            'sports_outdoors': [
                'fitness', 'outdoor_gear', 'team_sports', 'water_sports',
                'winter_sports', 'cycling', 'hunting_fishing'
            ],
            'beauty_health': [
                'skincare', 'makeup', 'hair_care', 'fragrances',
                'vitamins', 'personal_care', 'tools_accessories'
            ],
            'toys_games': [
                'action_figures', 'dolls', 'educational', 'board_games',
                'puzzles', 'video_games', 'outdoor_toys'
            ]
        }
        return random.choice(subcategories.get(category, ['general']))
    
    def supplier_name(self) -> str:
        """Generate supplier name."""
        prefixes = ['Global', 'International', 'Premium', 'Quality', 'Elite', 'Pro', 'Tech', 'Eco']
        suffixes = ['Suppliers', 'Distributors', 'Manufacturing', 'Trading', 'Imports', 'Exports', 'Corp', 'Ltd']
        return f"{random.choice(prefixes)} {random.choice(suffixes)}"
    
    def product_price(self, category: str) -> float:
        """Generate product price based on category."""
        # Price ranges by category (mean, std_dev)
        price_ranges = {
            'electronics': (250.0, 400.0),      # $250 avg, high variance
            'clothing': (45.0, 60.0),           # $45 avg
            'home_garden': (85.0, 150.0),       # $85 avg
            'books_media': (25.0, 20.0),        # $25 avg, low variance
            'sports_outdoors': (65.0, 100.0),   # $65 avg
            'beauty_health': (35.0, 45.0),      # $35 avg
            'toys_games': (30.0, 50.0)          # $30 avg
        }
        
        mean, std_dev = price_ranges.get(category, (50.0, 75.0))
        price = max(5.0, random.lognormvariate(np.log(mean), 0.8))  # Minimum $5
        return round(price, 2)
    
    def inventory_level(self, category: str) -> int:
        """Generate inventory level based on category popularity."""
        # Average inventory levels by category
        inventory_ranges = {
            'electronics': (50, 500),        # High value, lower inventory
            'clothing': (100, 1000),         # Medium value, medium inventory
            'home_garden': (75, 750),        # Medium value, medium inventory
            'books_media': (200, 2000),      # Low value, high inventory
            'sports_outdoors': (100, 800),   # Medium value, medium inventory
            'beauty_health': (150, 1200),    # Low-medium value, high inventory
            'toys_games': (120, 900)         # Low-medium value, high inventory
        }
        
        min_inv, max_inv = inventory_ranges.get(category, (50, 500))
        return random.randint(min_inv, max_inv)
    
    def product_rating(self) -> float:
        """Generate product rating with realistic distribution."""
        # Most products have ratings between 3.5 and 4.5
        rating = max(1.0, min(5.0, random.normalvariate(4.0, 0.5)))
        return round(rating, 1)


class ProductGenerator:
    """Generates synthetic product data with realistic patterns."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.faker = Faker('en_US')
        self.faker.add_provider(ProductProvider)

        # MinIO configuration
        self.storage_client = StorageClientFactory.create_minio_client(config)
        
        # Supplier distribution
        self.suppliers = [
            ('TechCorp Electronics', 'US', 0.15),
            ('Global Fashion Inc', 'CN', 0.12),
            ('Home Essentials Ltd', 'DE', 0.10),
            ('Book World Distributors', 'GB', 0.08),
            ('Sports & Outdoors Co', 'US', 0.09),
            ('Beauty Solutions Group', 'FR', 0.07),
            ('Toy Masters Inc', 'CN', 0.08),
            ('Eco Products Ltd', 'SE', 0.06),
            ('Premium Imports', 'HK', 0.07),
            ('Quality Manufacturing', 'JP', 0.05),
            ('Local Artisans Co', 'US', 0.06),
            ('International Trading', 'SG', 0.07)
        ]
        
        # Brand names by category
        self.brands = {
            'electronics': [
                'TechPro', 'GigaTech', 'PowerMax', 'SmartLife', 'UltraView',
                'Quantum', 'Nexus', 'AeroTech', 'VisionX', 'EchoSound'
            ],
            'clothing': [
                'UrbanStyle', 'ClassicWear', 'ActiveFit', 'ElegantLine', 'ComfortZone',
                'TrendSetters', 'Heritage', 'ModernEdge', 'PureComfort', 'StyleMasters'
            ],
            'home_garden': [
                'HomeComfort', 'GardenPro', 'KitchenEssentials', 'DecorMasters', 'FurniturePlus',
                'ToolTech', 'ApplianceWorld', 'LivingSpace', 'CozyHome', 'GreenThumb'
            ],
            'books_media': [
                'StoryWorld', 'KnowledgeBase', 'EntertainmentHub', 'CreativeWorks', 'LearningCenter',
                'MediaPro', 'BookLovers', 'FilmFanatics', 'MusicMasters', 'GameZone'
            ],
            'sports_outdoors': [
                'ActiveLife', 'OutdoorPro', 'SportMax', 'FitnessFirst', 'AdventureGear',
                'TeamChampions', 'WaterSports', 'WinterSports', 'CycleMasters', 'HuntFish'
            ],
            'beauty_health': [
                'PureBeauty', 'HealthPlus', 'SkinCarePro', 'HairMasters', 'FragranceWorld',
                'VitaminLife', 'PersonalCare', 'BeautyEssentials', 'NaturalCare', 'WellnessZone'
            ],
            'toys_games': [
                'FunLand', 'PlayTime', 'EducationalToys', 'GameMasters', 'PuzzleWorld',
                'ActionHeroes', 'DollCollection', 'OutdoorFun', 'CreativePlay', 'LearningToys'
            ]
        }
    
    def generate_batch(self, batch_size: int, batch_num: int) -> List[Dict]:
        """Generate a batch of product records."""
        batch_data = []
        
        for i in range(batch_size):
            product_id = self.config['start_id'] + (batch_num * self.config['batch_size']) + i
            
            # Generate product data
            category = self.faker.product_category()
            subcategory = self.faker.product_subcategory(category)
            price = self.faker.product_price(category)
            inventory = self.faker.inventory_level(category)
            rating = self.faker.product_rating()
            
            # Select supplier with realistic distribution
            supplier_info = random.choices(
                population=self.suppliers,
                weights=[s[2] for s in self.suppliers],
                k=1
            )[0]
            supplier_name, supplier_country, _ = supplier_info
            
            # Select brand based on category
            brand = random.choice(self.brands.get(category, ['Generic']))
            
            # Generate product name
            product_name = self._generate_product_name(category, subcategory, brand)
            
            # Generate SKU and other identifiers
            sku = f"{category[:3].upper()}{product_id:08d}"
            upc = self.faker.ean13()
            
            # Calculate derived fields
            total_value = round(price * inventory, 2)
            margin = round(random.uniform(0.25, 0.60), 2)  # 25-60% margin
            cost_price = round(price * (1 - margin), 2)
            
            # Generate dates
            created_date = self.faker.date_between(start_date='-2y', end_date='today')
            last_updated = self.faker.date_between(start_date=created_date, end_date='today')
            
            product_data = {
                'product_id': product_id,
                'product_uuid': self.faker.uuid4(),
                'product_name': product_name,
                'sku': sku,
                'upc': upc,
                'category': category,
                'subcategory': subcategory,
                'brand': brand,
                'description': self.faker.text(max_nb_chars=200),
                'price': price,
                'cost_price': cost_price,
                'margin': margin,
                'inventory_level': inventory,
                'total_value': total_value,
                'supplier_name': supplier_name,
                'supplier_country': supplier_country,
                'weight': round(random.uniform(0.1, 25.0), 2),  # 0.1 to 25 kg
                'dimensions': f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(2, 50)}",  # LxWxH cm
                'color': random.choice(['black', 'white', 'blue', 'red', 'green', 'silver', 'gold', 'gray']),
                'material': random.choice(['plastic', 'metal', 'wood', 'fabric', 'glass', 'ceramic', 'leather']),
                'product_rating': rating,
                'review_count': random.randint(0, 2000),
                'is_active': self.faker.boolean(chance_of_getting_true=95),
                'is_discontinued': self.faker.boolean(chance_of_getting_true=3),
                'created_at': created_date.strftime('%Y-%m-%d'),
                'last_updated': last_updated.strftime('%Y-%m-%d'),
                'warranty_months': random.choice([0, 6, 12, 24, 36, 60]),
                'return_policy_days': random.choice([7, 14, 30, 60, 90])
            }
            
            batch_data.append(product_data)
        
        return batch_data
    
    def _generate_product_name(self, category: str, subcategory: str, brand: str) -> str:
        """Generate a realistic product name."""
        # Product name patterns by category
        name_patterns = {
            'electronics': [
                f"{brand} {random.choice(['Smart', 'Pro', 'Ultra', 'Max', 'Plus'])} {random.choice(['Phone', 'Watch', 'Speaker', 'Headphones'])}",
                f"{brand} {random.choice(['Wireless', 'Bluetooth', 'USB-C', 'Type-C'])} {random.choice(['Charger', 'Cable', 'Adapter'])}",
                f"{brand} {random.choice(['HD', '4K', '8K'])} {random.choice(['Monitor', 'Camera', 'Display'])}"
            ],
            'clothing': [
                f"{brand} {random.choice(['Classic', 'Slim', 'Regular', 'Athletic'])} {random.choice(['T-Shirt', 'Jeans', 'Jacket', 'Sweater'])}",
                f"{brand} {random.choice(['Running', 'Training', 'Casual', 'Formal'])} {random.choice(['Shoes', 'Pants', 'Shirt'])}",
                f"{brand} {random.choice(['Winter', 'Summer', 'Spring', 'Fall'])} {random.choice(['Coat', 'Dress', 'Top'])}"
            ],
            'home_garden': [
                f"{brand} {random.choice(['Comfort', 'Premium', 'Classic', 'Modern'])} {random.choice(['Sofa', 'Table', 'Chair', 'Bed'])}",
                f"{brand} {random.choice(['Electric', 'Manual', 'Cordless'])} {random.choice(['Drill', 'Sander', 'Saw'])}",
                f"{brand} {random.choice(['Non-Stick', 'Stainless', 'Cast Iron'])} {random.choice(['Pan', 'Pot', 'Set'])}"
            ],
            'books_media': [
                f"{brand} {random.choice(['Bestselling', 'Award Winning', 'Classic'])} {random.choice(['Novel', 'Biography', 'Textbook'])}",
                f"{brand} {random.choice(['Action', 'Comedy', 'Drama', 'Documentary'])} {random.choice(['Movie', 'Series', 'Film'])}",
                f"{brand} {random.choice(['Rock', 'Pop', 'Jazz', 'Classical'])} {random.choice(['Album', 'Compilation', 'Single'])}"
            ],
            'sports_outdoors': [
                f"{brand} {random.choice(['Professional', 'Amateur', 'Beginner'])} {random.choice(['Racket', 'Bat', 'Gloves', 'Shoes'])}",
                f"{brand} {random.choice(['Camping', 'Hiking', 'Fishing', 'Hunting'])} {random.choice(['Gear', 'Equipment', 'Kit'])}",
                f"{brand} {random.choice(['Inflatable', 'Folding', 'Portable'])} {random.choice(['Chair', 'Table', 'Bed'])}"
            ],
            'beauty_health': [
                f"{brand} {random.choice(['Anti-Aging', 'Moisturizing', 'Brightening'])} {random.choice(['Cream', 'Serum', 'Lotion'])}",
                f"{brand} {random.choice(['Long-Lasting', 'Waterproof', 'Matte'])} {random.choice(['Lipstick', 'Mascara', 'Eyeshadow'])}",
                f"{brand} {random.choice(['Vitamin', 'Mineral', 'Herbal'])} {random.choice(['Supplement', 'Capsule', 'Tablet'])}"
            ],
            'toys_games': [
                f"{brand} {random.choice(['Educational', 'Interactive', 'Electronic'])} {random.choice(['Toy', 'Game', 'Puzzle'])}",
                f"{brand} {random.choice(['Action', 'Doll', 'Building'])} {random.choice(['Figure', 'House', 'Blocks'])}",
                f"{brand} {random.choice(['Outdoor', 'Indoor', 'Water'])} {random.choice(['Toy', 'Game', 'Set'])}"
            ]
        }
        
        return random.choice(name_patterns.get(category, [f"{brand} Product {random.randint(100, 999)}"]))
    
    def validate_data(self, data: List[Dict]) -> bool:
        """Validate generated product data."""
        if not data:
            return False
        
        # Check for required fields
        required_fields = ['product_id', 'product_name', 'category', 'price', 'inventory_level']
        for record in data:
            for field in required_fields:
                if not record.get(field):
                    logger.error(f"Missing required field: {field} in record {record.get('product_id')}")
                    return False
            
            # Validate price and inventory
            if record['price'] <= 0:
                logger.error(f"Invalid price: {record['price']} in record {record.get('product_id')}")
                return False
            
            if record['inventory_level'] < 0:
                logger.error(f"Invalid inventory: {record['inventory_level']} in record {record.get('product_id')}")
                return False
        
        return True
    
    def save_batch(self, data: List[Dict], batch_num: int, output_format: str = 'csv') -> str:
        """Save a batch of product data to MinIO."""
        if not self.validate_data(data):
            raise ValueError("Data validation failed")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"products_batch_{batch_num:04d}_{timestamp}"
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
    
    def generate_single_product(self, product_id: int = None) -> Dict:
        """Generate a single product record for API use using generate_batch method."""
        if product_id is None:
            product_id = self.faker.random_int(min=1, max=999999999)
        
        # Temporarily set batch_size to 1 for single product generation
        original_batch_size = self.config.get('batch_size', 1)
        self.config['batch_size'] = 1
        
        try:
            # Use generate_batch method with batch size 1
            batch_data = self.generate_batch(batch_size=1, batch_num=0)
            
            # Extract the single product record from the batch
            product_data = batch_data[0]
            
            # Override the product_id if provided
            if product_id is not None:
                product_data['product_id'] = product_id
            
            return product_data
        finally:
            # Restore original batch_size
            self.config['batch_size'] = original_batch_size

    def generate_products(self) -> Dict[str, int]:
        """Generate all product data in batches."""
        total_records = self.config['total_records']
        batch_size = self.config['batch_size']
        output_format = self.config['output_format']
        
        total_batches = (total_records + batch_size - 1) // batch_size
        files_created = []
        total_records_generated = 0
        
        logger.info(f"Starting product generation: {total_records:,} records in {total_batches} batches")
        
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
        
        logger.info(f"Product generation completed: {total_records_generated:,} records in {len(files_created)} files")
        
        return {
            'total_records': total_records_generated,
            'files_created': len(files_created),
            'files': files_created
        }


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate synthetic product data')
    parser.add_argument('--total-records', type=int, default=100000, 
                       help='Total number of product records to generate (default: 100,000)')
    parser.add_argument('--batch-size', type=int, default=10000,
                       help='Number of records per batch (default: 10,000)')
    parser.add_argument('--output-format', type=str, choices=['csv', 'json', 'parquet'], 
                       default='csv', help='Output file format (default: csv)')
    parser.add_argument('--start-id', type=int, default=1,
                       help='Starting product ID (default: 1)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducible results (default: 42)')
    parser.add_argument('--bucket-name', type=str, default='forge-commerce', help='Bucket Name (default: forge-commerce)')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:9000', help='Endpoint URL (default: http://localhost:9000)')
    parser.add_argument('--filepath-prefix', type=str, default='products', help='Filepath prefix (default: products)')
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
        'start_id': args.start_id,
        'endpoint_url': 'http://localhost:9000',
        'aws_access_key_id': 'admin',
        'aws_secret_access_key': 'password',
        'bucket_name': args.bucket_name,
        'filepath_prefix': args.filepath_prefix
    }

    logger.info(f"Product generation configuration: {config}")
    
    # Create generator and run
    generator = ProductGenerator(config)
    
    try:
        results = generator.generate_products()
        logger.info(f"Generation completed successfully:")
        logger.info(f"  Total records: {results['total_records']:,}")
        logger.info(f"  Files created: {results['files_created']}")
        
    except Exception as e:
        logger.error(f"Product generation failed: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()