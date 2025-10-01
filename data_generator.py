import sys
import json
import optional_faker as _
import uuid
import random
import boto3


from dotenv import load_dotenv
from faker import Faker
from datetime import date, datetime

load_dotenv()
fake = Faker()

inventory = [
    {"product_id": "P001", "name": "Monogram Classic Tote", "price": 245.00},
    {"product_id": "P002", "name": "Monogram Vintage Satchel", "price": 189.99},
    {"product_id": "P003", "name": "Monogram Leather Crossbody", "price": 156.50},
    {"product_id": "P004", "name": "Monogram Canvas Backpack", "price": 198.75},
    {"product_id": "P005", "name": "Monogram Evening Clutch", "price": 89.99},
    {"product_id": "P006", "name": "Monogram Travel Duffle", "price": 320.00},
    {"product_id": "P007", "name": "Monogram Mini Handbag", "price": 125.50},
    {"product_id": "P008", "name": "Monogram Messenger Bag", "price": 175.25},
    {"product_id": "P009", "name": "Monogram Hobo Bag", "price": 210.00},
    {"product_id": "P010", "name": "Monogram Bucket Bag", "price": 167.80},
    {"product_id": "P011", "name": "Monogram Weekend Bag", "price": 285.60},
    {"product_id": "P012", "name": "Monogram Belt Bag", "price": 98.75},
    {"product_id": "P013", "name": "Monogram Shoulder Bag", "price": 145.30},
    {"product_id": "P014", "name": "Monogram Chain Bag", "price": 220.90},
    {"product_id": "P015", "name": "Monogram Vintage Briefcase", "price": 340.00}
]

channels = ["Online", "Boutique", "Pop-up"]
countries = ["France", "United States", "United Kingdom", "Germany", "Italy", "Spain", "Canada", "Australia", "Japan", "Netherlands"]    


def generate_sales_data():
    global inventory, fake, channels, countries
    
    product = fake.random_element(elements=inventory)
    quantity = random.choices([1, 1, 1, 2, 2, 3], weights=[60, 20, 10, 5, 3, 2])[0]
    unit_price = product["price"]
    total_amount = round(quantity * unit_price, 2)
    
    sale_id = f"S{fake.random_int(min=1, max=9999):03d}"
    
    customer_id = f"C{fake.random_int(min=1, max=9999):03d}"
    
    sales_data = {
        'sale_id': sale_id,
        'sale_date': fake.date_between(start_date='-2y', end_date='today').isoformat(),
        'customer_id': customer_id,
        'product_id': product["product_id"],
        'product_name': product["name"],
        'quantity': quantity,
        'unit_price': unit_price,
        'total_amount': total_amount,
        'channel': fake.random_element(elements=channels),
        'country': fake.random_element(elements=countries)
    }
    
    d = json.dumps(sales_data) + '\n'
    sys.stdout.write(d)


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        generate_sales_data()
    print('')