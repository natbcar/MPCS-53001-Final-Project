import random
import json
import uuid
from datetime import datetime, timedelta
from faker import Faker
import mysql.connector
from pymongo import MongoClient
import redis

# --- CONFIGURATION & INITIALIZATION ---
fake = Faker()

# Mock Connection setup (Replace with actual credentials)
db_config = {
    'mysql': {'host': 'localhost', 'user': 'root', 'password': '1935723Nbc!', 'database': 'mpcs53001_final_project'},
    'mongo': 'mongodb://localhost:27017/',
    'redis': {'host': 'localhost', 'port': 6379, 'db': 0}
}

# Generation Constants
GEN_VARS = {
    "categories": {
        "Electronics": ["Noise-Canceling Headphones", "Wireless Earbuds", "Smartwatches", "Portable Chargers"],
        "Fashion": ["Silk Dresses", "Vintage Denim", "Athleisure Sets", "Statement Jewelry"],
        "Home": ["Ceramic Vases", "Throw Pillows", "Smart Kettles", "Indoor Plants"]
    },
    "brands": {
        "Electronics": ["Sony", "Bose", "Sennheiser", "Apple", "Technics"],
        "Fashion": ["Everlane", "Nordstrom", "Reformation", "Nike", "Levi's"],
        "Home": ["West Elm", "Anthropologie", "Etsy Artisans", "VaseCo"]
    },
    "couriers": ["FedEx", "UPS", "DHL", "GoBolt"],
    "volume": {"users": 1000, "products": 5000, "orders": 10000, "events": 50000} # Scaled for demonstration
}

def connect_services():
    mysql_conn = mysql.connector.connect(**db_config['mysql'])
    mongo_client = MongoClient(db_config['mongo'])
    redis_client = redis.Redis(**db_config['redis'])
    return mysql_conn, mongo_client['ecommerce'], redis_client

mysql_conn, mongo_db, r = connect_services()
cursor = mysql_conn.cursor()

# --- 1. ENTITY POPULATION ---

def populate_entities():
    print("Populating Entities...")
    
    # a. Address
    addresses = []
    for i in range(1, GEN_VARS['volume']['users'] + 1):
        addresses.append((i, fake.street_address(), fake.city(), fake.state(), fake.country(), random.randint(10000, 99999)))
    cursor.executemany("INSERT INTO address VALUES (%s, %s, %s, %s, %s, %s)", addresses)

    # b. Users
    users = []
    for i in range(1, GEN_VARS['volume']['users'] + 1):
        users.append((i, i, fake.first_name(), fake.last_name(), fake.email(), "hashed_password"))
    cursor.executemany("INSERT INTO users VALUES (%s, %s, %s, %s, %s, %s)", users)

    # c. Categories, Products, & Variants
    cat_id_map = {}
    products = []
    variants = []
    mongo_specs = []
    
    sku_counter = 1000
    prod_counter = 1
    
    for idx, (cat_name, subcats) in enumerate(GEN_VARS['categories'].items()):
        cursor.execute("INSERT INTO categories VALUES (%s, %s)", (idx, cat_name))
        
        for _ in range(GEN_VARS['volume']['products'] // len(GEN_VARS['categories'])):
            brand = random.choice(GEN_VARS['brands'][cat_name])
            subcat = random.choice(subcats)
            p_id = prod_counter
            products.append((p_id, idx, f"{brand} {subcat}", fake.sentence(), brand, brand))
            
            # Create 1-3 variants per product
            for _ in range(random.randint(1, 3)):
                sku = sku_counter
                variants.append((sku, p_id, round(random.uniform(10, 500), 2), 'available', random.randint(0, 100)))
                
                # d. MongoDB Specs
                mongo_specs.append({
                    "sku": sku,
                    "specs": {
                        "color": fake.color_name(),
                        "weight": f"{random.randint(100, 1000)}g",
                        "material": "High-Grade Composite"
                    }
                })
                sku_counter += 1
            prod_counter += 1

    cursor.executemany("INSERT INTO product VALUES (%s, %s, %s, %s, %s, %s)", products)
    cursor.executemany("INSERT INTO product_variant VALUES (%s, %s, %s, %s, %s)", variants)
    mongo_db.product_specs.insert_many(mongo_specs)
    mysql_conn.commit()
    return [v[0] for v in variants] # Return list of SKUs

# --- 2. USER EVENTS & LOGIC ---

def simulate_activity(skus):
    print("Simulating User Activity...")
    all_skus = skus
    # Power Law: 80% of events go to 10% of products
    top_10_percent = all_skus[:len(all_skus)//10]
    
    order_id_counter = 1
    cart_id_counter = 1
    ship_id_counter = 1
    
    event_logs = []

    for user_id in range(1, GEN_VARS['volume']['users'] + 1):
        # Determine Profile
        profile_roll = random.random()
        if profile_roll < 0.7: profile = "Browser"
        elif profile_roll < 0.95: profile = "Targeter"
        else: profile = "Returner"

        # Set event loop count based on profile
        loops = random.randint(50, 100) if profile == "Browser" else random.randint(5, 20)
        
        redis_pipe = r.pipeline()
        
        for _ in range(loops):
            # 1. VIEW EVENT
            sku = random.choice(top_10_percent) if random.random() < 0.8 else random.choice(all_skus)
            redis_pipe.lpush(f"user:{user_id}:recent_views", sku)
            event_logs.append((user_id, 'view', sku))

            # 2. 10:1 Rule for ADD_TO_CART
            if random.random() < 0.1:
                qty = random.randint(1, 2)
                redis_pipe.hset(f"user:{user_id}:cart", sku, qty)
                event_logs.append((user_id, 'cart_add', sku))

                # 3. 5:1 Rule for CHECKOUT
                if random.random() < 0.2:
                    # Execute Checkout
                    cart_data = r.hgetall(f"user:{user_id}:cart")
                    if cart_data:
                        # SQL Actions for Checkout
                        c_id = cart_id_counter
                        s_id = ship_id_counter
                        o_id = order_id_counter
                        
                        cursor.execute("INSERT INTO cart (cart_id, user_id, status) VALUES (%s, %s, 'converted')", (c_id, user_id))
                        cursor.execute("INSERT INTO shipments (shipment_id, address_id, courier) VALUES (%s, %s, %s)", 
                                       (s_id, user_id, random.choice(GEN_VARS['couriers'])))
                        cursor.execute("INSERT INTO orders (order_id, cart_id, user_id, shipment_id) VALUES (%s, %s, %s, %s)", 
                                       (o_id, c_id, user_id, s_id))
                        
                        for s, q in cart_data.items():
                            cursor.execute("INSERT INTO cart_items (cart_item_id, cart_id, sku, quantity) VALUES (%s, %s, %s, %s)", 
                                           (random.randint(1, 10000000), c_id, int(s), int(q)))
                        
                        event_logs.append((user_id, 'checkout', None))
                        redis_pipe.delete(f"user:{user_id}:cart")
                        
                        cart_id_counter += 1
                        ship_id_counter += 1
                        order_id_counter += 1
        
        redis_pipe.execute()

    # Batch insert event logs
    cursor.executemany("INSERT INTO event_log (user_id, event_type, sku) VALUES (%s, %s, %s)", event_logs)
    mysql_conn.commit()

# --- 3. RETURNS ---

def process_returns():
    print("Processing Returns...")
    cursor.execute("SELECT order_id, shipment_id FROM orders")
    orders = cursor.fetchall()
    
    returns = []
    for order_id, ship_id in orders:
        if random.random() < 0.10: # 10% Return Rate
            return_id = random.randint(1, 1000000)
            returns.append((return_id, order_id, ship_id, 'initiated'))
            
    cursor.executemany("INSERT INTO returns VALUES (%s, %s, %s, %s)", returns)
    mysql_conn.commit()

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    try:
        available_skus = populate_entities()
        simulate_activity(available_skus)
        process_returns()
        print("Database population complete.")
    finally:
        cursor.close()
        mysql_conn.close()