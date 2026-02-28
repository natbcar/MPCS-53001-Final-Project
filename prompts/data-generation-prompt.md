### TASK
Write a Python script using `faker`, `mysql-connector`, `pymongo`, and `redis-py` 
That populates an e-commerce database using the information contained in this document. 

### Order of Operations
Generate the data sequentually in the following order to maintain consistency between data sources.

    1. Entity 
    First we must populate the entity tables. These are either directly involved in events (e.g user buys product, user returns product) or link to those attributes (address/category)
        a. address (type = MySQL): address will be linked to user_id and shipping table
        b. users (type = MySQL): Users must be generated first since they are the basis for all events and actions in the system
        b. product, product_variant & category (type = MySQL): generate according to the brand, category, and volume specfications in the GENERATION VARIABLES (JSON) section
        d. product_specs (type = MongoDB): This should link to the sku key in the product_variant table and should list an arbitrary number of attributes based on the product.

    2. User Events
    This step simulates user event activity in the ecommerce system and populates the following data sources based on said activity. 

    cart, cart_items, order, payment, shipping, return, return_items, and event_log

    The following events can occur in this simulation. These are all captured in Redis

    | event_type | User Action | Redis action | SQL action | 
    --------------------------------------------------------
    | VIEW_PRODUCT | User clicks a product | LPUSH product SKU to user:{id}:recent_views | store to event_log | 
    | ADD_TO_CART | User adds product to cart | HSET SKU and Quantity to user:{id}:cart | store to event_log | 
    | REMOVE_FROM_CART | User removes product from cart | HDEL SKU from user:{id}:cart | store to event_log | 
    | CHECKOUT | User buys the items in cart | HGETALL user:{id}:cart then DEL cart | INSERT to cart, and cart_items and trigger generation for order, payment, shipping |

    3. Returns
    After all orders are generated, loop back through and populate the returns table. Generate returns on around 10% of the orders in the data.


NOTE: To maintain cross data source integrity make sure that the following mappings are upheld.
    - users.user_id must map to user:{id} in REDIS
    - product_variant.sku must map to sku in MongoDB

When simulating user actions adhere to the following generation logic principles
    - 10:1 Rule: For every 10 VIEW events, there should be roughly 1 ADD_TO_CART
    - The 5:1 Rule: For every 5 ADD_TO_CART events there should be 1 CHECKOUT
    - Power Law: Assign 80% of the 500k events to the top 10% of products
    - User Behavior Profiles: Define 3 types of users
        - The Browser: ~50-100 VIEW events to 0-1 PURCHASE
        - The Targeter: 5-10 VIEW events to 1 PURCHASE
        - The Returned 5 PURCHASE events to 4 RETURN events


### Technical Instructions
Use pipeline() in Redis: For 500,000 events, individual SET commands will be too slow. Piping commands reduces network overhead.

Use executemany() in MySQL: To batch the 100,000 orders.

Use insert_many() in MongoDB: To batch the 5,000 product specs.


### SYSTEM ARCHITECTURE
- SQL (MySQL): Transactional Truth (Orders, Users, Inventory)
- Document (MongoDB): Product Meta (Rich attributes, Tech Specs)
- Key-Value (Redis): Session/Cache (Active carts, Recent views)

### DATA SCHEMAS
#### Relational DB (MySQL)
CREATE TABLE categories (
    category_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE address (
    address_id INT PRIMARY KEY,
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    zip INT
);

-- 2. User & Product Base Tables
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    address_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    CONSTRAINT fk_user_address FOREIGN KEY (address_id) REFERENCES address(address_id)
);

CREATE TABLE product (
    product_id INT PRIMARY KEY,
    category_id INT,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    brand VARCHAR(100),
    manufacturer VARCHAR(100),
    CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

-- 3. Variant & Session Management
CREATE TABLE product_variant (
    sku INT PRIMARY KEY, -- Linked to MongoDB for rich attributes
    product_id INT,
    price DOUBLE NOT NULL,
    status ENUM('available', 'out_of_stock', 'discontinued'),
    stock_quantity INT DEFAULT 0,
    CONSTRAINT fk_variant_product FOREIGN KEY (product_id) REFERENCES product(product_id)
);

CREATE TABLE session (
    session_id INT PRIMARY KEY,
    user_id INT,
    device_type VARCHAR(50), -- To track tablet vs laptop usage
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    CONSTRAINT fk_session_user FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 4. Cart Logic
CREATE TABLE cart (
    cart_id INT PRIMARY KEY,
    user_id INT,
    status ENUM('active', 'abandoned', 'converted'),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_cart_user FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE cart_items (
    cart_item_id INT PRIMARY KEY,
    cart_id INT,
    sku INT,
    quantity INT DEFAULT 1,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_items_cart FOREIGN KEY (cart_id) REFERENCES cart(cart_id),
    CONSTRAINT fk_items_sku FOREIGN KEY (sku) REFERENCES product_variant(sku)
);

-- 5. Orders & Shipping
CREATE TABLE shipments (
    shipment_id INT PRIMARY KEY,
    address_id INT,
    courier VARCHAR(100),
    shipped_at TIMESTAMP NULL,
    delivered_at TIMESTAMP NULL,
    CONSTRAINT fk_shipment_address FOREIGN KEY (address_id) REFERENCES address(address_id)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    cart_id INT,
    user_id INT,
    shipment_id INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_order_cart FOREIGN KEY (cart_id) REFERENCES cart(cart_id),
    CONSTRAINT fk_order_user FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT fk_order_shipment FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id)
);

-- 6. Returns Management
CREATE TABLE returns (
    return_id INT PRIMARY KEY,
    order_id INT,
    shipment_id INT,
    status ENUM('initiated', 'received', 'refunded'),
    CONSTRAINT fk_return_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_return_shipment FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id)
);

CREATE TABLE return_items (
    return_item_id INT PRIMARY KEY,
    return_id INT,
    sku INT,
    CONSTRAINT fk_ret_items_parent FOREIGN KEY (return_id) REFERENCES returns(return_id),
    CONSTRAINT fk_ret_items_sku FOREIGN KEY (sku) REFERENCES product_variant(sku)
);

-- 7. Event Log
CREATE TABLE event_log (
    event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    event_type ENUM('view', 'cart_add', 'cart_remove', 'checkout'),
    sku INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES user(user_id)
);

#### NoSQL MongoDB
{
  "sku": "SNY-WH1000-BLK",
  "specs": {
    "battery_life": "30 hours",
    "noise_canceling": "Active",
    "weight": "254g",
    "color": "Midnight Black"
  }
}

#### Key-Value (Redis)
KEY: user:{id}:session -> Value: Current device, last active timestamp
KEY: user:{id}:recent_views -> Value: List of SKUs
Key: user:{id}:cart -> Value: Hash of SKU and Quantity

### GENERATION VARIABLES (JSON)
{
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
  "couriers": [
    {"name": "FedEx", "type": "Express", "cost_multiplier": 1.5},
    {"name": "UPS", "type": "Standard", "cost_multiplier": 1.0},
    {"name": "DHL", "type": "International", "cost_multiplier": 2.0},
    {"name": "GoBolt", "type": "Eco-Friendly", "cost_multiplier": 1.2}
  ]
  "volume": {"users": 1000, "products": 5000, "orders": 100000, "events": 500000}
}