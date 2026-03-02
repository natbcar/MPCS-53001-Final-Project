### TASK
Write a Python script using `faker`, `mysql-connector`, `pymongo`, and `redis-py` 
That populates an e-commerce database using the information contained in this document. 

### Order of Operations
Generate the data sequentually in the following order to maintain consistency between data sources.

    1. Entity 
    First we must populate the entity tables. These are either directly involved in events (e.g user buys product, user returns product) or link to those attributes (address/category)
        a. address (type = MySQL): address will be linked to user_id and shipping table
        b. users (type = MySQL): Users must be generated first since they are the basis for all events and actions in the system
        b. product, product_variant, inventory, & categories (type = MySQL): generate according to the brand, category, and volume specfications in the GENERATION VARIABLES (JSON) section
        d. product_specs (type = MongoDB): This should link to the sku key in the product_variant table and should list an arbitrary number of attributes based on the product.

    2. User Events
    This step simulates user event activity in the ecommerce system and populates the following data sources based on said activity. 

    cart, cart_items, orders, order_items, payment_transactions, shipments, and event_log

    The following events can occur in this simulation. These are all captured in Redis

    | event_type | User Action | Redis action | SQL action | 
    --------------------------------------------------------
    | VIEW_PRODUCT | User clicks a product | LPUSH product SKU to user:{id}:recent_views | store to event_log | 
    | ADD_TO_CART | User adds product to cart | HSET SKU and Quantity to user:{id}:cart | store to event_log | 
    | REMOVE_FROM_CART | User removes product from cart | HDEL SKU from user:{id}:cart | store to event_log | 
    | CHECKOUT | User buys the items in cart | HGETALL user:{id}:cart then DEL cart | INSERT to cart, and cart_items and trigger generation for order, payment, shipping |

    Event Generation Loop:
        - Loop through users
        - Generate 1,..,N sessions in time order
        - During a session simulate events (view, search, cart_add, cart_remove, checkout) and maintain the cart state in redis
        - On checkout write cart and cart_items, along with orders, order_items, payment_transactions, shipments. Generate data for payment_transactions and shipments while making sure they link to the transaction.
        - if no checkout by session end mark cart abandoned and write cart and cart items 
        - 

    3. Returns
    After all orders are generated, loop back through and populate the returns table. Generate returns on around 10% of the orders in the data.



NOTE: To maintain cross data source integrity make sure that the following mappings are upheld.
    - users.user_id must map to user:{id} in REDIS
    - product_variant.sku must map to sku in MongoDB

When simulating user actions adhere to the following generation logic principles
    - 10:1 Rule: For every 10 VIEW events, there should be roughly 1 ADD_TO_CART
    - The 5:1 Rule: For every 5 ADD_TO_CART events there should be 1 CHECKOUT
    - Power Law: Assign 80% of the 500k events to the top 10% of products


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

-- lookups and base entities
CREATE TABLE categories (
  category_id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE address (
  address_id INT AUTO_INCREMENT PRIMARY KEY,
  street  VARCHAR(255) NOT NULL,
  city    VARCHAR(100) NOT NULL,
  state   VARCHAR(100) NOT NULL,
  country VARCHAR(100) NOT NULL,
  zip     VARCHAR(20)  NOT NULL
);

-- users / devices / sessions
CREATE TABLE users (
  user_id INT AUTO_INCREMENT PRIMARY KEY,
  address_id INT,                       
  first_name VARCHAR(100),
  last_name  VARCHAR(100),
  email VARCHAR(255) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_user_address
    FOREIGN KEY (address_id) REFERENCES address(address_id)
);

CREATE TABLE devices (
  device_id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  device_type VARCHAR(50) NOT NULL,           -- tablet/laptop/phone/web
  last_seen_at TIMESTAMP NULL,
  CONSTRAINT fk_device_user
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE session (
  session_id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  device_id INT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP NULL,
  is_active BOOLEAN DEFAULT TRUE,
  CONSTRAINT fk_session_user
    FOREIGN KEY (user_id) REFERENCES users(user_id),
  CONSTRAINT fk_session_device
    FOREIGN KEY (device_id) REFERENCES devices(device_id)
);

-- catalog of products + variants
CREATE TABLE product (
  product_id INT AUTO_INCREMENT PRIMARY KEY,
  category_id INT NOT NULL,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  brand VARCHAR(100),
  manufacturer VARCHAR(100),
  status ENUM('active','discontinued','draft') DEFAULT 'active',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_product_category
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

CREATE TABLE product_variant (
  sku VARCHAR(64) PRIMARY KEY,                -- alphanumeric-friendly
  product_id INT NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  currency CHAR(3) DEFAULT 'USD',
  status ENUM('active','discontinued') DEFAULT 'active',
  -- color VARCHAR(50) NULL,
  -- size  VARCHAR(50) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_variant_product
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);

-- Inventory separated (real-time stock + optional reservation)
CREATE TABLE inventory (
  sku VARCHAR(64) PRIMARY KEY,
  qty_available INT NOT NULL DEFAULT 0,
  qty_reserved  INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_inventory_sku
    FOREIGN KEY (sku) REFERENCES product_variant(sku),
  CONSTRAINT chk_qty_available_nonneg CHECK (qty_available >= 0),
  CONSTRAINT chk_qty_reserved_nonneg  CHECK (qty_reserved >= 0)
);

-- cart
CREATE TABLE cart (
  cart_id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  session_id INT NULL,
  status ENUM('active','abandoned','converted') DEFAULT 'active',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_cart_user
    FOREIGN KEY (user_id) REFERENCES users(user_id),
  CONSTRAINT fk_cart_session
    FOREIGN KEY (session_id) REFERENCES session(session_id)
);

CREATE TABLE cart_items (
  cart_item_id INT AUTO_INCREMENT PRIMARY KEY,
  cart_id INT NOT NULL,
  sku VARCHAR(64) NOT NULL,
  quantity INT NOT NULL DEFAULT 1,
  added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_cart_items_cart
    FOREIGN KEY (cart_id) REFERENCES cart(cart_id),
  CONSTRAINT fk_cart_items_sku
    FOREIGN KEY (sku) REFERENCES product_variant(sku),
  CONSTRAINT chk_cart_qty CHECK (quantity > 0),
  CONSTRAINT uq_cart_sku UNIQUE (cart_id, sku)
);

-- orders and order items (w/ denormalized totals)
CREATE TABLE orders (
  order_id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  cart_id INT NULL,                           -- optional traceability
  status ENUM('placed','paid','shipped','delivered','cancelled','refunded','partially_refunded')
         DEFAULT 'placed',
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  paid_at TIMESTAMP NULL,

  -- Shipping option snapshot + totals snapshot (denormalization)
  shipping_option VARCHAR(50) NULL,
  subtotal_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  tax_amount      DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  shipping_fee    DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  total_amount    DECIMAL(10,2) NOT NULL DEFAULT 0.00,

  -- Shipping address snapshot (prevents old orders changing)
  ship_street  VARCHAR(255) NOT NULL,
  ship_city    VARCHAR(100) NOT NULL,
  ship_state   VARCHAR(100) NOT NULL,
  ship_country VARCHAR(100) NOT NULL,
  ship_zip     VARCHAR(20)  NOT NULL,

  CONSTRAINT fk_order_user FOREIGN KEY (user_id) REFERENCES users(user_id),
  CONSTRAINT fk_order_cart FOREIGN KEY (cart_id) REFERENCES cart(cart_id)
);

CREATE TABLE order_items (
  order_item_id INT AUTO_INCREMENT PRIMARY KEY,
  order_id INT NOT NULL,
  sku VARCHAR(64) NOT NULL,
  quantity INT NOT NULL,
  CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_order_items_sku   FOREIGN KEY (sku) REFERENCES product_variant(sku),
  CONSTRAINT chk_order_qty CHECK (quantity > 0)
);

-- shipments (linked to orders so order can be fulfilled in multiple shipments if need be)
CREATE TABLE shipments (
  shipment_id INT AUTO_INCREMENT PRIMARY KEY,
  order_id INT NOT NULL,
  courier VARCHAR(100) NULL,
  tracking_number VARCHAR(128) NULL UNIQUE,
  shipped_at TIMESTAMP NULL,
  delivered_at TIMESTAMP NULL,
  estimated_delivery_start DATE NULL,
  estimated_delivery_end   DATE NULL,
  CONSTRAINT fk_shipment_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- payments

CREATE TABLE payment_transactions (
  payment_txn_id INT AUTO_INCREMENT PRIMARY KEY,
  order_id INT NOT NULL,
  payment_type ENUM('credit_card','bank_account') NOT NULL,
  status ENUM('authorized','captured','declined','refunded','partially_refunded') NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  provider_ref VARCHAR(128) NULL UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_payment_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- returns + labels
CREATE TABLE returns (
  return_id INT AUTO_INCREMENT PRIMARY KEY,
  order_id INT NOT NULL,
  status ENUM('initiated','received','refunded','exchanged','rejected')
         DEFAULT 'initiated',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_return_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE return_items (
  return_item_id INT AUTO_INCREMENT PRIMARY KEY,
  return_id INT NOT NULL,
  order_item_id INT NOT NULL,
  quantity INT NOT NULL,
  reason VARCHAR(100) NULL,
  refund_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  restocking_fee DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  CONSTRAINT fk_return_items_return FOREIGN KEY (return_id) REFERENCES returns(return_id),
  CONSTRAINT fk_return_items_order_item FOREIGN KEY (order_item_id) REFERENCES order_items(order_item_id),
  CONSTRAINT chk_return_qty CHECK (quantity > 0)
);

-- event log
CREATE TABLE event_log (
  event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NULL,
  session_id INT NULL,
  device_id INT NULL,
  event_type ENUM('view','cart_add','cart_remove','checkout','search') NOT NULL,
  sku VARCHAR(64) NULL,
  metadata_json JSON NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_event_user    FOREIGN KEY (user_id) REFERENCES users(user_id),
  CONSTRAINT fk_event_session FOREIGN KEY (session_id) REFERENCES session(session_id),
  CONSTRAINT fk_event_device  FOREIGN KEY (device_id) REFERENCES devices(device_id)
);

#### NoSQL MongoDB
{
  "sku": "SNY-WH1000-BLK",
  "category": "electronics",
  "brand": "sony",
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