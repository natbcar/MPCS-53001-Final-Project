CREATE DATABASE IF NOT EXISTS mpcs53001_final_project;
USE mpcs53001_final_project;

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS return_items;
DROP TABLE IF EXISTS returns;

DROP TABLE IF EXISTS payment_transactions;

DROP TABLE IF EXISTS shipments;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;

DROP TABLE IF EXISTS cart_items;
DROP TABLE IF EXISTS cart;

DROP TABLE IF EXISTS inventory;

DROP TABLE IF EXISTS session;
DROP TABLE IF EXISTS devices;

DROP TABLE IF EXISTS product_variant;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS categories;

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS address;

DROP TABLE IF EXISTS event_log;

SET FOREIGN_KEY_CHECKS = 1;

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
  sku VARCHAR(64) PRIMARY KEY,                
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

-- inventory
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

-- orders and order items 
CREATE TABLE orders (
  order_id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  cart_id INT NULL,                           
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

-- shipments 
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

-- indexes
CREATE INDEX idx_product_category ON product(category_id);
CREATE INDEX idx_variant_product  ON product_variant(product_id);
CREATE INDEX idx_inventory_updated ON inventory(updated_at);

CREATE INDEX idx_cart_user_status ON cart(user_id, status);
CREATE INDEX idx_cart_session ON cart(session_id);
CREATE INDEX idx_cart_items_cart ON cart_items(cart_id);
CREATE INDEX idx_cart_items_sku  ON cart_items(sku);

CREATE INDEX idx_orders_user_date ON orders(user_id, order_date);
CREATE INDEX idx_order_items_order ON order_items(order_id);

CREATE INDEX idx_shipments_order ON shipments(order_id);

CREATE INDEX idx_returns_order ON returns(order_id);
CREATE INDEX idx_return_items_return ON return_items(return_id);
