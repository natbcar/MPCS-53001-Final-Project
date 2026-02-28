-- First time run: CREATE DATABASE mpcs53001_final_project

USE mpcs53001_final_project;
SET FOREIGN_KEY_CHECKS = 0;

-- -- Drop tables in reverse order of dependencies
DROP TABLE IF EXISTS return_items;
DROP TABLE IF EXISTS returns;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS shipments;
DROP TABLE IF EXISTS cart_items;
DROP TABLE IF EXISTS cart;
DROP TABLE IF EXISTS session;
DROP TABLE IF EXISTS product_variant;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS address;
DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS event_log;

SET FOREIGN_KEY_CHECKS = 1;

-- 1. Independent Tables (Lookups & Base Entities)
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
    email VARCHAR(255) NOT NULL,
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
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(user_id)
);