from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any

import pymysql
from neo4j import GraphDatabase
from pymongo import MongoClient

# configure via the env variables
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "mpcs53001_final_project")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "mpcs53001_final_project")
# to account for variant docs stored per SKU in "product_specs"
MONGO_VARIANT_COLL = os.getenv("MONGO_VARIANT_COLL", "product_specs")

def mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def neo_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def mongo_db():
    return MongoClient(MONGO_URI)[MONGO_DB]

# neo4j schema (constraints)
def create_constraints(driver):
    queries = [
        "CREATE CONSTRAINT customer_user_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.user_id IS UNIQUE",
        "CREATE CONSTRAINT product_product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE",
        "CREATE CONSTRAINT variant_sku IF NOT EXISTS FOR (v:Variant) REQUIRE v.sku IS UNIQUE",
        "CREATE CONSTRAINT search_term IF NOT EXISTS FOR (t:SearchTerm) REQUIRE t.term IS UNIQUE",
    ]
    with driver.session() as s:
        for q in queries:
            s.run(q)

# loading of the dimension nodes
def load_customers(driver):
    sql = """
    SELECT u.user_id, u.first_name, u.last_name, a.city, a.state
    FROM users u
    LEFT JOIN address a ON a.address_id = u.address_id
    """
    with mysql_conn().cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    cypher = """
    UNWIND $rows AS r
    MERGE (c:Customer {user_id: r.user_id})
    SET c.first_name = r.first_name,
        c.last_name  = r.last_name,
        c.city       = r.city,
        c.state      = r.state
    """
    with driver.session() as s:
        s.run(cypher, rows=rows)


def load_products_and_variants(driver, mongo_enrich):
    # Products
    sql_products = """
    SELECT p.product_id, p.name, p.brand, c.name AS category
    FROM product p
    JOIN categories c ON c.category_id = p.category_id
    WHERE p.status = 'active'
    """
    # Variants
    sql_variants = """
    SELECT v.sku, v.product_id, v.currency
    FROM product_variant v
    WHERE v.status = 'active'
    """

    with mysql_conn().cursor() as cur:
        cur.execute(sql_products)
        products = cur.fetchall()
        cur.execute(sql_variants)
        variants = cur.fetchall()

    # mongo enrich therefore color/size stored per SKU in Mongo "product_specs"
    sku_to_attrs: dict[str, dict[str, Any]] = {}
    if mongo_enrich:
        db = mongo_db()
        coll = db[MONGO_VARIANT_COLL]
        # Assumes docs have "sku" and "attributes" (from generator script)
        for doc in coll.find({}, {"_id": 0, "sku": 1, "attributes": 1}):
            sku = doc.get("sku")
            attrs = doc.get("attributes") or {}
            if sku:
                sku_to_attrs[str(sku)] = attrs

        # Attach color/size if present
        for v in variants:
            attrs = sku_to_attrs.get(v["sku"], {})
            v["color"] = attrs.get("color")
            v["size"] = attrs.get("size")

    cypher_products = """
    UNWIND $rows AS r
    MERGE (p:Product {product_id: r.product_id})
    SET p.name = r.name,
        p.brand = r.brand,
        p.category = r.category
    """

    cypher_variants = """
    UNWIND $rows AS r
    MERGE (v:Variant {sku: r.sku})
    SET v.currency = r.currency,
        v.color = r.color,
        v.size  = r.size
    WITH v, r
    MATCH (p:Product {product_id: r.product_id})
    MERGE (v)-[:OF_PRODUCT]->(p)
    """

    with driver.session() as s:
        s.run(cypher_products, rows=products)
        # Ensure color/size keys exist even if not enriching
        for v in variants:
            v.setdefault("color", None)
            v.setdefault("size", None)
        s.run(cypher_variants, rows=variants)

# load relationship events
def load_event_edges(driver, limit):
    # Note: MySQL event_type values: view, search, cart_add, cart_remove, checkout
    sql = """
    SELECT event_id, user_id, session_id, device_id, event_type, sku, metadata_json, created_at
    FROM event_log
    ORDER BY event_id
    """
    if limit:
        sql += " LIMIT %s"

    with mysql_conn().cursor() as cur:
        cur.execute(sql, (limit,) if limit else None)
        rows = cur.fetchall()

    # Normalize metadata_json from MySQL (may be None)
    for r in rows:
        mj = r.get("metadata_json")
        if isinstance(mj, (str, bytes)):
            try:
                r["metadata"] = json.loads(mj)
            except Exception:
                r["metadata"] = {"raw": mj}
        else:
            r["metadata"] = mj or {}

        # convert datetime to ISO (Neo4j driver accepts datetime too; keep as string for simplicity)
        if isinstance(r.get("created_at"), datetime):
            r["ts"] = r["created_at"].isoformat()
        else:
            r["ts"] = str(r.get("created_at"))

    # VIEW: link to Product via Variant->Product (since events have sku)
    cypher_view = """
    UNWIND $rows AS r
    WITH r WHERE r.event_type = 'view' AND r.user_id IS NOT NULL AND r.sku IS NOT NULL
    MATCH (c:Customer {user_id: r.user_id})
    MATCH (v:Variant {sku: r.sku})-[:OF_PRODUCT]->(p:Product)
    MERGE (c)-[e:VIEWED]->(p)
    ON CREATE SET e.first_ts = r.ts
    SET e.last_ts = r.ts
    """

    # CART_ADD / CART_REMOVE: link to Variant
    cypher_cart_add = """
    UNWIND $rows AS r
    WITH r WHERE r.event_type = 'cart_add' AND r.user_id IS NOT NULL AND r.sku IS NOT NULL
    MATCH (c:Customer {user_id: r.user_id})
    MATCH (v:Variant {sku: r.sku})
    CREATE (c)-[:ADDED_TO_CART {ts: r.ts, session_id: r.session_id, device_id: r.device_id}]->(v)
    """

    cypher_cart_remove = """
    UNWIND $rows AS r
    WITH r WHERE r.event_type = 'cart_remove' AND r.user_id IS NOT NULL AND r.sku IS NOT NULL
    MATCH (c:Customer {user_id: r.user_id})
    MATCH (v:Variant {sku: r.sku})
    CREATE (c)-[:REMOVED_FROM_CART {ts: r.ts, session_id: r.session_id, device_id: r.device_id}]->(v)
    """

    # SEARCH: create SearchTerm node, link from Customer
    # Assumes metadata includes something like {"query": "..."}.
    cypher_search = """
    UNWIND $rows AS r
    WITH r WHERE r.event_type = 'search' AND r.user_id IS NOT NULL
    WITH r, coalesce(r.metadata.query, r.metadata.term, r.metadata.q) AS term
    WHERE term IS NOT NULL
    MATCH (c:Customer {user_id: r.user_id})
    MERGE (t:SearchTerm {term: term})
    CREATE (c)-[:SEARCHED {ts: r.ts, session_id: r.session_id, device_id: r.device_id}]->(t)
    """

    with driver.session() as s:
        s.run(cypher_view, rows=rows)
        s.run(cypher_cart_add, rows=rows)
        s.run(cypher_cart_remove, rows=rows)
        s.run(cypher_search, rows=rows)


def load_purchases_and_returns(driver):
    # Purchases: orders + items
    sql_purchases = """
    SELECT o.order_id, o.user_id, o.order_date, oi.sku, oi.quantity
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    """
    # Returns: returns + return_items -> order_items
    sql_returns = """
    SELECT r.return_id, o.user_id, r.created_at AS ts, oi.sku, ri.quantity
    FROM returns r
    JOIN orders o ON o.order_id = r.order_id
    JOIN return_items ri ON ri.return_id = r.return_id
    JOIN order_items oi ON oi.order_item_id = ri.order_item_id
    """

    with mysql_conn().cursor() as cur:
        cur.execute(sql_purchases)
        purchases = cur.fetchall()
        cur.execute(sql_returns)
        returns = cur.fetchall()

    for p in purchases:
        if isinstance(p.get("order_date"), datetime):
            p["ts"] = p["order_date"].isoformat()
        else:
            p["ts"] = str(p.get("order_date"))

    for r in returns:
        if isinstance(r.get("ts"), datetime):
            r["ts"] = r["ts"].isoformat()
        else:
            r["ts"] = str(r.get("ts"))

    cypher_purchases = """
    UNWIND $rows AS r
    MATCH (c:Customer {user_id: r.user_id})
    MATCH (v:Variant {sku: r.sku})
    CREATE (c)-[:PURCHASED {ts: r.ts, qty: r.quantity, order_id: r.order_id}]->(v)
    """

    cypher_returns = """
    UNWIND $rows AS r
    MATCH (c:Customer {user_id: r.user_id})
    MATCH (v:Variant {sku: r.sku})
    CREATE (c)-[:RETURNED {ts: r.ts, qty: r.quantity, return_id: r.return_id}]->(v)
    """

    with driver.session() as s:
        s.run(cypher_purchases, rows=purchases)
        s.run(cypher_returns, rows=returns)

def main():
    driver = neo_driver()
    create_constraints(driver)

    load_customers(driver)
    load_products_and_variants(driver, mongo_enrich=True)

    # Limit edges for a quick demo if your event_log is huge:
    load_event_edges(driver, limit=None)
    load_purchases_and_returns(driver)

    driver.close()
    print("ETL complete: Neo4j graph populated.")


if __name__ == "__main__":
    main()