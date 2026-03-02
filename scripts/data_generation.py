import argparse
import json
import os
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import mysql.connector
import redis
from faker import Faker
from pymongo import MongoClient


CATEGORY_PRODUCTS = {
    "Electronics": [
        "Noise-Canceling Headphones",
        "Wireless Earbuds",
        "Smartwatch",
        "Portable Charger",
        "Bluetooth Speaker",
        "Gaming Mouse",
        "Mechanical Keyboard",
    ],
    "Fashion": [
        "Summer Dress",
        "Athleisure Set",
        "Vintage Denim Jacket",
        "Running Shoes",
        "Statement Jewelry",
        "Silk Blouse",
        "Leather Belt",
    ],
    "Home": [
        "Ceramic Vase",
        "Throw Pillow",
        "Smart Kettle",
        "Floor Lamp",
        "Wall Art",
        "Storage Basket",
        "Coffee Table",
    ],
}

CATEGORY_BRANDS = {
    "Electronics": ["Sony", "Bose", "Sennheiser", "Apple", "Technics", "Anker"],
    "Fashion": ["Everlane", "Nordstrom", "Reformation", "Nike", "Levi's", "Madewell"],
    "Home": ["West Elm", "Anthropologie", "Etsy Artisans", "IKEA", "CB2", "VaseCo"],
}

DEVICE_TYPES = ["tablet", "laptop", "phone", "web"]
SEARCH_TERMS = [
    "headphones",
    "blue dress",
    "ceramic vase",
    "running shoes",
    "smartwatch",
    "home decor",
    "gift ideas",
    "wireless earbuds",
    "summer outfit",
    "minimalist lamp",
]

SHIPPING_OPTIONS = {
    "standard": 5.99,
    "expedited": 12.99,
    "overnight": 24.99,
}

COURIERS = ["FedEx", "UPS", "DHL", "USPS"]
PAYMENT_TYPES = ["credit_card", "bank_account"]


@dataclass
class GenerationConfig:
    users: int = 1000
    products: int = 5000
    orders: int = 100000
    events: int = 500000
    return_rate: float = 0.10
    seed: int = 42
    mongo_collection: str = "product_specs"
    event_batch_size: int = 5000
    mysql_commit_interval: int = 2000
    start_days_ago: int = 220


class EcommerceDataGenerator:
    def __init__(self, cfg: GenerationConfig):
        self.cfg = cfg
        self.fake = Faker()
        random.seed(cfg.seed)
        Faker.seed(cfg.seed)

        self.mysql_conn = None
        self.mysql_cursor = None
        self.mongo_db = None
        self.redis_client = None

        self.start_ts = (datetime.utcnow() - timedelta(days=cfg.start_days_ago)).replace(
            microsecond=0
        )
        self.end_ts = (datetime.utcnow() - timedelta(minutes=1)).replace(microsecond=0)

        self.user_ids: List[int] = []
        self.user_address: Dict[int, Tuple[str, str, str, str, str]] = {}
        self.user_devices: Dict[int, List[int]] = defaultdict(list)

        self.sku_list: List[str] = []
        self.sku_price: Dict[str, float] = {}
        self.sku_category: Dict[str, str] = {}
        self.sku_qty: Dict[str, int] = {}
        self.in_stock_skus = set()

        self.order_ids: List[int] = []
        self.inventory_delta: Dict[str, int] = defaultdict(int)

        self.event_buffer = []
        self.event_count = 0
        self.order_count = 0
        self.session_count = 0

    def connect(self) -> None:
        mysql_cfg = {
            "host": os.getenv("MYSQL_HOST", "localhost"),
            "port": int(os.getenv("MYSQL_PORT", "3306")),
            "user": os.getenv("MYSQL_USER", "root"),
            "password": os.getenv("MYSQL_PASSWORD", "mypassword"),
            "database": os.getenv("MYSQL_DB", "mpcs53001_final_project"),
        }
        self.mysql_conn = mysql.connector.connect(**mysql_cfg)
        self.mysql_conn.autocommit = False
        self.mysql_cursor = self.mysql_conn.cursor()
        # Use UTC for TIMESTAMP writes to avoid DST-invalid local wall-clock times.
        self.mysql_cursor.execute("SET time_zone = '+00:00'")

        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        mongo_db_name = os.getenv("MONGO_DB", "mpcs53001_final_project")
        self.mongo_db = MongoClient(mongo_uri)[mongo_db_name]

        redis_cfg = {
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "db": int(os.getenv("REDIS_DB", "0")),
            "decode_responses": True,
        }
        self.redis_client = redis.Redis(**redis_cfg)

    def close(self) -> None:
        if self.mysql_cursor:
            self.mysql_cursor.close()
        if self.mysql_conn:
            self.mysql_conn.close()

    def _random_dt(self, low: datetime, high: datetime) -> datetime:
        if low >= high:
            return low
        delta = int((high - low).total_seconds())
        return low + timedelta(seconds=random.randint(0, delta))

    def _event_times(self, start: datetime, end: datetime, n: int) -> List[datetime]:
        if n <= 0:
            return []
        if n == 1:
            return [start]
        times = [self._random_dt(start, end) for _ in range(n)]
        times.sort()
        return times

    def _batch_insert(self, query: str, rows: List[Tuple], batch_size: int = 2000) -> None:
        if not rows:
            return
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            self.mysql_cursor.executemany(query, chunk)

    def _flush_events(self, force: bool = False) -> None:
        if not self.event_buffer:
            return
        if len(self.event_buffer) < self.cfg.event_batch_size and not force:
            return

        query = """
            INSERT INTO event_log
                (user_id, session_id, device_id, event_type, sku, metadata_json, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self._batch_insert(query, self.event_buffer, batch_size=self.cfg.event_batch_size)
        self.event_buffer.clear()

    def _record_event(
        self,
        user_id: int,
        session_id: int,
        device_id: int,
        event_type: str,
        created_at: datetime,
        sku: str = None,
        metadata: Dict = None,
    ) -> None:
        md = json.dumps(metadata or {}, separators=(",", ":"))
        self.event_buffer.append(
            (user_id, session_id, device_id, event_type, sku, md, created_at)
        )
        self.event_count += 1
        self._flush_events()

    def _clear_redis_user_keys(self) -> None:
        keys = list(self.redis_client.scan_iter(match="user:*"))
        if keys:
            self.redis_client.delete(*keys)

    def _assert_mysql_is_empty(self) -> None:
        core_tables = ["categories", "users", "product", "orders", "event_log"]
        non_empty = []
        for table in core_tables:
            self.mysql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            if self.mysql_cursor.fetchone()[0] > 0:
                non_empty.append(table)
        if non_empty:
            joined = ", ".join(non_empty)
            raise RuntimeError(
                f"MySQL tables are not empty ({joined}). "
                "Run sql/schema.sql to reset the database before generation."
            )

    def generate_entities(self) -> None:
        print("[1/5] Generating base entities...")

        self._assert_mysql_is_empty()
        self._clear_redis_user_keys()

        # 1) categories
        category_rows = [(name,) for name in CATEGORY_PRODUCTS.keys()]
        self._batch_insert("INSERT INTO categories (name) VALUES (%s)", category_rows)

        self.mysql_cursor.execute("SELECT category_id, name FROM categories")
        category_map = {name: cid for cid, name in self.mysql_cursor.fetchall()}

        # 2) address
        address_rows = []
        for _ in range(self.cfg.users):
            address_rows.append(
                (
                    self.fake.street_address(),
                    self.fake.city(),
                    self.fake.state(),
                    "USA",
                    self.fake.postcode(),
                )
            )
        self._batch_insert(
            "INSERT INTO address (street, city, state, country, zip) VALUES (%s, %s, %s, %s, %s)",
            address_rows,
        )

        self.mysql_cursor.execute("SELECT address_id, street, city, state, country, zip FROM address ORDER BY address_id")
        address_data = self.mysql_cursor.fetchall()
        address_ids = [row[0] for row in address_data]

        # 3) users
        user_rows = []
        for i in range(self.cfg.users):
            address_id = address_ids[i]
            user_rows.append(
                (
                    address_id,
                    self.fake.first_name(),
                    self.fake.last_name(),
                    f"user_{i+1}@example.com",
                    "hashed_password",
                    self._random_dt(self.start_ts, self.end_ts),
                    self._random_dt(self.start_ts, self.end_ts),
                )
            )
        self._batch_insert(
            """
            INSERT INTO users
                (address_id, first_name, last_name, email, password_hash, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            user_rows,
        )

        self.mysql_cursor.execute("SELECT user_id, address_id FROM users ORDER BY user_id")
        user_address_ids = self.mysql_cursor.fetchall()
        self.user_ids = [u for u, _ in user_address_ids]

        address_by_id = {row[0]: row[1:] for row in address_data}
        for user_id, address_id in user_address_ids:
            self.user_address[user_id] = address_by_id[address_id]

        # 4) devices
        device_rows = []
        for user_id in self.user_ids:
            for _ in range(random.randint(1, 3)):
                device_rows.append(
                    (
                        user_id,
                        random.choice(DEVICE_TYPES),
                        self._random_dt(self.start_ts, self.end_ts),
                    )
                )
        self._batch_insert(
            "INSERT INTO devices (user_id, device_type, last_seen_at) VALUES (%s, %s, %s)",
            device_rows,
        )

        self.mysql_cursor.execute("SELECT device_id, user_id FROM devices")
        for device_id, user_id in self.mysql_cursor.fetchall():
            self.user_devices[user_id].append(device_id)

        # 5) product
        product_rows = []
        category_names = list(CATEGORY_PRODUCTS.keys())
        for _ in range(self.cfg.products):
            category_name = random.choice(category_names)
            category_id = category_map[category_name]
            brand = random.choice(CATEGORY_BRANDS[category_name])
            base_name = random.choice(CATEGORY_PRODUCTS[category_name])
            product_rows.append(
                (
                    category_id,
                    f"{brand} {base_name}",
                    self.fake.sentence(nb_words=12),
                    brand,
                    brand,
                    "active",
                    self._random_dt(self.start_ts, self.end_ts),
                    self._random_dt(self.start_ts, self.end_ts),
                )
            )

        self._batch_insert(
            """
            INSERT INTO product
                (category_id, name, description, brand, manufacturer, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            product_rows,
        )

        self.mysql_cursor.execute(
            "SELECT p.product_id, c.name FROM product p JOIN categories c ON c.category_id = p.category_id"
        )
        products = self.mysql_cursor.fetchall()

        # 6) product_variant + 7) inventory + Mongo product_specs
        variant_rows = []
        inventory_rows = []
        mongo_docs = []

        for product_id, category_name in products:
            variant_count = random.randint(1, 3)
            for idx in range(1, variant_count + 1):
                sku = f"SKU-{product_id:07d}-{idx}"
                if category_name == "Electronics":
                    price = round(random.uniform(40, 950), 2)
                    attributes = {
                        "battery_life_hours": random.randint(6, 60),
                        "connectivity": random.choice(["Bluetooth", "WiFi", "USB-C"]),
                        "weight_grams": random.randint(80, 1500),
                    }
                elif category_name == "Fashion":
                    price = round(random.uniform(20, 350), 2)
                    attributes = {
                        "size": random.choice(["XS", "S", "M", "L", "XL"]),
                        "material": random.choice(["Cotton", "Silk", "Linen", "Denim"]),
                        "color": random.choice(["Blue", "Black", "White", "Red", "Green"]),
                    }
                else:
                    price = round(random.uniform(25, 700), 2)
                    attributes = {
                        "dimensions_cm": f"{random.randint(10,120)}x{random.randint(10,120)}x{random.randint(5,80)}",
                        "material": random.choice(["Ceramic", "Wood", "Metal", "Glass"]),
                        "care_instructions": random.choice(["Hand wash", "Wipe clean", "Machine wash"]),
                    }

                qty = random.randint(150, 900)
                created_at = self._random_dt(self.start_ts, self.end_ts)
                updated_at = self._random_dt(created_at, self.end_ts)

                variant_rows.append(
                    (sku, product_id, price, "USD", "active", created_at, updated_at)
                )
                inventory_rows.append((sku, qty, 0, updated_at))

                self.sku_list.append(sku)
                self.sku_price[sku] = price
                self.sku_category[sku] = category_name
                self.sku_qty[sku] = qty
                if qty > 0:
                    self.in_stock_skus.add(sku)

                mongo_docs.append(
                    {
                        "sku": sku,
                        "category": category_name,
                        "attributes": attributes,
                        "updated_at": updated_at,
                    }
                )

        self._batch_insert(
            """
            INSERT INTO product_variant
                (sku, product_id, price, currency, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            variant_rows,
        )

        self._batch_insert(
            "INSERT INTO inventory (sku, qty_available, qty_reserved, updated_at) VALUES (%s, %s, %s, %s)",
            inventory_rows,
        )

        collection = self.mongo_db[self.cfg.mongo_collection]
        collection.delete_many({})
        if mongo_docs:
            collection.insert_many(mongo_docs, ordered=False)

        self.mysql_conn.commit()

        print(
            f"  entities complete: users={len(self.user_ids)}, products={len(products)}, skus={len(self.sku_list)}"
        )

    def _create_cart(self, user_id: int, session_id: int, created_at: datetime) -> int:
        self.mysql_cursor.execute(
            "INSERT INTO cart (user_id, session_id, status, created_at, updated_at) VALUES (%s, %s, 'active', %s, %s)",
            (user_id, session_id, created_at, created_at),
        )
        return self.mysql_cursor.lastrowid

    def _materialize_cart_items(self, cart_id: int, snapshot: Dict[str, str], ts: datetime) -> None:
        rows = []
        for sku, qty_text in snapshot.items():
            qty = int(qty_text)
            if qty > 0:
                rows.append((cart_id, sku, qty, ts, ts))
        self._batch_insert(
            """
            INSERT INTO cart_items (cart_id, sku, quantity, added_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            rows,
            batch_size=2000,
        )

    def _apply_checkout(self, user_id: int, cart_id: int, checkout_time: datetime, snapshot: Dict[str, str]) -> None:
        address = self.user_address[user_id]

        items = []
        subtotal = 0.0
        for sku, qty_text in snapshot.items():
            qty = int(qty_text)
            if qty <= 0:
                continue
            if self.sku_qty.get(sku, 0) < qty:
                continue
            items.append((sku, qty))
            subtotal += self.sku_price[sku] * qty

        if not items:
            self.mysql_cursor.execute(
                "UPDATE cart SET status='abandoned', updated_at=%s WHERE cart_id=%s",
                (checkout_time, cart_id),
            )
            return

        shipping_option = random.choice(list(SHIPPING_OPTIONS.keys()))
        shipping_fee = SHIPPING_OPTIONS[shipping_option]
        tax_amount = round(subtotal * random.uniform(0.06, 0.10), 2)
        total = round(subtotal + tax_amount + shipping_fee, 2)

        paid_at = checkout_time + timedelta(minutes=random.randint(0, 15))

        self.mysql_cursor.execute(
            """
            INSERT INTO orders (
                user_id, cart_id, status, order_date, paid_at,
                shipping_option, subtotal_amount, tax_amount, shipping_fee, total_amount,
                ship_street, ship_city, ship_state, ship_country, ship_zip
            ) VALUES (%s, %s, 'paid', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                cart_id,
                checkout_time,
                paid_at,
                shipping_option,
                round(subtotal, 2),
                tax_amount,
                shipping_fee,
                total,
                address[0],
                address[1],
                address[2],
                address[3],
                address[4],
            ),
        )
        order_id = self.mysql_cursor.lastrowid
        self.order_ids.append(order_id)

        order_item_rows = []
        for sku, qty in items:
            order_item_rows.append((order_id, sku, qty))
            self.sku_qty[sku] -= qty
            self.inventory_delta[sku] -= qty
            if self.sku_qty[sku] <= 0 and sku in self.in_stock_skus:
                self.in_stock_skus.remove(sku)

        self._batch_insert(
            "INSERT INTO order_items (order_id, sku, quantity) VALUES (%s, %s, %s)",
            order_item_rows,
            batch_size=500,
        )

        payment_status = random.choice(["authorized", "captured", "captured", "captured"])
        self.mysql_cursor.execute(
            """
            INSERT INTO payment_transactions (
                order_id, payment_type, status, amount, provider_ref, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                order_id,
                random.choice(PAYMENT_TYPES),
                payment_status,
                total,
                str(uuid.uuid4()),
                checkout_time,
                checkout_time,
            ),
        )

        shipped_at = checkout_time + timedelta(days=random.randint(0, 2))
        delivered_at = shipped_at + timedelta(days=random.randint(1, 6))
        est_start = shipped_at.date() + timedelta(days=1)
        est_end = est_start + timedelta(days=3)

        self.mysql_cursor.execute(
            """
            INSERT INTO shipments (
                order_id, courier, tracking_number, shipped_at, delivered_at,
                estimated_delivery_start, estimated_delivery_end
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                order_id,
                random.choice(COURIERS),
                f"TRK-{uuid.uuid4().hex[:18]}",
                shipped_at,
                delivered_at,
                est_start,
                est_end,
            ),
        )

        self.mysql_cursor.execute(
            "UPDATE cart SET status='converted', updated_at=%s WHERE cart_id=%s",
            (checkout_time, cart_id),
        )

        self.order_count += 1

    def simulate_user_activity(self) -> None:
        print("[2/5] Simulating sessions, events, carts, and orders...")

        if not self.user_ids:
            raise RuntimeError("No users found; generate entities first.")

        # Heavy-tail order allocation so power users place more orders.
        weights = [random.paretovariate(2.5) for _ in self.user_ids]
        total_weight = sum(weights)
        quotas = [int(w / total_weight * self.cfg.orders) for w in weights]
        remainder = self.cfg.orders - sum(quotas)
        for idx in random.sample(range(len(quotas)), remainder):
            quotas[idx] += 1

        writes_since_commit = 0

        for user_idx, user_id in enumerate(self.user_ids):
            order_quota = quotas[user_idx]
            extra_sessions = random.randint(3, 14)
            total_sessions = order_quota + extra_sessions

            # Session times are sorted per user to maintain chronology.
            session_starts = sorted(
                self._random_dt(self.start_ts, self.end_ts - timedelta(minutes=5))
                for _ in range(total_sessions)
            )

            checkout_sessions = set(random.sample(range(total_sessions), order_quota)) if order_quota > 0 else set()

            for s_idx, session_start in enumerate(session_starts):
                duration_min = random.randint(8, 95)
                session_end = min(session_start + timedelta(minutes=duration_min), self.end_ts)
                device_id = random.choice(self.user_devices[user_id])

                self.mysql_cursor.execute(
                    """
                    INSERT INTO session (user_id, device_id, created_at, expires_at, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (user_id, device_id, session_start, session_end, False),
                )
                session_id = self.mysql_cursor.lastrowid
                self.session_count += 1

                cart_key = f"user:{user_id}:cart"
                views_key = f"user:{user_id}:recent_views"
                self.redis_client.delete(cart_key)

                cart_id = None
                all_times = []

                if s_idx in checkout_sessions:
                    # 1) Browsing/search events
                    browse_events = random.randint(2, 6)
                    add_count = random.randint(1, 4)
                    # Allocate enough timeline slots for:
                    # browse events + add events + optional remove + checkout.
                    all_times.extend(
                        self._event_times(
                            session_start,
                            session_end,
                            browse_events + add_count + 2,
                        )
                    )
                    t_idx = 0

                    for _ in range(browse_events):
                        ts = all_times[t_idx]
                        t_idx += 1
                        if random.random() < 0.25:
                            term = random.choice(SEARCH_TERMS)
                            self._record_event(
                                user_id,
                                session_id,
                                device_id,
                                "search",
                                ts,
                                None,
                                {"term": term},
                            )
                        else:
                            sku = random.choice(self.sku_list)
                            self.redis_client.lpush(views_key, sku)
                            self._record_event(
                                user_id,
                                session_id,
                                device_id,
                                "view",
                                ts,
                                sku,
                                {"surface": "product_page"},
                            )

                    # 2) Add items and create cart on first add
                    if cart_id is None:
                        cart_id = self._create_cart(user_id, session_id, all_times[t_idx])
                        writes_since_commit += 1

                    source_skus = list(self.in_stock_skus) if self.in_stock_skus else self.sku_list
                    chosen_skus = random.sample(source_skus, k=min(add_count + 1, len(source_skus)))
                    for i in range(add_count):
                        ts = all_times[t_idx]
                        t_idx += 1
                        sku = chosen_skus[i]
                        qty = random.randint(1, 3)
                        current = int(self.redis_client.hget(cart_key, sku) or 0)
                        self.redis_client.hset(cart_key, sku, current + qty)
                        self._record_event(
                            user_id,
                            session_id,
                            device_id,
                            "cart_add",
                            ts,
                            sku,
                            {"qty": qty},
                        )

                    # Optional remove before checkout
                    if random.random() < 0.35:
                        snapshot = self.redis_client.hgetall(cart_key)
                        if len(snapshot) > 1:
                            ts = all_times[t_idx]
                            t_idx += 1
                            rem_sku = random.choice(list(snapshot.keys()))
                            self.redis_client.hdel(cart_key, rem_sku)
                            self._record_event(
                                user_id,
                                session_id,
                                device_id,
                                "cart_remove",
                                ts,
                                rem_sku,
                                {"reason": "user_change"},
                            )

                    # 3) Checkout
                    checkout_ts = all_times[t_idx]
                    self._record_event(
                        user_id,
                        session_id,
                        device_id,
                        "checkout",
                        checkout_ts,
                        None,
                        {"channel": "web"},
                    )

                    snapshot = self.redis_client.hgetall(cart_key)
                    if snapshot:
                        self._materialize_cart_items(cart_id, snapshot, checkout_ts)
                        self._apply_checkout(user_id, cart_id, checkout_ts, snapshot)
                        writes_since_commit += 1
                    else:
                        self.mysql_cursor.execute(
                            "UPDATE cart SET status='abandoned', updated_at=%s WHERE cart_id=%s",
                            (checkout_ts, cart_id),
                        )
                        writes_since_commit += 1

                    self.redis_client.delete(cart_key)

                else:
                    # Non-checkout session: browsing, maybe cart additions/removals, then abandon if cart has items.
                    event_n = random.randint(2, 10)
                    times = self._event_times(session_start, session_end, event_n)

                    for ts in times:
                        r = random.random()
                        if r < 0.50:
                            sku = random.choice(self.sku_list)
                            self.redis_client.lpush(views_key, sku)
                            self._record_event(user_id, session_id, device_id, "view", ts, sku, {"surface": "product_page"})
                        elif r < 0.72:
                            term = random.choice(SEARCH_TERMS)
                            self._record_event(user_id, session_id, device_id, "search", ts, None, {"term": term})
                        elif r < 0.92:
                            if cart_id is None:
                                cart_id = self._create_cart(user_id, session_id, ts)
                                writes_since_commit += 1
                            source_skus = list(self.in_stock_skus) if self.in_stock_skus else self.sku_list
                            sku = random.choice(source_skus)
                            qty = random.randint(1, 2)
                            current = int(self.redis_client.hget(cart_key, sku) or 0)
                            self.redis_client.hset(cart_key, sku, current + qty)
                            self._record_event(user_id, session_id, device_id, "cart_add", ts, sku, {"qty": qty})
                        else:
                            snapshot = self.redis_client.hgetall(cart_key)
                            if snapshot:
                                sku = random.choice(list(snapshot.keys()))
                                self.redis_client.hdel(cart_key, sku)
                                self._record_event(
                                    user_id,
                                    session_id,
                                    device_id,
                                    "cart_remove",
                                    ts,
                                    sku,
                                    {"reason": "user_change"},
                                )

                    snapshot = self.redis_client.hgetall(cart_key)
                    if cart_id and snapshot:
                        self._materialize_cart_items(cart_id, snapshot, session_end)
                        self.mysql_cursor.execute(
                            "UPDATE cart SET status='abandoned', updated_at=%s WHERE cart_id=%s",
                            (session_end, cart_id),
                        )
                        writes_since_commit += 1

                    self.redis_client.delete(cart_key)

                if writes_since_commit >= self.cfg.mysql_commit_interval:
                    self._flush_events(force=True)
                    self.mysql_conn.commit()
                    writes_since_commit = 0

        # Filler events if needed to meet minimum event target.
        while self.event_count < self.cfg.events:
            user_id = random.choice(self.user_ids)
            device_id = random.choice(self.user_devices[user_id])
            s_start = self._random_dt(self.start_ts, self.end_ts - timedelta(minutes=2))
            s_end = s_start + timedelta(minutes=random.randint(2, 20))
            self.mysql_cursor.execute(
                "INSERT INTO session (user_id, device_id, created_at, expires_at, is_active) VALUES (%s, %s, %s, %s, %s)",
                (user_id, device_id, s_start, s_end, False),
            )
            session_id = self.mysql_cursor.lastrowid
            self.session_count += 1

            n = min(self.cfg.events - self.event_count, random.randint(2, 8))
            for ts in self._event_times(s_start, s_end, n):
                if random.random() < 0.6:
                    sku = random.choice(self.sku_list)
                    self._record_event(user_id, session_id, device_id, "view", ts, sku, {"surface": "product_page"})
                else:
                    term = random.choice(SEARCH_TERMS)
                    self._record_event(user_id, session_id, device_id, "search", ts, None, {"term": term})

        self._flush_events(force=True)
        self.mysql_conn.commit()

        print(
            f"  activity complete: sessions={self.session_count}, orders={self.order_count}, events={self.event_count}"
        )

    def generate_returns(self) -> None:
        print("[3/5] Generating returns...")
        if not self.order_ids:
            print("  no orders available; skipping returns")
            return

        return_target = int(len(self.order_ids) * self.cfg.return_rate)
        selected_orders = set(random.sample(self.order_ids, return_target)) if return_target > 0 else set()

        order_items_by_order: Dict[int, List[Tuple[int, str, int]]] = defaultdict(list)
        selected_list = list(selected_orders)
        chunk_size = 1000

        for i in range(0, len(selected_list), chunk_size):
            chunk = selected_list[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            self.mysql_cursor.execute(
                f"SELECT order_item_id, order_id, sku, quantity FROM order_items WHERE order_id IN ({placeholders})",
                tuple(chunk),
            )
            for row in self.mysql_cursor.fetchall():
                order_items_by_order[row[1]].append((row[0], row[2], row[3]))

        generated_returns = 0
        writes_since_commit = 0

        for order_id in selected_list:
            items = order_items_by_order.get(order_id, [])
            if not items:
                continue

            created_at = self._random_dt(self.start_ts, self.end_ts)
            status = random.choice(["initiated", "received", "refunded", "exchanged"])
            self.mysql_cursor.execute(
                "INSERT INTO returns (order_id, status, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                (order_id, status, created_at, created_at),
            )
            return_id = self.mysql_cursor.lastrowid

            sample_k = random.randint(1, min(2, len(items)))
            picked = random.sample(items, sample_k)

            rows = []
            for order_item_id, sku, qty in picked:
                ret_qty = random.randint(1, qty)
                gross = self.sku_price.get(sku, 20.0) * ret_qty
                restocking_fee = round(gross * random.choice([0.0, 0.05, 0.10]), 2)
                refund_amount = round(max(gross - restocking_fee, 0.0), 2)
                rows.append(
                    (
                        return_id,
                        order_item_id,
                        ret_qty,
                        random.choice(["size_issue", "damaged", "changed_mind", "wrong_item"]),
                        refund_amount,
                        restocking_fee,
                    )
                )

                if random.random() < 0.70:
                    self.inventory_delta[sku] += ret_qty
                    self.sku_qty[sku] += ret_qty
                    self.in_stock_skus.add(sku)

            self._batch_insert(
                """
                INSERT INTO return_items
                    (return_id, order_item_id, quantity, reason, refund_amount, restocking_fee)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                rows,
                batch_size=200,
            )

            generated_returns += 1
            writes_since_commit += 1

            if writes_since_commit >= self.cfg.mysql_commit_interval:
                self.mysql_conn.commit()
                writes_since_commit = 0

        self.mysql_conn.commit()
        print(f"  returns complete: returns={generated_returns}")

    def apply_inventory_updates(self) -> None:
        print("[4/5] Applying inventory updates...")
        rows = [(delta, sku) for sku, delta in self.inventory_delta.items() if delta != 0]
        self._batch_insert(
            "UPDATE inventory SET qty_available = GREATEST(qty_available + %s, 0) WHERE sku = %s",
            rows,
            batch_size=3000,
        )
        self.mysql_conn.commit()
        print(f"  inventory updates applied: touched_skus={len(rows)}")

    def run_validations(self) -> None:
        print("[5/5] Running validation checks...")

        checks = {
            "orphan_cart_items": "SELECT COUNT(*) FROM cart_items ci LEFT JOIN cart c ON ci.cart_id = c.cart_id WHERE c.cart_id IS NULL",
            "orphan_order_items": "SELECT COUNT(*) FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.order_id WHERE o.order_id IS NULL",
            "orphan_event_sessions": "SELECT COUNT(*) FROM event_log e LEFT JOIN session s ON e.session_id = s.session_id WHERE e.session_id IS NOT NULL AND s.session_id IS NULL",
        }

        for label, sql in checks.items():
            self.mysql_cursor.execute(sql)
            value = self.mysql_cursor.fetchone()[0]
            print(f"  {label}: {value}")

        self.mysql_cursor.execute("SELECT COUNT(*) FROM users")
        users = self.mysql_cursor.fetchone()[0]
        self.mysql_cursor.execute("SELECT COUNT(*) FROM product")
        products = self.mysql_cursor.fetchone()[0]
        self.mysql_cursor.execute("SELECT COUNT(*) FROM orders")
        orders = self.mysql_cursor.fetchone()[0]
        self.mysql_cursor.execute("SELECT COUNT(*) FROM event_log")
        events = self.mysql_cursor.fetchone()[0]

        self.mysql_cursor.execute("SELECT status, COUNT(*) FROM cart GROUP BY status")
        cart_breakdown = self.mysql_cursor.fetchall()

        self.mysql_cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM event_log")
        min_evt, max_evt = self.mysql_cursor.fetchone()

        print(f"  counts: users={users}, products={products}, orders={orders}, events={events}")
        print(f"  cart_status_breakdown: {cart_breakdown}")
        print(f"  event_time_window: {min_evt} -> {max_evt}")

        if users < self.cfg.users:
            raise RuntimeError("Validation failed: users below configured minimum")
        if products < self.cfg.products:
            raise RuntimeError("Validation failed: products below configured minimum")
        if orders < self.cfg.orders:
            raise RuntimeError("Validation failed: orders below configured minimum")
        if events < self.cfg.events:
            raise RuntimeError("Validation failed: events below configured minimum")

    def run(self) -> None:
        self.connect()
        try:
            self.generate_entities()
            self.simulate_user_activity()
            self.generate_returns()
            self.apply_inventory_updates()
            self.run_validations()
            print("Data generation completed successfully.")
        finally:
            self.close()


def parse_args() -> GenerationConfig:
    parser = argparse.ArgumentParser(description="Generate multi-store e-commerce synthetic data")
    parser.add_argument("--users", type=int, default=1000)
    parser.add_argument("--products", type=int, default=5000)
    parser.add_argument("--orders", type=int, default=100000)
    parser.add_argument("--events", type=int, default=500000)
    parser.add_argument("--return-rate", type=float, default=0.10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--event-batch-size", type=int, default=5000)
    parser.add_argument("--mysql-commit-interval", type=int, default=2000)
    parser.add_argument("--start-days-ago", type=int, default=220)
    args = parser.parse_args()

    return GenerationConfig(
        users=args.users,
        products=args.products,
        orders=args.orders,
        events=args.events,
        return_rate=args.return_rate,
        seed=args.seed,
        event_batch_size=args.event_batch_size,
        mysql_commit_interval=args.mysql_commit_interval,
        start_days_ago=args.start_days_ago,
    )


if __name__ == "__main__":
    cfg = parse_args()
    generator = EcommerceDataGenerator(cfg)
    generator.run()
