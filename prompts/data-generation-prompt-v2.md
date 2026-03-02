## Data Generation Specification (Script-Ready)

### Goal
Generate realistic multi-store e-commerce data with referential consistency across MySQL, MongoDB, and Redis.

### Target Volumes
- users: >= 1,000
- products: >= 5,000
- orders: >= 100,000
- events: >= 500,000

### Time Window
- Generate timestamps across the last 6+ months.
- Ensure session/event/order timestamps are chronologically valid.

## 1) Base Entity Generation (MySQL first, then Mongo)

### 1.1 MySQL tables (dependency order)
1. `categories`
2. `address`
3. `users` (FK -> `address`)
4. `devices` (FK -> `users`)
5. `product`
6. `product_variant` (FK -> `product`)
7. `inventory` (FK -> `product_variant`)

### 1.2 MongoDB
- Collection: `product_specs`
- Key: `sku` (must match `product_variant.sku`)
- Store category-specific attributes (variable schema per category).

### 1.3 Lookup structures (in memory)
- `user_ids`
- `user_id -> address_id`
- `user_id -> device_ids`
- `sku_list`
- `sku -> price`
- `sku -> category`
- `sku -> qty_available`

## 2) User Activity Simulation

### 2.1 Event types
- `view`
- `search`
- `cart_add`
- `cart_remove`
- `checkout`

### 2.2 Redis keys
- `user:{id}:recent_views` (LIST of SKUs, newest first)
- `user:{id}:cart` (HASH sku -> quantity)
- optional: `session:{id}:events` (temporary buffering)

### 2.3 Session loop
For each user:
1. Generate 1..N sessions in timestamp order.
2. Assign/create one `session` row (linked to `device`).
3. Emit events for the session.
4. Persist every emitted event to `event_log` with `user_id`, `session_id`, `device_id`, `event_type`, `sku`, timestamp.

## 3) Cart + Checkout Policy (simplified and consistent)

### 3.1 Cart creation
- Create `cart` on first `cart_add` in a session.
- Set `status='active'`.

### 3.2 During session
- Keep cart mutations in Redis only (`HSET/HDEL`).
- Do not continuously write `cart_items`.

### 3.3 Checkout trigger
On `checkout` event:
1. Read final cart snapshot from Redis (`HGETALL`).
2. Write `cart_items` snapshot once.
3. Create `orders` row linked to user/cart.
4. Create `order_items` from snapshot with unit price snapshot.
5. Create `payment_transactions`.
6. Create `shipments`.
7. Decrement `inventory.qty_available`.
8. Mark cart `converted`.
9. Clear Redis cart key.

### 3.4 Session ends without checkout
- If cart exists and has items:
1. Write `cart_items` snapshot once.
2. Mark cart `abandoned`.
3. Clear Redis cart key.

## 4) Returns Generation

After orders are created:
1. Sample ~10% of orders for returns.
2. Insert into `returns` (FK -> `orders`).
3. Insert `return_items` linked to `order_items`.
4. Update return status flow (`initiated` -> `received` -> `refunded` etc.).
5. Optional: restock some returned quantities into `inventory`.

## 5) Consistency Rules (must enforce)

- Never generate child rows before parent rows.
- Use only known IDs/SKUs from lookup structures.
- Wrap each checkout write set in a DB transaction.
- Ensure `order_items.quantity > 0`, `cart_items.quantity > 0`.
- Keep monetary fields non-negative.
- Ensure inventory never goes below zero.
- Keep all FK references valid across tables.
- Ensure Mongo `product_specs.sku` exists in MySQL `product_variant`.

## 6) Validation Checks (post-generation)

Run checks and fail generation if any condition fails:
- orphan FK counts = 0
- `orders.total_amount` matches item/tax/shipping logic
- converted + abandoned + active carts look reasonable
- event counts meet target
- order/event timestamps within configured window
- last-5-views query works for sample users
- cart conversion query (past 30 days) returns plausible rate
