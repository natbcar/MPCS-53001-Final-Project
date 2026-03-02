-- 2. Retrieve the last five products viewed by Sarah within the past six months, ordered by most recent activity.
SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    p.*,
    pr.name,
    pr.brand,
    pr.description
FROM users u
JOIN event_log e 
    ON u.user_id = e.user_id
JOIN product_variant p 
    ON e.sku = p.sku 
JOIN product pr 
    ON p.product_id = pr.product_id
WHERE u.user_id = 882
    AND e.event_type = "view"
ORDER BY created_at DESC
LIMIT 5
;

-- 3. Check the current stock level for all items and return only items that are low in stock
SELECT
    pr.name,
    pr.brand,
    pr.manufacturer,
    qty_available
FROM inventory i
JOIN product_variant p ON i.sku = p.sku 
JOIN product pr ON p.product_id = pr.product_id
WHERE i.qty_available < 5 AND p.status = 'active'
;

-- 5. 
SELECT
    pr.product_id,
    pr.name,
    pr.brand,
    COUNT(DISTINCT event_id) n_views,
    COUNT(DISTINCT e.user_id) n_user
FROM event_log e 
JOIN product_variant p ON e.sku = p.sku 
JOIN product pr ON p.product_id = pr.product_id
WHERE event_type = "view"
GROUP BY 
    pr.product_id,
    pr.name,
    pr.brand
ORDER BY n_views DESC
;

-- 6. 
SELECT
    user_id,
    GROUP_CONCAT(search_term)
FROM (
    SELECT
        *,
        JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.term')) AS search_term
    FROM event_log
    WHERE event_type = "search"
) s
GROUP BY user_id
;

-- 7. Fetch carts information such as device type (e.g., laptop, tablet), the number of items in the cart, and total amount.
SELECT DISTINCT
    c.cart_id,
    d.device_type,
    ci.cart_item_id,
    p.price
    COUNT(DISTINCT ci.cart_item_id) item_count,
    SUM(p.price) total_price
FROM cart c 
JOIN cart_items ci ON c.cart_id = ci.cart_id
JOIN product_variant p ON ci.sku = p.sku
JOIN session s ON c.session_id = s.session_id
JOIN devices d ON s.device_id = d.device_id
GROUP BY c.cart_id, d.device_type
ORDER BY total_price DESC
;

-- 8. Retrieve all orders placed by Sarah, showing order IDs, item details, payment methods, shipping options chosen, and the status of each order.



-- 10.  Retrieve the average number of days between purchases for Sarah.
WITH orders_diff AS (
    SELECT
        order_id,
        order_date,
        ROW_NUMBER() OVER(ORDER BY order_date DESC) rn
    FROM orders
    WHERE user_id = 882
    ORDER BY rn
)
SELECT
    AVG(DATEDIFF(t2.order_date, t1.order_date)) avg_days_between_order
FROM orders_diff t1
LEFT JOIN orders_diff t2 ON t1.rn = t2.rn + 1
;

-- 11. Calculate the percentage of carts that did not convert to orders in the past 30 days.
SELECT
    COUNT(DISTINCT CASE WHEN status <> "converted" THEN cart_id ELSE NULL END) / COUNT(DISTINCT cart_id) AS pct_abandoned
FROM cart 
WHERE created_at BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE
;

-- 13. For each user, compute days since last purchase and total order count.
SELECT
    u.user_id,
    MIN(DATEDIFF(CURRENT_DATE, o.order_date)) days_since_last_order,
    COUNT(DISTINCT o.order_id) total_orders
FROM users u 
JOIN orders o ON u.user_id = o.order_id 
GROUP BY u.user_id 
ORDER BY days_since_last_order
;