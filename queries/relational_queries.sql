-- 2. Retrieve the last five products viewed by Sarah within the past six months, ordered by most recent activity.
SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    p.sku,
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
ORDER BY e.created_at DESC
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
ORDER BY qty_available DESC
;

-- 5. Display the number of times each product pages has been viewed, ordered by popularity 
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

-- 6. Retrieve all recent search terms used by the user and categorize them based on frequency and time of day.
WITH search_terms AS (
    SELECT
        user_id,
        event_id,
        JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.term')) AS search_term,
        CASE
            WHEN HOUR(created_at) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN HOUR(created_at) BETWEEN 12 AND 17 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day
    FROM event_log e
    WHERE event_type = 'search'
        AND user_id = 882
)
SELECT
    user_id, search_term, time_of_day,
    COUNT(DISTINCT event_id) AS search_count
FROM search_terms
GROUP BY
    user_id, search_term, time_of_day
ORDER BY search_count DESC, search_term
;

-- 7. Fetch carts information such as device type (e.g., laptop, tablet), the number of items in the cart, and total amount.
SELECT 
    c.cart_id,
    d.device_type,
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
SELECT
    o.order_id,
    o.order_date,
    o.status AS order_status,
    o.shipping_option,
    pt.payment_type,
    pt.status AS payment_status,
    oi.quantity,
    pr.name AS product_name,
    pr.brand
FROM orders o
JOIN order_items oi
    ON o.order_id = oi.order_id
JOIN product_variant pv
    ON oi.sku = pv.sku
JOIN product pr
    ON pv.product_id = pr.product_id
LEFT JOIN payment_transactions pt
    ON o.order_id = pt.order_id
WHERE o.user_id = 882
ORDER BY o.order_date DESC, o.order_id DESC, oi.order_item_id
;



-- 9. List all items returned by the user, along with the refund status, amount, and any restocking fees.
SELECT
    o.user_id,
    o.order_id,
    r.return_id,
    r.status AS refund_status,
    r.created_at AS return_created_at,
    pr.name AS product_name,
    pr.brand,
    ri.quantity AS returned_qty,
    ri.refund_amount,
    ri.restocking_fee
FROM returns r
JOIN orders o
    ON r.order_id = o.order_id
JOIN return_items ri
    ON r.return_id = ri.return_id
JOIN order_items oi
    ON ri.order_item_id = oi.order_item_id
JOIN product_variant pv
    ON oi.sku = pv.sku
JOIN product pr
    ON pv.product_id = pr.product_id
WHERE o.user_id = 882
ORDER BY r.created_at DESC
;


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
    ROUND(
        100 * COUNT(DISTINCT CASE WHEN status <> "converted" THEN cart_id ELSE NULL END) 
        / COUNT(DISTINCT cart_id),
        2) AS pct_abandoned
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
;
