WITH RankedProducts AS (
    SELECT 
        c.customer_state,
        pt.product_category_name_english,
        COUNT(o.order_id) AS number_of_orders,
        ROW_NUMBER() OVER (PARTITION BY c.customer_state ORDER BY COUNT(o.order_id) DESC) AS product_rank
    FROM olist_orders o
    JOIN olist_customers c ON o.customer_id = c.customer_id
    JOIN olist_order_items oi ON o.order_id = oi.order_id
    JOIN olist_products p ON oi.product_id = p.product_id
    LEFT JOIN product_category_name_translation pt ON p.product_category_name = pt.product_category_name
    WHERE pt.product_category_name_english IS NOT NULL
    GROUP BY c.customer_state, pt.product_category_name_english
)
SELECT 
    customer_state,
    product_category_name_english,
    number_of_orders
FROM RankedProducts
WHERE product_rank <= 5
ORDER BY customer_state, product_rank;


# where are the most popular products peforming worst?
SELECT
    c.customer_state,
    pt.product_category_name_english,
    COUNT(o.order_id) AS total_orders
FROM olist_orders o
JOIN olist_order_items oi ON o.order_id = oi.order_id
JOIN olist_products p ON p.product_id = oi.product_id
JOIN olist_customers c ON c.customer_id = o.customer_id
LEFT JOIN product_category_name_translation pt ON p.product_category_name = pt.product_category_name
WHERE pt.product_category_name_english IN (
    'bed_bath_table',
    'health_beauty',
    'sports_leisure',
    'furniture_decor',
    'computers_accessories'
)
GROUP BY pt.product_category_name_english, c.customer_state
ORDER BY pt.product_category_name_english, total_orders ASC;



# What product categories perform relatively better in low-revenue regions?
SELECT 
    c.customer_state,
    pt.product_category_name_english,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(oi.price), 2) AS total_revenue
FROM 
    olist_orders o
JOIN 
    olist_order_items oi ON o.order_id = oi.order_id
JOIN 
    olist_products p ON oi.product_id = p.product_id
LEFT JOIN 
    product_category_name_translation pt ON p.product_category_name = pt.product_category_name
JOIN 
    olist_customers c ON o.customer_id = c.customer_id
WHERE 
    c.customer_state IN ('AM', 'AP', 'AC', 'RR')
GROUP BY 
    c.customer_state, pt.product_category_name_english
ORDER BY 
    c.customer_state ASC, total_orders DESC;

-- The impact of price / implicit discount on both volume and revenue
-- Scatterplot of sale_price and units_sold, and sales_price and total_revenue to determine relationship
SELECT product_id, 
	price AS sale_price, 
	COUNT(*) AS units_sold, 
	SUM(price) AS total_revenue
FROM olist_order_items ooi 
JOIN olist_orders oo ON ooi.order_id = oo.order_id
WHERE order_status = "delivered"
GROUP BY 1, 2
ORDER BY 1, 2;

-- Time-series of average price, volume, and revenue
-- Line chart with three series of avg price, sales volume, and revenue over time entire period of time
SELECT
  SUBSTR(oo.order_purchase_timestamp, 1, 7) AS `year_month`,
  ROUND(AVG(ooi.price), 2) AS average_price,
  COUNT(*) AS sales_volume,
  SUM(ooi.price) AS total_revenue
FROM olist_order_items AS ooi
JOIN olist_orders AS oo ON ooi.order_id = oo.order_id
WHERE oo.order_status = 'delivered'
GROUP BY SUBSTR(oo.order_purchase_timestamp, 1, 7)
ORDER BY SUBSTR(oo.order_purchase_timestamp, 1, 7);

-- Category level pricing and demand tracking
-- Use bar charts to compare the avg price, total revenue, and volume metrics
SELECT product_category_name_english,
	ROUND(AVG(price), 2) AS average_price,
	SUM(price) AS total_revenue,
	COUNT(*) AS volume
FROM olist_order_items ooi
JOIN olist_orders oo ON ooi.order_id = oo.order_id
JOIN olist_products op ON ooi.product_id = op.product_id
JOIN product_category_name_translation pcnt ON op.product_category_name = pcnt.product_category_name
GROUP BY 1
ORDER BY average_price DESC;





SELECT 
  COUNT(*) AS repeat_customers
FROM (
  SELECT customer_id
  FROM olist_orders
  GROUP BY customer_id
  HAVING COUNT(*) > 1
) t;

SELECT
  COUNT(*)               AS total_orders,
  COUNT(DISTINCT customer_unique_id) AS distinct_customers
FROM olist_orders oo
JOIN olist_customers oc ON oo.customer_id = oc.customer_id;

-- Pricing tiers and Profitability/Retention
-- Side-by-side bar charts of avg first order value and repeat rate percentage for each tier
WITH first_orders AS (
	SELECT customer_unique_id, 
		MIN(order_purchase_timestamp) AS first_transaction
	FROM olist_orders oo
	JOIN olist_customers oc ON oo.customer_id = oc.customer_id
	WHERE order_status = 'delivered'
	GROUP BY customer_unique_id
),
first_order_ids AS (
	SELECT oc.customer_unique_id, order_id
	FROM olist_orders oo
	JOIN olist_customers oc ON oo.customer_id = oc.customer_id
	JOIN first_orders fo ON oc.customer_unique_id = fo.customer_unique_id
		AND oo.order_purchase_timestamp = fo.first_transaction
	WHERE order_status = 'delivered'
),
order_revenue AS (
	SELECT order_id, SUM(price) AS revenue
	FROM olist_order_items
	GROUP BY 1
),
customer_order_count AS (
	SELECT customer_unique_id, COUNT(DISTINCT order_id) AS order_count
	FROM olist_orders oo
	JOIN olist_customers oc ON oo.customer_id = oc.customer_id
	WHERE order_status = 'delivered'
	GROUP BY customer_unique_id
)
SELECT
	CASE
		WHEN `or`.revenue < 50 THEN 'Low-price (<50)'
		WHEN `or`.revenue < 100 THEN 'Mid-price (<100)'
		ELSE 'High-price (>100)'
	END AS price_tier,
	COUNT(*) AS customers, ROUND(AVG(`or`.revenue), 2) AS avg_first_order_value,
	ROUND(
		SUM(CASE WHEN coc.order_count > 1 THEN 1 ELSE 0 END)
		/ COUNT(*) * 100, 
	2) AS repeat_rate_percentage
FROM first_order_ids foi
JOIN order_revenue `or` ON foi.order_id = `or`.order_id
JOIN customer_order_count coc ON foi.customer_unique_id = coc.customer_unique_id
GROUP BY price_tier
ORDER BY price_tier;


# Customer Segmentation SQL Codes

USE maryteke_final_project;

# Customer Location
	# Number of Customers in Each Brazilian State
SELECT customer_state AS State, COUNT(customer_unique_id) AS "Number of Customers"
FROM olist_customers
GROUP BY State
ORDER BY COUNT(customer_unique_id) ASC;

# Customer Product Preferences
	# Most Popular Product in the Top 5 States
WITH total_num_of_cust_orders AS (
SELECT oc.customer_state AS State, COUNT(oc.customer_unique_id) AS "Total Number of Customers Who Ordered"
FROM olist_customers AS oc
WHERE (oc.customer_state = "SP" OR 
		oc.customer_state = "RJ" OR 
        oc.customer_state = "MG" OR 
        oc.customer_state = "RS" OR 
        oc.customer_state = "PR") 
GROUP BY State
ORDER BY State ASC),
ranked_products_by_state AS (
SELECT oc.customer_state AS State, pc.product_category_name_english AS "Product Category", 
	COUNT(oc.customer_unique_id) AS "Number of Customers Who Ordered", 
	ROW_NUMBER() OVER (PARTITION BY oc.customer_state ORDER BY COUNT(DISTINCT oc.customer_unique_id) DESC) AS rn
FROM olist_customers AS oc
JOIN olist_orders AS o
	ON oc.customer_id = o.customer_id
JOIN olist_order_items AS oi
	ON o.order_id = oi.order_id
JOIN olist_products AS op
	ON oi.product_id = op.product_id
JOIN product_category_name_translation AS pc
	ON op.product_category_name = pc.product_category_name
GROUP BY State, pc.product_category_name_english
ORDER BY State ASC, COUNT(oc.customer_unique_id) DESC)
SELECT r.State, r.`Product Category`, r.`Number of Customers Who Ordered`,
       t.`Total Number of Customers Who Ordered`,
       100*((r.`Number of Customers Who Ordered`)/(t.`Total Number of Customers Who Ordered`)) AS "Percentage of Total Orders"
FROM ranked_products_by_state AS r
JOIN total_num_of_cust_orders AS t
	ON r.State = t.State
WHERE (r.State = "SP" OR r.State = "RJ" OR r.State = "MG" OR r.State = "RS" OR r.State = "PR") AND r.rn = 1 
#ORDER BY `Number of Customers Who Ordered` DESC;
ORDER BY State ASC;

# Customer Satisfaction Rates for Each Product Category Based on Customer Reviews
WITH ranked_reviewed_product_score AS (
SELECT p.product_category_name_english AS Product, r.review_score AS Score, 
	COUNT(r.review_score) AS "Number of Customers",
    ROW_NUMBER() OVER (PARTITION BY p.product_category_name_english ORDER BY COUNT(DISTINCT r.review_score) DESC) AS rn
FROM product_category_name_translation AS p
JOIN olist_products AS op
	ON p.product_category_name = op.product_category_name
JOIN olist_order_items AS oi
	ON op.product_id = oi.product_id
JOIN olist_order_reviews AS r
	ON oi.order_id = r.order_id
GROUP BY Product, Score
ORDER BY Product ASC, COUNT(r.review_score) DESC),
num_of_total_cust_review AS (
SELECT pc.product_category_name_english AS "Product Category", 
	   COUNT(r.review_id) AS "Number of Customers Who Reviewed"
FROM olist_order_reviews AS r
JOIN olist_orders AS o
	ON r.order_id = o.order_id
JOIN olist_order_items AS oi
	ON o.order_id = oi.order_id
JOIN olist_products AS op
	ON oi.product_id = op.product_id
JOIN product_category_name_translation AS pc
	ON op.product_category_name = pc.product_category_name
GROUP BY pc.product_category_name_english
ORDER BY pc.product_category_name_english ASC)
SELECT r.Product, r.Score, 
	r.`Number of Customers` AS "Number of Customers Who Gave Top Review Score", 
    n.`Number of Customers Who Reviewed` AS "Total Number of Customers Who Reviewed Product",
	((r.`Number of Customers`)/(n.`Number of Customers Who Reviewed`))*100 AS "Percentage of Customers that Gave the Top Review Score"
FROM ranked_reviewed_product_score AS r
JOIN num_of_total_cust_review AS n
	ON r.Product = n.`Product Category`
WHERE r.rn = 1
GROUP BY r.Score, r.Product, r.`Number of Customers`, n.`Number of Customers Who Reviewed`
ORDER BY r.Score ASC, "Percentage of Customers that Gave the Top Review Score" DESC;


