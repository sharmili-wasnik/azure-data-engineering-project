SELECT 
    category,
    SUM(price * quantity) AS total_revenue
FROM sales_data
GROUP BY category
ORDER BY total_revenue DESC;
