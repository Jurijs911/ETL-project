--Top Counterparties by Total Sales Revenue

SELECT CO.counterparty_id, CO.counterparty_legal_name, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_counterparty CO ON F.counterparty_id = CO.counterparty_id
GROUP BY CO.counterparty_id, CO.counterparty_legal_name
ORDER BY total_revenue DESC
LIMIT 5;

--Top Counterparties by Total Units Sold

SELECT CO.counterparty_id, CO.counterparty_legal_name, SUM(F.units_sold) AS total_units_sold
FROM fact_sales_order F
JOIN dim_counterparty CO ON F.counterparty_id = CO.counterparty_id
GROUP BY CO.counterparty_id, CO.counterparty_legal_name
ORDER BY total_units_sold DESC
LIMIT 5;

