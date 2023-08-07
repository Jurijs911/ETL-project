--Average Units Sold by Design Name

SELECT D.design_name, AVG(F.units_sold) AS avg_units_sold
FROM fact_sales_order F
JOIN dim_design D ON F.design_id = D.design_id
GROUP BY D.design_name;

--Sales Count and Revenue by Design Name and Year

SELECT D.design_name, DT.year, COUNT(*) AS sales_count, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_design D ON F.design_id = D.design_id
JOIN dim_date DT ON F.created_date = DT.date_id
GROUP BY D.design_name, DT.year
ORDER BY D.design_name, DT.year;

--Monthly Sales Revenue Trend for a Specific Design

SELECT DT.year, DT.month, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_design D ON F.design_id = D.design_id
JOIN dim_date DT ON F.created_date = DT.date_id
WHERE D.design_name = 'Your Design Name Here'
GROUP BY DT.year, DT.month
ORDER BY DT.year, DT.month;

