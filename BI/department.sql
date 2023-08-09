--Sales Performance by Department

SELECT S.department_name, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order AS F
JOIN dim_staff AS S ON F.sales_staff_id = S.staff_id
GROUP BY S.department_name;

--Total Sales Revenue and Units Sold by Department

SELECT S.department_name, SUM(F.unit_price * F.units_sold) AS total_revenue, SUM(F.units_sold) AS total_units_sold
FROM fact_sales_order AS F
JOIN dim_staff S ON F.sales_staff_id = S.staff_id
GROUP BY S.department_name
ORDER BY total_revenue DESC;