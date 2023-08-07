--Top Sales Staff by Total Sales Revenue

SELECT S.staff_id, S.first_name, S.last_name, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_staff S ON F.sales_staff_id = S.staff_id
GROUP BY S.staff_id, S.first_name, S.last_name
ORDER BY total_revenue DESC
LIMIT 5;

--Average Sales Revenue per Sales Staff

SELECT S.staff_id, S.first_name, S.last_name, AVG(F.unit_price * F.units_sold) AS avg_revenue_per_staff
FROM fact_sales_order F
JOIN dim_staff S ON F.sales_staff_id = S.staff_id
GROUP BY S.staff_id, S.first_name, S.last_name;

--Average Units Sold and Average Unit Price by Sales Staff

SELECT S.staff_id, S.first_name, S.last_name,
       AVG(F.units_sold) AS avg_units_sold,
       AVG(F.unit_price) AS avg_unit_price
FROM fact_sales_order F
JOIN dim_staff S ON F.sales_staff_id = S.staff_id
GROUP BY S.staff_id, S.first_name, S.last_name;

--Sales Performance Comparison of Top Sales Staff

WITH TopSalesStaff AS (
  SELECT S.staff_id, S.first_name, S.last_name, SUM(F.unit_price * F.units_sold) AS total_revenue
  FROM fact_sales_order F
  JOIN dim_staff S ON F.sales_staff_id = S.staff_id
  GROUP BY S.staff_id, S.first_name, S.last_name
  ORDER BY total_revenue DESC
  LIMIT 5
)
SELECT TSS.staff_id, TSS.first_name, TSS.last_name, TSS.total_revenue,
       SUM(F.unit_price * F.units_sold) AS total_revenue_all_staff
FROM fact_sales_order F
JOIN TopSalesStaff TSS ON F.sales_staff_id = TSS.staff_id
GROUP BY TSS.staff_id, TSS.first_name, TSS.last_name, TSS.total_revenue
ORDER BY TSS.total_revenue DESC;

--Total Sales Revenue and Units Sold by Staff and Quarter

SELECT DT.year, DT.quarter, S.staff_id, S.first_name, S.last_name,
       SUM(F.unit_price * F.units_sold) AS total_revenue, SUM(F.units_sold) AS total_units_sold
FROM fact_sales_order F
JOIN dim_staff S ON F.sales_staff_id = S.staff_id
JOIN dim_date DT ON F.created_date = DT.date_id
GROUP BY DT.year, DT.quarter, S.staff_id, S.first_name, S.last_name
ORDER BY DT.year, DT.quarter, total_revenue DESC;