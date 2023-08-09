--Average Units Sold by Design Name

SELECT D.design_name, AVG(F.units_sold) AS avg_units_sold
FROM fact_sales_order AS F
JOIN dim_design D ON F.design_id = D.design_id
GROUP BY D.design_name;

--Sales Count and Revenue by Design Name and Year

SELECT D.design_name, DT.year, COUNT(*) AS sales_count, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order AS F
JOIN dim_design D ON F.design_id = D.design_id
JOIN dim_date DT ON F.created_date = DT.date_id
GROUP BY D.design_name, DT.year
ORDER BY D.design_name, DT.year;

--Total Sales Revenue by Design

SELECT D.design_name, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order AS F
JOIN dim_design AS D ON F.design_id = D.design_id
GROUP BY D.design_name;

--Top Designs by Total Sales Revenue

SELECT D.design_name, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order AS F
JOIN dim_design AS D ON F.design_id = D.design_id
GROUP BY D.design_name
ORDER BY total_revenue DESC
LIMIT 5;

--Units Sold by Design and Quarter

SELECT D.design_name, DT.quarter, SUM(F.units_sold) AS total_units_sold
FROM fact_sales_order AS F
JOIN dim_design AS D ON F.design_id = D.design_id
JOIN dim_date AS DT ON F.created_date = DT.date_id
GROUP BY D.design_name, DT.quarter
ORDER BY D.design_name, DT.quarter;

--Average Unit Price by Design

SELECT D.design_name, AVG(F.unit_price) AS avg_unit_price
FROM fact_sales_order AS F
JOIN dim_design AS D ON F.design_id = D.design_id
GROUP BY D.design_name;

--Sales Revenue Distribution by Design and Department

SELECT D.design_name, S.department_name, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order AS F
JOIN dim_design AS D ON F.design_id = D.design_id
JOIN dim_staff AS S ON F.sales_staff_id = S.staff_id
GROUP BY D.design_name, S.department_name;

--Monthly Sales Revenue Trend for Top Designs

WITH TopDesigns AS (
  SELECT D.design_id, D.design_name
  FROM dim_design AS D
  ORDER BY D.design_id
  LIMIT 5
)
SELECT TD.design_name, DT.year, DT.month, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order AS F
JOIN TopDesigns AS TD ON F.design_id = TD.design_id
JOIN dim_date AS DT ON F.created_date = DT.date_id
GROUP BY TD.design_name, DT.year, DT.month
ORDER BY TD.design_name, DT.year, DT.month;



