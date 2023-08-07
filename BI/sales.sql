--Total Sales Revenue by Currency Code

SELECT C.currency_code, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_currency C ON F.currency_id = C.currency_id
GROUP BY C.currency_code;

--Sales Revenue by Quarter and Year

SELECT DT.year, DT.quarter, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_date DT ON F.created_date = DT.date_id
GROUP BY DT.year, DT.quarter
ORDER BY DT.year, DT.quarter;

--Top Sales Staff by Total Sales Revenue

SELECT S.staff_id, S.first_name, S.last_name, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_staff S ON F.sales_staff_id = S.staff_id
GROUP BY S.staff_id, S.first_name, S.last_name
ORDER BY total_revenue DESC
LIMIT 5;

--Sales Count by Day of Week

SELECT DT.day_name, COUNT(*) AS sales_count
FROM fact_sales_order F
JOIN dim_date DT ON F.created_date = DT.date_id
GROUP BY DT.day_name
ORDER BY DT.day_of_week;

--Total Sales Revenue by Country

SELECT LOC.country, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_location LOC ON F.agreed_delivery_location_id = LOC.location_id
GROUP BY LOC.country;

--Total Sales Revenue by Month and Year

SELECT DT.year, DT.month, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_date DT ON F.created_date = DT.date_id
GROUP BY DT.year, DT.month
ORDER BY DT.year, DT.month;

--Sales Revenue by Quarter and Design

SELECT DT.year, DT.quarter, D.design_name, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_design D ON F.design_id = D.design_id
JOIN dim_date DT ON F.created_date = DT.date_id
GROUP BY DT.year, DT.quarter, D.design_name
ORDER BY DT.year, DT.quarter, D.design_name;

--Sales Revenue by Month and Country

SELECT DT.year, DT.month, LOC.country, SUM(F.unit_price * F.units_sold) AS total_revenue
FROM fact_sales_order F
JOIN dim_location LOC ON F.agreed_delivery_location_id = LOC.location_id
JOIN dim_date DT ON F.created_date = DT.date_id
GROUP BY DT.year, DT.month, LOC.country
ORDER BY DT.year, DT.month, LOC.country;

