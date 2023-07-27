from manipulation_utils import (
    format_dim_counterparty,
    format_dim_currency,
    format_dim_date,
    format_dim_design,
    format_dim_location,
    format_dim_staff,
    format_fact_sales_order,
)
from read_ingestion_csv import read_ingestion_csv
import upload_csv
from datetime import datetime


def lambda_handler(event, context):
    ingested_data = read_ingestion_csv()

    formatted_sales_orders = format_fact_sales_order(
        ingested_data["sales_order"]
    )
    formatted_designs = format_dim_design(ingested_data["design"])
    formatted_staff = format_dim_staff(
        ingested_data["staff"], ingested_data["department"]
    )
    formatted_locations = format_dim_location(ingested_data["address"])
    formatted_currencies = format_dim_currency(ingested_data["currency"])
    formatted_counterparties = format_dim_counterparty(
        ingested_data["counterparty"], ingested_data["address"]
    )
    formatted_dates = []
    for table in ingested_data:
        for row in table:
            if isinstance(row, datetime):
                formatted_dates.append(format_dim_date(row))

    bucket_name = "kp-northcoders-processed-bucket"

    upload_csv(
        formatted_sales_orders,
        "fact_sales_order",
        bucket_name,
    )
    upload_csv(formatted_designs, "dim_design", bucket_name)
    upload_csv(formatted_staff, "dim_staff", bucket_name)
    upload_csv(formatted_locations, "dim_location", bucket_name)
    upload_csv(formatted_currencies, "dim_currency", bucket_name)
    upload_csv(formatted_counterparties, "dim_counterparty", bucket_name)
