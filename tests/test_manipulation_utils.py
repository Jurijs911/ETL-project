from src.remodelling.manipulation_utils import format_fact_sales_order
from datetime import datetime
def test_format_fact_sales_order():
    sample_sales_data = [
        {
           
            "sales_order_id": 2,
            "created_at": datetime.datetime(2023, 7, 25, 15, 20, 49, 962000),
            "last_updated": datetime.datetime(2023, 7, 25, 15, 20, 49, 962000),
            "sales_staff_id": 100,
            "counterparty_id": 200,
            "units_sold": 2000,
            "unit_price": 20.65,
            "currency_id": 5,
            "design_id": 1,
            "agreed_payment_date": '2023, 7, 30',
            "agreed_delivery_date": '2023, 8, 12',
            "agreed_delivery_location_id": 2,
        },
    ]

    formatted_data = format_fact_sales_order(sample_sales_data)
    expected_sales_data = [
        {
           
            "sales_order_id": 2,
            "created_date": '2023-7-25',
            "created_time": '15:20:49:962000',
            "last_updated_date": '2023-7-25',
            "last_updated_time": '15:20:49:962000',
            "sales_staff_id": 100,
            "counterparty_id": 200,
            "units_sold": 2000,
            "unit_price": 20.65,
            "currency_id": 5,
            "design_id": 1,
            "agreed_payment_date": '2023, 7, 30',
            "agreed_delivery_date": '2023, 8, 12',
            "agreed_delivery_location_id": 2,
        },
    ]
    assert formatted_data == expected_sales_data
