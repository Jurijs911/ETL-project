from datetime import datetime

"""
Receives data from csv_reader and manipulates it to match
the final database schema

format_fact_sales_order
format_dim_staff
format_dim_location
format_dim_design
format_dim_date
format_dim_currency
format_dim_counterparty
"""


def format_dim_design(design_data):
    """
    Manipulate design data to match the format of the dim_design
    table in the data warehouse.
    """
    formatted_data = []
    for design in design_data:
        formatted_design = {
            "design_id": design["design_id"],
            "design_name": design["design_name"],
            "file_location": design["file_location"],
            "file_name": design["file_name"],
        }
        formatted_data.append(formatted_design)
    return formatted_data


def format_fact_sales_order(sales_data):
    """
    Manipulate sales data to match the format of the fact_sales_order
    table in the data warehouse.
    """
    formatted_data = []
    for sale in sales_data:
        created_date = sale["created_at"].strftime("%Y-%m-%d")
        created_time = sale["created_at"].strftime("%H:%M:%S:%f")
        last_updated_date = sale["last_updated"].strftime("%Y-%m-%d")
        last_updated_time = sale["last_updated"].strftime("%H:%M:%S:%f")

        formatted_sale = {
            "sales_order_id": sale["sales_order_id"],
            "created_date": created_date,
            "created_time": created_time,
            "last_updated_date": last_updated_date,
            "last_updated_time": last_updated_time,
            "sales_staff_id": sale["sales_staff_id"],
            "counterparty_id": sale["counterparty_id"],
            "units_sold": sale["units_sold"],
            "unit_price": sale["unit_price"],
            "currency_id": sale["currency_id"],
            "design_id": sale["design_id"],
            "agreed_payment_date": sale["agreed_payment_date"],
            "agreed_delivery_date": sale["agreed_delivery_date"],
            "agreed_delivery_location_id": sale["agreed_delivery_location_id"],
        }
        formatted_data.append(formatted_sale)
    return formatted_data


# [1, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh', '28441',
#      'Turkey', '1803 637401', datetime.datetime(
#          2023, 7, 25, 15, 20, 49, 962000),
#      datetime.datetime(2023, 7, 25, 15, 20, 49, 962000)],
#     [2, '1234 Calle Norte', None, 'North District', 'Madrid', '112250',
#      'Spain', '0123 14567', datetime.datetime(
#          2019, 11, 3, 14, 20, 49, 962000),
#      datetime.datetime(
#          2019, 11, 3, 14, 20, 49, 962000)]]


def format_dim_staff(staff_data):
    """
    Manipulate staff data to match the format of the dim_staff
    table in the data warehouse.
    """
    formatted_data = []
    for staff in staff_data:
        formatted_staff = {
            "staff_id": staff["staff_id"],
            "first_name": staff["first_name"],
            "last_name": staff["last_name"],
            "department_name": staff["department_name"],
            "location": staff["location"],
            "email_address": staff["email_address"],
        }
        formatted_data.append(formatted_staff)
    return formatted_data


def format_dim_location(location_data):
    """
    Manipulate location data to match the format of the dim_location
    table in the data warehouse.
    """
    formatted_data = []
    for location in location_data:
        formatted_location = {
            "location_id": location["location_id"],
            "address_line_1": location["address_line_1"],
            "address_line_2": location["address_line_2"],
            "district": location["district"],
            "city": location["city"],
            "postal_code": location["postal_code"],
            "country": location["country"],
            "phone": location["phone"],
        }
        formatted_data.append(formatted_location)
    return formatted_data


def format_dim_date(date_data):
    """
    Manipulate date data to match the format of the dim_date
    table in the data warehouse.
    """
    formatted_data = []
    for date_info in date_data:
        date_id = date_info["date_id"]
        date_obj = datetime.strptime(date_id, "%Y-%m-%d")
        formatted_date = {
            "date_id": date_id,
            "year": date_obj.year,
            "month": date_obj.month,
            "day": date_obj.day,
            "day_of_week": date_obj.weekday(),
            "day_name": date_obj.strftime("%A"),
            "month_name": date_obj.strftime("%B"),
            "quarter": (date_obj.month - 1) // 3 + 1,
        }
        formatted_data.append(formatted_date)
    return formatted_data


def format_dim_currency(currency_data):
    """
    Manipulate currency data to match the format of the dim_currency
    table in the data warehouse.
    """
    formatted_data = []
    for currency in currency_data:
        formatted_currency = {
            "currency_id": currency["currency_id"],
            "currency_code": currency["currency_code"],
            "currency_name": currency["currency_name"],
        }
        formatted_data.append(formatted_currency)
    return formatted_data


def format_dim_counterparty(counterparty_data):
    """
    Manipulate counterparty data to match the format of the
    dim_counterparty table in the data warehouse.
    """
    formatted_data = []
    for counterparty in counterparty_data:
        formatted_counterparty = {
            "counterparty_id": counterparty["counterparty_id"],
            "counterparty_legal_name": counterparty["counterparty_legal_name"],
            "counterparty_legal_address_line_1": counterparty[
                "counterparty_legal_address_line_1"
            ],
            "counterparty_legal_address_line2": counterparty[
                "counterparty_legal_address_line2"
            ],
            "counterparty_legal_district": counterparty[
                "counterparty_legal_district"
            ],
            "counterparty_legal_city": counterparty["counterparty_legal_city"],
            "counterparty_legal_postal_code": counterparty[
                "counterparty_legal_postal_code"
            ],
            "counterparty_legal_country": counterparty[
                "counterparty_legal_country"
            ],
            "counterparty_legal_phone_number": counterparty[
                "counterparty_legal_phone_number"
            ],
        }
        formatted_data.append(formatted_counterparty)
    return formatted_data
