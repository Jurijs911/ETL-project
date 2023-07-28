from datetime import datetime
import ccy

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


class InputValidationError(Exception):
    pass


def format_fact_sales_order(sales_data):
    """
    Manipulate sales data to match the format of the fact_sales_order
    table in the data warehouse.
    """
    formatted_data = []
    for sale in sales_data:
        for index, column in enumerate(sale):
            if index not in (1, 2) and not isinstance(column, str):
                raise InputValidationError
            elif index in (1, 2) and not isinstance(column, datetime):
                raise InputValidationError
            if index in (0, 3, 4, 5, 11):
                try:
                    int(column)
                except ValueError:
                    raise InputValidationError
        if len(sale) != 12:
            raise InputValidationError

        created_date = sale[1].strftime("%Y-%m-%d")
        created_time = sale[1].strftime("%H:%M:%S:%f")
        last_updated_date = sale[2].strftime("%Y-%m-%d")
        last_updated_time = sale[2].strftime("%H:%M:%S:%f")

        formatted_sale = {
            "sales_order_id": sale[0],
            "created_date": created_date,
            "created_time": created_time,
            "last_updated_date": last_updated_date,
            "last_updated_time": last_updated_time,
            "sales_staff_id": sale[4],
            "counterparty_id": sale[5],
            "units_sold": sale[6],
            "unit_price": sale[7],
            "currency_id": sale[8],
            "design_id": sale[3],
            "agreed_payment_date": sale[10],
            "agreed_delivery_date": sale[9],
            "agreed_delivery_location_id": sale[11],
        }
        formatted_data.append(formatted_sale)
    return formatted_data


def format_dim_design(design_data):
    """
    Manipulate design data to match the format of the dim_design
    table in the data warehouse.
    """
    formatted_data = []
    for design in design_data:
        for index, column in enumerate(design):
            if index not in (1, 2) and not isinstance(column, str):
                raise InputValidationError
            elif index in (1, 2) and not isinstance(column, datetime):
                raise InputValidationError
            if index == 0:
                try:
                    int(column)
                except ValueError:
                    raise InputValidationError
        if len(design) != 6:
            raise InputValidationError

        formatted_design = {
            "design_id": design[0],
            "design_name": design[3],
            "file_location": design[4],
            "file_name": design[5],
        }
        formatted_data.append(formatted_design)
    return formatted_data


def format_dim_staff(staff_data, department_data):
    """
    Manipulate staff data to match the format of the dim_staff
    table in the data warehouse.
    """
    formatted_data = []
    for staff in staff_data:
        for dep in department_data:
            if dep[0] == staff[3]:
                formatted_staff = {
                    "staff_id": staff[0],
                    "first_name": staff[1],
                    "last_name": staff[2],
                    "department_name": dep[1],
                    "location": dep[2],
                    "email_address": staff[4],
                }
                formatted_data.append(formatted_staff)
    return formatted_data

def validate_dim_staff_data(staff_data, department_data):
    for staff in staff_data:
        if len(staff) != 5:
            raise InputValidationError
        for dep in department_data:
            if dep[0] == staff[3]:
                if not all(isinstance(item, str) for item in staff[1:]):
                    raise InputValidationError
                break
        else:
            raise InputValidationError



def format_dim_location(location_data):
    """
    Manipulate location data to match the format of the dim_location
    table in the data warehouse.
    """
    formatted_data = []
    for location in location_data:
        formatted_location = {
            "location_id": location[0],
            "address_line_1": location[1],
            "address_line_2": location[2],
            "district": location[3],
            "city": location[4],
            "postal_code": location[5],
            "country": location[6],
            "phone": location[7],
        }
        formatted_data.append(formatted_location)
    return formatted_data

def validate_dim_location_data(location_data):
    for location in location_data:
        if len(location) != 8:
            raise InputValidationError
        if not all(isinstance(item, str) for item in location[1:]):
            raise InputValidationError


def format_dim_date(date_data):
    """
    Manipulate date data to match the format of the dim_date
    table in the data warehouse.
    """
    formatted_data = []
    date_obj = datetime.strptime(date_data, "%Y-%m-%d")
    formatted_date = {
        "date_id": date_data,
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

def validate_dim_date_data(date_data):
    try:
        datetime.strptime(date_data, "%Y-%m-%d")
    except ValueError:
        raise InputValidationError


def format_dim_currency(currency_data):
    """
    Manipulate currency data to match the format of the dim_currency
    table in the data warehouse.
    """
    formatted_data = []
    for currency in currency_data:
        formatted_currency = {
            "currency_id": currency[0],
            "currency_code": currency[1],
            "currency_name": ccy.currency(currency[1]).name,
        }
        formatted_data.append(formatted_currency)
    return formatted_data

def validate_dim_currency_data(currency_data):
    for currency in currency_data:
        if len(currency) != 2:
            raise InputValidationError
        if not isinstance(currency[1], str):
            raise InputValidationError


def format_dim_counterparty(counterparty_data, location_data):
    """
    Manipulate counterparty data to match the format of the
    dim_counterparty table in the data warehouse.
    """
    formatted_data = []
    for counterparty in counterparty_data:
        for location in location_data:
            if location[0] == counterparty[2]:
                formatted_counterparty = {
                    "counterparty_id": counterparty[0],
                    "counterparty_legal_name": counterparty[1],
                    "counterparty_legal_address_line_1": location[1],
                    "counterparty_legal_address_line2": location[2],
                    "counterparty_legal_district": location[3],
                    "counterparty_legal_city": location[4],
                    "counterparty_legal_postal_code": location[5],
                    "counterparty_legal_country": location[6],
                    "counterparty_legal_phone_number": location[7],
                }
                formatted_data.append(formatted_counterparty)
    return formatted_data

def validate_dim_counterparty_data(counterparty_data, location_data):
    for counterparty in counterparty_data:
        if len(counterparty) != 3:
            raise InputValidationError
        if not isinstance(counterparty[1], str):
            raise InputValidationError
        for location in location_data:
            if location[0] == counterparty[2]:
                if not all(isinstance(item, str) for item in location[1:]):
                    raise InputValidationError
                break
        else:
            raise InputValidationError
