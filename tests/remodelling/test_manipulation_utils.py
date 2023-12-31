from src.remodelling.manipulation_utils import (
    format_fact_sales_order,
    format_dim_design,
    format_dim_staff,
    format_dim_location,
    format_dim_date,
    format_dim_currency,
    format_dim_counterparty,
    InputValidationError,
)
import pytest


class Test_Format_Fact_Sales_Order:
    """
    Test cases for the format_fact_sales_order function.
    """

    def test_formats_data_correctly(self):
        """
        Test if the function formats sales data correctly to match the
        'fact_sales_order' table schema.
        """
        sample_sales_data = [
            [
                "2",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
                "100",
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        formatted_data = format_fact_sales_order(sample_sales_data)

        expected_sales_data = [
            {
                "sales_order_id": "2",
                "created_date": "2023-07-25",
                "created_time": "15:20:49.962000",
                "last_updated_date": "2023-07-25",
                "last_updated_time": "15:20:49.962000",
                "sales_staff_id": "200",
                "counterparty_id": "2000",
                "units_sold": "5",
                "unit_price": "20.65",
                "currency_id": "1",
                "design_id": "100",
                "agreed_payment_date": "2023, 8, 12",
                "agreed_delivery_date": "2023, 7, 30",
                "agreed_delivery_location_id": "2",
            },
        ]
        assert formatted_data == expected_sales_data

    def test_raises_exception_when_input_is_wrong_type(self):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong data types.
        """
        sample_sales_data = [
            [
                "2",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
                True,
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_fact_sales_order(sample_sales_data)

    def test_raises_exception_when_input_has_id_that_cannot_convert_to_integer(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        contains an ID that cannot be converted to an integer.
        """
        sample_sales_data = [
            [
                "2",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
                "letters",
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_fact_sales_order(sample_sales_data)

    def test_raises_exception_when_input_is_wrong_length(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong length.
        """
        sample_sales_data = [
            [
                "2",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_fact_sales_order(sample_sales_data)


class Test_Format_Dim_Design:
    """
    Test cases for the format_dim_design function.
    """

    def test_formats_data_correctly(self):
        """
        Test if the function formats design data correctly to match the
        'dim_design' table schema.
        """
        sample_design_data = [
            [
                "1",
                "2023-06-12 15:20:49.962000",
                "design 1",
                "./design.jpg",
                "design.jpg",
                "2023-06-12 15:20:49.962000",
            ],
        ]

        formatted_design = format_dim_design(sample_design_data)

        expected_design_data = [
            {
                "design_id": "1",
                "design_name": "design 1",
                "file_location": "./design.jpg",
                "file_name": "design.jpg",
            },
        ]

        assert formatted_design == expected_design_data

    def test_raises_exception_when_input_is_wrong_type(self):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong data types.
        """
        sample_design_data = [
            [
                "1",
                "2023-06-12 15:20:49.962000",
                None,
                "./design.jpg",
                "design.jpg",
                "2023-06-12 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_design(sample_design_data)

    def test_raises_exception_when_input_has_id_that_cannot_convert_to_integer(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        contains an ID that cannot be converted to an integer.
        """
        sample_design_data = [
            [
                "letters",
                "2023-06-12 15:20:49.962000",
                "design 1",
                "./design.jpg",
                "design.jpg",
                "2023-06-12 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_design(sample_design_data)

    def test_raises_exception_when_input_is_wrong_length(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong length.
        """
        sample_design_data = [
            [
                "letters",
                "design.jpg",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_design(sample_design_data)


class Test_Format_Dim_Staff:
    """
    Test cases for the format_dim_staff function.
    """

    def test_format_dim_staff(self):
        """
        Test if the function formats staff data correctly to match the
        'dim_staff' table schema.
        """
        sample_staff_data = [
            [
                "1",
                "zenab",
                "haider",
                "1",
                "zenab@gmail.com",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]
        sample_department_data = [
            [
                "1",
                "coding",
                "manchester",
                "zenab",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]

        formatted_staff = format_dim_staff(
            sample_staff_data, sample_department_data
        )

        expected_staff_data = [
            {
                "staff_id": "1",
                "first_name": "zenab",
                "last_name": "haider",
                "department_name": "coding",
                "location": "manchester",
                "email_address": "zenab@gmail.com",
            },
        ]

        assert formatted_staff == expected_staff_data

    def test_raises_exception_when_input_is_wrong_type(self):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong data types.
        """
        sample_staff_data = [
            [
                "1",
                "zenab",
                "haider",
                "1",
                "zenab@gmail.com",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]
        sample_department_data = [
            [
                "1",
                "coding",
                "manchester",
                5,
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_staff(sample_staff_data, sample_department_data)

    def test_raises_exception_when_input_has_id_that_cannot_convert_to_integer(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        contains an ID that cannot be converted to an integer.
        """
        sample_staff_data = [
            [
                "1",
                "zenab",
                "haider",
                "letters",
                "zenab@gmail.com",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]
        sample_department_data = [
            [
                "letters",
                "coding",
                "manchester",
                "zenab",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_staff(sample_staff_data, sample_department_data)

    def test_raises_exception_when_input_is_wrong_length(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong length.
        """
        sample_staff_data = [
            [
                "1",
                "zenab",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]
        sample_department_data = [
            [
                "1",
                "coding",
                "manchester",
                "zenab",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_staff(sample_staff_data, sample_department_data)


class Test_Format_Dim_Location:
    """
    Test cases for the format_dim_location function.
    """

    def test_format_dim_location(self):
        """
        Test if the function formats location data correctly to match the
        'dim_location' table schema.
        """
        sample_address = [
            [
                "1",
                "123 apple street",
                "apple street",
                "bolton",
                "greater manchester",
                "ABC 123",
                "England",
                "123 456 789",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        formatted_location = format_dim_location(sample_address)

        expected_location_data = [
            {
                "location_id": "1",
                "address_line_1": "123 apple street",
                "address_line_2": "apple street",
                "district": "bolton",
                "city": "greater manchester",
                "postal_code": "ABC 123",
                "country": "England",
                "phone": "123 456 789",
            },
        ]

        assert formatted_location == expected_location_data

    def test_raises_exception_when_input_is_wrong_type(self):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong data types.
        """
        sample_address = [
            [
                1,
                "123 apple street",
                "apple street",
                "bolton",
                "greater manchester",
                "ABC 123",
                "England",
                "123 456 789",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_location(sample_address)

    def test_raises_exception_when_input_has_id_that_cannot_convert_to_integer(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        contains an ID that cannot be converted to an integer.
        """
        sample_address = [
            [
                "letter",
                "123 apple street",
                "apple street",
                "bolton",
                "greater manchester",
                "ABC 123",
                "England",
                "123 456 789",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_location(sample_address)

    def test_raises_exception_when_input_is_wrong_length(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong length.
        """
        sample_address = [
            [
                "greater manchester",
                "ABC 123",
                "England",
                "123 456 789",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_location(sample_address)


class Test_Format_Dim_Date:
    """
    Test cases for the format_dim_date function.
    """

    def test_format_dim_date(self):
        """
        Test if the function formats date data correctly to match the
        'dim_date' table schema.
        """
        sample_date_data = "2023-01-01"

        formatted_date = format_dim_date(sample_date_data)

        expected_date_data = {
            "date_id": "2023-01-01",
            "year": 2023,
            "month": 1,
            "day": 1,
            "day_of_week": 6,
            "day_name": "Sunday",
            "month_name": "January",
            "quarter": 1,
        }

        assert formatted_date == expected_date_data

    def test_raises_exception_when_input_is_wrong_type(self):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong data types.
        """
        sample_date_data = 1

        with pytest.raises(InputValidationError):
            format_dim_date(sample_date_data)

    def test_raises_exception_when_input_is_invalid_date(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        contains an invalid date.
        """
        sample_date_data = "2023-16-51"

        with pytest.raises(InputValidationError):
            format_dim_date(sample_date_data)


class Test_Format_Dim_Currency:
    """
    Test cases for the format_dim_currency function.
    """

    def test_format_dim_currency(self):
        """
        Test if the function formats currency data correctly to match the
        'dim_currency' table schema.
        """
        sample_currency_data = [
            [
                "1",
                "gbp",
                "2023-06-12 15:20:49.962000",
                "2023-06-12 15:20:49.962000",
            ],
        ]

        formatted_currency = format_dim_currency(sample_currency_data)

        expected_currency_data = [
            {
                "currency_id": "1",
                "currency_code": "gbp",
                "currency_name": "British Pound",
            },
        ]

        assert formatted_currency == expected_currency_data

    def test_raises_exception_when_input_is_wrong_type(self):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong data types.
        """
        sample_currency_data = [
            [
                1,
                "gbp",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_currency(sample_currency_data)

    def test_raises_exception_when_input_has_id_that_cannot_convert_to_integer(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        contains an ID that cannot be converted to an integer.
        """
        sample_currency_data = [
            [
                "letters",
                "gbp",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_currency(sample_currency_data)

    def test_raises_exception_when_input_is_wrong_length(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong length.
        """
        sample_currency_data = [
            [
                "1",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_currency(sample_currency_data)

    def test_raises_exception_when_input_currency_code_is_invalid(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        contains an invalid currency code.
        """
        sample_currency_data = [
            [
                "1",
                "xyz",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_currency(sample_currency_data)


class Test_Format_Dim_Counterparty:
    """
    Test cases for the format_dim_counterparty function.
    """

    def test_format_dim_counterparty(self):
        """
        Test if the function formats counterparty data correctly to match the
        'dim_counterparty' table schema.
        """
        sample_counterparty_data = [
            [
                "1",
                "hello",
                "1",
                "commercial_contact",
                "delivery_contact",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        sample_address = [
            [
                "1",
                "123 apple street",
                "apple street",
                "bolton",
                "greater manchester",
                "ABC 123",
                "England",
                "123 456 789",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        formatted_counterparty_data = format_dim_counterparty(
            sample_counterparty_data, sample_address
        )

        expected_counterparty_data = [
            {
                "counterparty_id": "1",
                "counterparty_legal_name": "hello",
                "counterparty_legal_address_line_1": "123 apple street",
                "counterparty_legal_address_line2": "apple street",
                "counterparty_legal_district": "bolton",
                "counterparty_legal_city": "greater manchester",
                "counterparty_legal_postal_code": "ABC 123",
                "counterparty_legal_country": "England",
                "counterparty_legal_phone_number": "123 456 789",
            },
        ]

        assert formatted_counterparty_data == expected_counterparty_data

    def test_raises_exception_when_input_is_wrong_type(self):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong data types.
        """
        sample_counterparty_data = [
            [
                1,
                "hello",
                1,
                "commercial_contact",
                "delivery_contact",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        sample_address = [
            [
                "1",
                "123 apple street",
                "apple street",
                "bolton",
                "greater manchester",
                "ABC 123",
                "England",
                "123 456 789",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_counterparty(sample_counterparty_data, sample_address)

    def test_raises_exception_when_input_has_id_that_cannot_convert_to_integer(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        contains an ID that cannot be converted to an integer.
        """
        sample_counterparty_data = [
            [
                "1",
                "hello",
                "1",
                "commercial_contact",
                "delivery_contact",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        sample_address = [
            [
                "letters",
                "123 apple street",
                "apple street",
                "bolton",
                "greater manchester",
                "ABC 123",
                "England",
                "123 456 789",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_counterparty(sample_counterparty_data, sample_address)

    def test_raises_exception_when_input_is_wrong_length(
        self,
    ):
        """
        Test if the function raises an InputValidationError when input data
        has the wrong length.
        """
        sample_counterparty_data = [
            [
                "1",
                "hello",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        sample_address = [
            [
                "1",
                "123 apple street",
                "apple street",
                "bolton",
                "greater manchester",
                "ABC 123",
                "England",
                "123 456 789",
                "2023-07-25 15:20:49.962000",
                "2023-07-25 15:20:49.962000",
            ],
        ]

        with pytest.raises(InputValidationError):
            format_dim_counterparty(sample_counterparty_data, sample_address)
