import pytest
from unittest.mock import patch, Mock
from src.loading.loading_utils import (
    insert_into_dim_design,
    insert_into_dim_currency,
    insert_into_dim_staff,
    insert_into_dim_location,
    insert_into_dim_date,
    insert_into_dim_counterparty,
    insert_into_dim_fact_sales_order,
    InputValidationError,
)


class Test_Dim_Design:
    def test_read_inserted_data(self):
        """Insert the given design data into the dimension table for designs.
        Raises:
            InputValidationError: If the design_data is not in the
            expected format or contains invalid values.
        """

        mock_connection = Mock()
        with patch(
            "src.loading.loading_utils.create_connection",
            return_value=mock_connection,
        ):
            design_data = [
                [5, "Design 5", "location5", "file5"],
                [6, "Design 6", "location6", "file6"],
            ]
            mock_connection.run.side_effect = (
                lambda query, **params: design_data
            )

            result = insert_into_dim_design(mock_connection, design_data)
            assert result == design_data

    def test_insert_invalid_input(self):
        mock_connection = Mock()
        invalid_design_data = [
            [1, "Invalid Design 1", 123, "Invalid File"],
            [2, "Invalid Design 2", "location2", 456],
        ]

        with pytest.raises(InputValidationError):
            insert_into_dim_design(mock_connection, invalid_design_data)

    def test_insert_missing_columns(self):
        mock_connection = Mock()
        invalid_design_data = [
            [1, "Invalid Design 1"],
            [2, "Invalid Design 2", "location2", "file2", "Extra Column"],
        ]

        with pytest.raises(InputValidationError):
            insert_into_dim_design(mock_connection, invalid_design_data)


class Test_Dim_Currency:
    def test_read_inserted_data(self):
        """Insert the given currency data into the
        dimension table for currencies.

        Parameters:
            connection (object): The database connection object.
            currency_data (list): A list of lists containing currency data.
                                Each inner list should have
                                the following format:
                                [currency_id (int), currency_code (str),
                                currency_name (str)]

        Returns:
            list: The inserted currency data as a list of lists,
                with each inner list containing the inserted row's values.
                If successful, this will be the same as the
                input currency_data.

        Raises:
            InputValidationError: If the currency_data is not
                                    in the expected format or
                                    contains invalid values.
        """

        mock_connection = Mock()
        with patch(
            "src.loading.loading_utils.create_connection",
            return_value=mock_connection,
        ):
            currency_data = [
                [1, "USD", "US Dollar"],
                [2, "GBP", "GB Pound"],
            ]
            mock_connection.run.return_value = currency_data

            result = insert_into_dim_currency(mock_connection, currency_data)
            assert result == currency_data

    def test_insert_invalid_input(self):
        mock_connection = Mock()
        invalid_currency_data = [
            [1, 123, "Invalid Currency Name"],
            [2, "GBP", 456],
        ]

        with pytest.raises(InputValidationError):
            insert_into_dim_currency(mock_connection, invalid_currency_data)

    def test_insert_missing_columns(self):
        mock_connection = Mock()
        invalid_currency_data = [
            [1, "USD"],
            [2],
        ]

        with pytest.raises(InputValidationError):
            insert_into_dim_currency(mock_connection, invalid_currency_data)


def test_read_inserted_dim_staff_data():
    """Insert the given staff data into the dimension table for staff members.
    Raises:
        InputValidationError: If the staff_data is not
            in the expected format or contains invalid values.
    """

    mock_connection = Mock()
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        staff_data = [
            [1, "Zenab", "Haider", "Sales", "Manchester", "zenab.email.com"],
            [2, "Lisa", "S", "Marketing", "London", "lisa@email.com"],
        ]
        mock_connection.run.return_value = staff_data

        with pytest.raises(InputValidationError):
            insert_into_dim_staff(mock_connection, staff_data)


def test_insert_into_dim_staff_invalid_input():
    mock_connection = Mock()
    invalid_staff_data = [
        [
            1,
            "Invalid",
            "Staff1",
            123,
            "Invalid Location",
            "email1@example.com",
        ],
        [
            2,
            "Staff2",
            456,
            "Invalid Department",
            "London",
            "email2@example.com",
        ],
    ]

    with pytest.raises(InputValidationError):
        insert_into_dim_staff(mock_connection, invalid_staff_data)


def test_insert_into_dim_staff_missing_columns():
    mock_connection = Mock()
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        staff_data_with_missing_columns = [
            # Missing last two columns
            [1, "Lisa", "Sco", "Sales", "London", "lisa@example.com"],
            # Extra column
            [
                2,
                "Zenab",
                "Haider",
                "Marketing",
                "New York",
                "zen@example.com",
                "987654321",
                "Extra column",
            ],
            # Too many columns
            [
                3,
                "Cam",
                "P",
                "GOD",
                "Chicago",
                "Cam@example.com",
                "123456789",
                "Column 1",
                "Column 2",
            ],
        ]
        with pytest.raises(InputValidationError):
            insert_into_dim_staff(
                mock_connection, staff_data_with_missing_columns
            )


def test_insert_into_dim_location():
    """Insert the given location data into the dimension table for locations.
    Raises:
        InputValidationError: If the location_data is not in the
                            expected format or contains invalid values.
    """

    mock_connection = Mock()
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        location_data = [
            [
                1,
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
            ],
            [
                2,
                "123 apple street",
                "address_line_2",
                "apple",
                "applecity",
                "ABC-123",
                "England",
                "123-456-789",
            ],
        ]
        mock_connection.run.return_value = location_data

        result = insert_into_dim_location(mock_connection, location_data)
        assert result == location_data


def test_insert_into_dim_location_invalid_input():
    mock_connection = Mock()
    invalid_location_data = [
        [
            1,
            "Address 1",
            123,
            "Invalid District",
            "Cityville",
            "12345",
            "Countryland",
            "555-123-4567",
        ],
        [
            2,
            "123 Main St",
            "Apt 4",
            "Central District",
            "Townsville",
            "67890",
            "Countryland",
            987,
        ],
    ]

    with pytest.raises(InputValidationError):
        insert_into_dim_location(mock_connection, invalid_location_data)


def test_insert_into_dim_location_missing_columns():
    mock_connection = Mock()
    invalid_location_data = [
        [
            1,
            "Address 1",
            "District",
            "Cityville",
            "12345",
            "Countryland",
            "555-123-4567",
        ],
        [
            2,
            "123 Main St",
            "Apt 4",
            "Central District",
            "Townsville",
            "67890",
            "Countryland",
            "555-987-6543",
            "Extra Column",
        ],
    ]

    with pytest.raises(InputValidationError):
        insert_into_dim_location(mock_connection, invalid_location_data)


def test_insert_into_dim_date_invalid_input():
    """Insert the given date data into the dimension table for dates.

    Raises:
        InputValidationError: If the date_data is not
                    in the expected format or contains invalid values.
    """

    mock_connection = Mock()
    # Return an empty list to mock no data in the table
    mock_connection.run.return_value = []
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        date_data_with_invalid_input = [
            ["2023-07-27", 2023, 7, 27, False, "Thursday", "July", 2]
        ]
        with pytest.raises(InputValidationError):
            insert_into_dim_date(mock_connection, date_data_with_invalid_input)


def test_insert_into_dim_date_invalid_date_format():
    mock_connection = Mock()
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        date_data = [["2023-07-27", False, 7, 27, 4, "Thursday", "July", 2]]

        with pytest.raises(InputValidationError):
            insert_into_dim_date(mock_connection, date_data)


def test_insert_into_dim_counterparty():
    """Insert the given counterparty data into
        the dimension table for counterparties.
    Raises:
        InputValidationError: If the counterparty_data is not in
        the expected format or contains invalid values.
    """

    mock_connection = Mock()
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        counterparty_data = [
            [
                1,
                "counterparty_legal_name",
                "counterparty_legal_address_line_1",
                "counterparty_legal_address_line_2",
                "counterparty_legal_district",
                "counterparty_legal_city",
                "counterparty_legal_postal_code",
                "counterparty_legal_country",
                "counterparty_legal_phone_number",
            ]
        ]
        mock_connection.run.return_value = counterparty_data

        result = insert_into_dim_counterparty(
            mock_connection, counterparty_data
        )
        assert result == counterparty_data


def test_insert_into_dim_counterparty_invalid_input():
    mock_connection = Mock()
    # Return an empty list to mirror no data in the table
    mock_connection.run.return_value = []
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        counterparty_data = [
            [
                1,
                "Counterparty 1",
                "Address 1",
                "Address 2",
                "District 1",
                "City 1",
                "12345",
                "Country 1",
                12345,
            ]
        ]

        with pytest.raises(InputValidationError):
            insert_into_dim_counterparty(mock_connection, counterparty_data)


def test_insert_into_dim_counterparty_invalid_currency_id():
    mock_connection = Mock()
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        counterparty_data = [
            [
                1,
                "Counterparty 1",
                "Address 1",
                "Address 2",
                "District 1",
                "City 1",
                "12345",
                "Country 1",
                "123456789",
            ],
            [
                "Invalid ID",
                "Invalid Name",
                "Invalid Address",
                "Invalid District",
                "Invalid City",
                "12345",
                "Invalid Country",
                "Invalid Phone",
            ],
        ]
        with pytest.raises(InputValidationError):
            insert_into_dim_counterparty(mock_connection, counterparty_data)


def test_insert_into_dim_fact_sales_order():
    """Insert the given sales order data into the fact table for sales orders.
    Raises:
        InputValidationError: If the sales_data is not in the expected format
        or contains invalid values.
    """

    mock_connection = Mock()
    # Return an empty list to show no data in the table
    mock_connection.run.return_value = []
    with patch(
        "src.loading.loading_utils.create_connection",
        return_value=mock_connection,
    ):
        sales_data = [
            [
                1,
                1,
                "2023-07-27",
                "15:20:49.962000",
                "2023-07-27",
                "15:20:49.962000",
                10,
                12,
                115,
                20.20,
                15,
                16,
                "2023-07-30",
                "2023-08-05",
                18,
            ]
        ]
        with pytest.raises(InputValidationError):
            insert_into_dim_fact_sales_order(mock_connection, sales_data)


def test_insert_into_dim_fact_sales_order_invalid_input():
    mock_connection = Mock()
    invalid_fact_sales_order_data = [
        [
            1,
            1001,
            "2023-07-28",
            "08:30:00.000000",
            "2023-07-28",
            "10:15:00.000000",
            "Invalid Sales Staff",
            501,
            10,
            50.0,
            401,
            201,
            "2023-08-15",
            "2023-08-20",
            302,
        ],
        [
            2,
            "Invalid Order ID",
            "2023-07-28",
            "09:45:00.000000",
            "2023-07-28",
            "11:30:00.000000",
            102,
            502,
            5,
            "Invalid Unit Price",
            402,
            202,
            "2023-08-16",
            "2023-08-21",
            301,
        ],
    ]

    with pytest.raises(InputValidationError):
        insert_into_dim_fact_sales_order(
            mock_connection, invalid_fact_sales_order_data
        )


def test_insert_into_dim_fact_sales_order_invalid_date_format():
    mock_connection = Mock()
    invalid_fact_sales_order_data = [
        [
            1,
            1001,
            "2023-07-28",
            "08:30:00.000000",
            "2023-07-28",
            "10:15:00.000000",
            "Invalid Sales Staff",
            501,
            10,
            50.0,
            401,
            201,
            "2023-08-15",
            "2023-08-20",
            302,
        ],
        [
            2,
            "Invalid Order ID",
            "28-07-2023",
            "09:45:00.000000",
            "2023-07-28",
            "11:30:00.000000",
            102,
            502,
            5,
            "Invalid Unit Price",
            402,
            202,
            "2023-08-16",
            "2023-08-21",
            301,
        ],
    ]

    with pytest.raises(InputValidationError):
        insert_into_dim_fact_sales_order(
            mock_connection, invalid_fact_sales_order_data
        )
