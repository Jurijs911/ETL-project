from src.csv_utils import add_csv, update_csv
import csv


class Test_add_csv:
    def test_creates_csv_file_with_correct_data(self):
        test_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-06-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2022-12-12"},
        ]

        add_csv(test_data, "currency")

        with open("add.csv", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                assert ["currency_id", "currency_code", "created_at", "last_updated"] == list(row.keys())
