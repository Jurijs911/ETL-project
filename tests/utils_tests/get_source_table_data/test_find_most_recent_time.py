from src.ingestion_lambda.utils.find_most_recent_time \
    import find_most_recent_time
import datetime

test_data = [
    {'location_id': 28, 'address_line_1': '079 Horacio Landing',
     'address_line_2': None, 'district': None, 'city': 'Utica',
     'postal_code': '93045', 'country': 'Austria', 'phone': '7772 084705',
     'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
     'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)},
    {'location_id': 29, 'address_line_1': '37736 Heathcote Lock',
     'address_line_2': 'Noemy Pines', 'district': None, 'city': 'Bartellview',
     'postal_code': '42400-5199', 'country': 'Congo', 'phone': '1684 702261',
     'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
     'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)},
    {'location_id': 30, 'address_line_1': '0336 Ruthe Heights',
     'address_line_2': None, 'district': 'Buckinghamshire',
     'city': 'Lake Myrlfurt', 'postal_code': '94545-4284',
     'country': 'Falkland Islands (Malvinas)',
     'phone': '1083 286132', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
     'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)}]


def test_find_most_recent_time_should_return_valid_date_from_list():

    returned_time = find_most_recent_time(test_data)
    assert returned_time == '2022-11-03 14:20:49.962000'
