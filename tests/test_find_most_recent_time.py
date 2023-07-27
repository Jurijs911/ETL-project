from src.ingestion_lambda.utils.find_most_recent_time import find_most_recent_time
import datetime

test_data = [
    [1, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh', '28441',
     'Turkey', '1803 637401', datetime.datetime(
         1973, 2, 3, 14, 20, 49, 962000),
     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
    [2, '179 Alexie Cliffs', None, None, 'Aliso Viejo', '99305-7380',
     'San Marino', '9621 880720', datetime.datetime(
         2022, 10, 3, 14, 20, 49, 962000),
     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
    [3, '148 Sincere Fort', None, None, 'Lake Charles', '89360', 'Samoa',
     '0730 783349', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
     datetime.datetime(1990, 11, 3, 14, 20, 49, 962000)],
    [4, '6102 Rogahn Skyway', None, 'Bedfordshire', 'Olsonside', '47518',
     'Republic of Korea', '1239 706295', datetime.datetime(
         2022, 1, 6, 14, 20, 49, 962000),
     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
    [5, '34177 Upton Track', None, None, 'Fort Shadburgh', '55993-8850',
     'Bosnia and Herzegovina', '0081 009772', datetime.datetime(
         2022, 11, 3, 14, 20, 49, 962000),
     datetime.datetime(2023, 7, 26, 12, 00, 49, 962000)]]


def test_find_most_recent_time_should_return_valid_date_from_list():

    returned_time = find_most_recent_time(test_data)
    assert returned_time == '2023-07-26 12:00:49.962000'