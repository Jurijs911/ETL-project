import datetime

def find_most_recent_time(data):
    """
    arguments:

        data: A list of dictionaries


    returns:

        most_recent_date: a datetime of the most recent date
    """
    
    input_rows = data
    most_recent_date = datetime.datetime(1000, 1, 1, 1, 00, 0, 0)

    for row in input_rows:
        for key in row:
            if isinstance(key, datetime.date):
                if key > most_recent_date:
                    most_recent_date = key

    formatted_return_date = datetime.datetime.strftime(
        most_recent_date, '%Y-%m-%d %H:%M:%S.%f')

    return formatted_return_date