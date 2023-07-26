from datetime import datetime

test_data = datetime(2023, 7, 25, 15, 20, 49, 962000)

formatted_date = test_data.strftime("%Y-%m-%d")
formatted_time = test_data.strftime("%H:%M:%S:%f")




print(formatted_date)
print(formatted_time)



