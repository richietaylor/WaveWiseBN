# import arrow
# import requests
# import json
# from pprint import pprint

# import arrow
# import requests
# import json
# from pprint import pprint

# # Define all desired parameters based on StormGlass API
# # Define all desired parameters as separate strings in a list
# desired_params = [
#     'waterTemperature',
#     'wavePeriod',
#     'waveDirection',
#     'waveHeight',
#     'windSpeed',
#     'windDirection', # N NE E SE...
#     'airTemperature',
#     'precipitation',
#     'cloudCover',
#     'humidity',
#     'pressure',
#     'visibility',
#     'currentSpeed',
#     'currentDirection',
#     'seaLevel',
#     # 'uvIndex',
# ]

# # Construct the parameters string
# params_string = ','.join(sorted(set(desired_params)))

# # Get the first hour of today in UTC
# start = arrow.now().floor('day').to('UTC').timestamp()

# # Get the last hour of today in UTC
# end = arrow.now().ceil('day').to('UTC').timestamp()


# response = requests.get(
#   'https://api.stormglass.io/v2/weather/point',
#   params={
#     'lat': -33.9249,
#     'lng': 18.4241,
#     'params': params_string,
#     'start': start,  # Convert to UTC timestamp
#     'end': end  # Convert to UTC timestamp
#   },
#   headers={
#     'Authorization': 
#   }
# )
# '''
# response = requests.get(
#   'https://api.stormglass.io/v2/tide/stations',
#   headers={
#     'Authorization': 
#   }
# )
# '''

# # Do something with response data.
# json_data = response.json()

# pprint(json_data)



import arrow
import requests
import json
from pprint import pprint
import csv
import time
import os

def read_api_key(file_path='apiKey.txt'):
    """
    Reads the API key from a specified file.

    :param file_path: Path to the API key file.
    :return: API key as a string.
    :raises FileNotFoundError: If the file does not exist.
    :raises ValueError: If the file is empty.
    """
    try:
        with open(file_path, 'r') as file:
            api_key = file.read().strip()
            if not api_key:
                raise ValueError("API key file is empty.")
            return api_key
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        raise
    except Exception as e:
        print(f"An error occurred while reading '{file_path}': {e}")
        raise

def fetch_weather_data_for_day(lat, lng, params, date, api_key):
    """
    Fetches weather data for a specific day at 8 AM UTC.

    :param lat: Latitude of the location.
    :param lng: Longitude of the location.
    :param params: Comma-separated string of weather parameters.
    :param date: Arrow object representing the date.
    :param api_key: StormGlass API key.
    :return: JSON data for the specified day at 8 AM or None if not found.
    """
    # Define the time range for 8 AM UTC
    start = date.replace(hour=8, minute=0, second=0).to('UTC').timestamp()
    end = date.replace(hour=9, minute=0, second=0).to('UTC').timestamp()  # 1-hour window

    try:
        response = requests.get(
            'https://api.stormglass.io/v2/weather/point',
            params={
                'lat': lat,
                'lng': lng,
                'params': params,
                'start': start,
                'end': end
            },
            headers={
                'Authorization': api_key
            },
            timeout=10  # Timeout after 10 seconds
        )

        if response.status_code == 200:
            data = response.json()
            hours = data.get('hours', [])
            if not hours:
                print(f"No data available for {date.format('YYYY-MM-DD')} at 8 AM UTC.")
                return None
            return hours[0]  # Assuming the first entry is 8 AM
        elif response.status_code == 429:
            # Rate limit exceeded; wait and retry
            print("Rate limit exceeded. Sleeping for 60 seconds.")
            time.sleep(60)
            return fetch_weather_data_for_day(lat, lng, params, date, api_key)
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def collect_historical_weather_data(lat, lng, params, start_date, end_date, api_key, output_file='historical_weather_8am.csv'):
    """
    Collects historical weather data for every day at 8 AM and saves it to a CSV file.

    :param lat: Latitude of the location.
    :param lng: Longitude of the location.
    :param params: Comma-separated string of weather parameters.
    :param start_date: Arrow object representing the start date.
    :param end_date: Arrow object representing the end date.
    :param api_key: StormGlass API key.
    :param output_file: Filename for the output CSV.
    """
    current_date = start_date

    # Open the CSV file for writing
    with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
        # Define the header based on the parameters
        fieldnames = ['date'] + sorted(set(params.split(',')))
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        total_days = (end_date - start_date).days + 1
        fetched_days = 0

        while current_date <= end_date:
            print(f"Fetching data for {current_date.format('YYYY-MM-DD')} at 8 AM UTC...")
            day_data = fetch_weather_data_for_day(lat, lng, params, current_date, api_key)

            if day_data:
                # Prepare the row for CSV
                row = {'date': day_data.get('time')}
                for param in fieldnames[1:]:
                    row[param] = day_data.get(param, None)
                writer.writerow(row)
                print(f"Data for {current_date.format('YYYY-MM-DD')} written to CSV.")
                fetched_days += 1
            else:
                print(f"No data for {current_date.format('YYYY-MM-DD')}.")

            # Move to the next day
            current_date = current_date.shift(days=1)

            # Optional: Sleep to respect rate limits
            time.sleep(1)  # Adjust based on your API plan's rate limits

    print(f"Data collection complete. {fetched_days}/{total_days} days fetched. Saved to '{output_file}'.")

def main():
    # Read the API key from apiKey.txt
    try:
        API_KEY = read_api_key('apiKey.txt')
    except Exception as e:
        print(f"Failed to read API key: {e}")
        return

    # Define the location coordinates (e.g., Cape Town)
    latitude = -33.9249
    longitude = 18.4241

    # Define the date range (e.g., last 3 years)
    years_of_data = 3
    end_date = arrow.utcnow().floor('day')  # Today's date in UTC
    start_date = end_date.shift(years=-years_of_data)  # 3 years ago

    print(f"Fetching data from {start_date.format('YYYY-MM-DD')} to {end_date.format('YYYY-MM-DD')}")

    # Define desired parameters
    desired_params = [
        'waterTemperature',
        'wavePeriod',
        'waveDirection',
        'waveHeight',
        'windSpeed',
        'windDirection',  # N, NE, E, SE, etc.
        'airTemperature',
        'precipitation',
        'cloudCover',
        'humidity',
        'pressure',
        'visibility',
        'currentSpeed',
        'currentDirection',
        'seaLevel',
        # 'uvIndex',  # Uncomment if available and needed
    ]

    # Ensure parameters are unique and sorted
    unique_params = sorted(set(desired_params))

    # Convert the list of parameters into a comma-separated string
    params_string = ','.join(unique_params)

    # Collect the historical weather data
    collect_historical_weather_data(
        lat=latitude,
        lng=longitude,
        params=params_string,
        start_date=start_date,
        end_date=end_date,
        api_key=API_KEY,
        output_file='historical_weather_8am.csv'
    )

if __name__ == "__main__":
    main()
