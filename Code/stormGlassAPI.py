import arrow
import requests
import csv
import os
from math import ceil
import time

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

def fetch_weather_data(lat, lng, params, start_timestamp, end_timestamp, api_key):
    """
    Fetches weather data for a specified time range.

    :param lat: Latitude of the location.
    :param lng: Longitude of the location.
    :param params: Comma-separated string of weather parameters.
    :param start_timestamp: Start of the time range (UTC timestamp).
    :param end_timestamp: End of the time range (UTC timestamp).
    :param api_key: StormGlass API key.
    :return: JSON data for the specified time range or None if an error occurs.
    """
    try:
        response = requests.get(
            'https://api.stormglass.io/v2/weather/point',
            params={
                'lat': lat,
                'lng': lng,
                'params': params,
                'start': int(start_timestamp),
                'end': int(end_timestamp)
            },
            headers={
                'Authorization': api_key
            },
            timeout=300  # Increased timeout for large data
        )

        if response.status_code == 200:
            data = response.json()
            return data
        elif response.status_code == 429:
            # Rate limit exceeded; wait and retry
            print("Rate limit exceeded. Sleeping for 60 seconds.")
            time.sleep(60)
            return fetch_weather_data(lat, lng, params, start_timestamp, end_timestamp, api_key)
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def extract_8am_data(data):
    """
    Extracts data points corresponding to 8 AM UTC.

    :param data: JSON data from the API.
    :return: List of dictionaries containing 8 AM data.
    """
    eight_am_data = []
    hours = data.get('hours', [])
    for hour_entry in hours:
        time_str = hour_entry.get('time')
        if not time_str:
            continue
        time_obj = arrow.get(time_str)
        if time_obj.hour == 8:
            eight_am_data.append(hour_entry)
    return eight_am_data

def extract_sg_values(data_list, params):
    """
    Extracts 'sg' values from each parameter's dictionary.

    :param data_list: List of dictionaries containing hourly data.
    :param params: List of weather parameters.
    :return: List of dictionaries with 'sg' values only.
    """
    processed_data = []
    for entry in data_list:
        processed_entry = {'date': entry.get('time')}
        for param in params:
            param_data = entry.get(param, {})
            if isinstance(param_data, dict):
                # Extract the 'sg' value if available
                processed_entry[param] = param_data.get('sg')
            else:
                # If the parameter is not a dict, assign it directly
                processed_entry[param] = param_data
        processed_data.append(processed_entry)
    return processed_data

def save_to_csv(data, params, output_file='historical_weather_8am.csv', mode='a'):
    """
    Saves the processed 8 AM data to a CSV file.

    :param data: List of dictionaries containing processed 8 AM data.
    :param params: List of weather parameters.
    :param output_file: Filename for the output CSV.
    :param mode: File mode ('w' for write, 'a' for append).
    """
    if not data:
        print("No 8 AM data to save.")
        return

    # Define CSV headers
    headers = ['date'] + params

    # Check if the file exists to write headers only once
    write_header = not os.path.isfile(output_file) or mode == 'w'

    with open(output_file, mode=mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        if write_header:
            writer.writeheader()

        for entry in data:
            writer.writerow(entry)

    print(f"Data successfully {'appended to' if mode == 'a' else 'saved to'} '{output_file}'.")

def verify_data_coverage(output_file, start_date, end_date):
    """
    Verifies that the CSV file covers the entire date range.

    :param output_file: Filename of the CSV file.
    :param start_date: Arrow object representing the start date.
    :param end_date: Arrow object representing the end date.
    """
    try:
        with open(output_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data_dates = set(entry['date'] for entry in reader)

        # Generate expected dates in ISO format
        expected_dates = set(
            (start_date.shift(days=i)).format('YYYY-MM-DDTHH:mm:ssZ') 
            for i in range((end_date - start_date).days + 1)
        )

        missing_dates = expected_dates - data_dates

        if not missing_dates:
            print("All dates are covered in the fetched data.")
        else:
            print(f"Missing dates: {len(missing_dates)}")
            # Optionally, list some missing dates
            sample_missing = list(missing_dates)[:10]  # Show first 10 missing dates
            print("Sample missing dates:", sample_missing)

    except FileNotFoundError:
        print(f"Error: The file '{output_file}' was not found.")
    except Exception as e:
        print(f"An error occurred while verifying data coverage: {e}")

def collect_historical_weather_data(lat, lng, params, start_date, end_date, api_key, output_file='historical_weather_8am.csv', max_requests_per_day=10):
    """
    Collects historical weather data for every day at 8 AM and saves it to a CSV file.

    :param lat: Latitude of the location.
    :param lng: Longitude of the location.
    :param params: Comma-separated string of weather parameters.
    :param start_date: Arrow object representing the start date.
    :param end_date: Arrow object representing the end date.
    :param api_key: StormGlass API key.
    :param output_file: Filename for the output CSV.
    :param max_requests_per_day: Maximum number of API requests allowed per day.
    """
    total_days = (end_date - start_date).days + 1
    days_per_request = ceil(total_days / max_requests_per_day)  # Calculate chunk size

    print(f"Total days to fetch: {total_days}")
    print(f"Days per API request: {days_per_request}")
    print(f"Total API requests needed: {ceil(total_days / days_per_request)}")

    # Prepare the CSV file by writing headers
    save_to_csv([], sorted(set(params.split(','))), output_file=output_file, mode='w')

    # Split the date range into chunks
    for i in range(max_requests_per_day):
        chunk_start_date = start_date.shift(days=i * days_per_request)
        chunk_end_date = chunk_start_date.shift(days=days_per_request - 1)
        if chunk_end_date > end_date:
            chunk_end_date = end_date

        # Define the start and end timestamps for the chunk
        chunk_start_timestamp = chunk_start_date.replace(hour=0, minute=0, second=0).to('UTC').timestamp()
        chunk_end_timestamp = chunk_end_date.replace(hour=23, minute=59, second=59).to('UTC').timestamp()

        print(f"\nAPI Request {i+1}/{max_requests_per_day}: Fetching data from {chunk_start_date.format('YYYY-MM-DD')} to {chunk_end_date.format('YYYY-MM-DD')}")

        # Fetch the data for the current chunk
        data = fetch_weather_data(lat, lng, params, chunk_start_timestamp, chunk_end_timestamp, api_key)

        if data and 'hours' in data:
            # Extract 8 AM data points
            eight_am_data = extract_8am_data(data)

            # Extract 'sg' values
            processed_data = extract_sg_values(eight_am_data, sorted(set(params.split(','))))

            # Write to CSV
            save_to_csv(processed_data, sorted(set(params.split(','))), output_file=output_file, mode='a')

            print(f"  - Retrieved {len(processed_data)} data points for this chunk.")
        else:
            print("  - No data returned for this chunk.")

        # Optional: Sleep to respect rate limits
        time.sleep(1)  # Adjust as needed based on API rate limits

    print(f"\nData collection complete. Saved to '{output_file}'.")

def main():
    # Read the API key from apiKey.txt
    try:
        API_KEY = read_api_key('apiKey2.txt')  # Ensure the correct file name
    except Exception as e:
        print(f"Failed to read API key: {e}")
        return

    # Define the location coordinates (e.g., Muizenberg)
    latitude = -34.0899
    longitude = 18.4959

    # Define the desired parameters
    desired_params = [
        'airTemperature',
        'cloudCover',
        'currentDirection',
        'currentSpeed',
        'humidity',
        'precipitation',
        'pressure',
        'seaLevel',
        'visibility',
        'waterTemperature',
        'waveDirection',
        'waveHeight',
        'wavePeriod',
        'windDirection',
        'windSpeed',
        'gust',
        'swellDirection',
        'swellHeight',
        'swellPeriod',
        'seaLevel'
    ]

    # Ensure parameters are unique and sorted
    unique_params = sorted(set(desired_params))

    # Convert the list of parameters into a comma-separated string
    params_string = ','.join(unique_params)

    # Define the date range (last 3 years)
    years_of_data = 3
    end_date = arrow.utcnow().floor('day')  # Today's date in UTC
    start_date = end_date.shift(years=-years_of_data)  # 3 years ago

    print(f"Fetching data from {start_date.format('YYYY-MM-DD')} to {end_date.format('YYYY-MM-DD')}")

    # Collect the historical weather data in multiple requests
    # Set your new daily request limit here
    max_requests_per_day = 200  # Update this value based on your new API limits

    collect_historical_weather_data(
        lat=latitude,
        lng=longitude,
        params=params_string,
        start_date=start_date,
        end_date=end_date,
        api_key=API_KEY,
        output_file='historical_weather_8am.csv',
        max_requests_per_day=max_requests_per_day
    )

    # Optionally, verify data coverage after collection
    # Load the CSV data for verification
    try:
        with open('historical_weather_8am.csv', 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            processed_data = [row for row in reader]
            verify_data_coverage('historical_weather_8am.csv', start_date, end_date)
    except Exception as e:
        print(f"Failed to verify data coverage: {e}")

if __name__ == "__main__":
    main()
