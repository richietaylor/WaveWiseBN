import arrow
import requests
import csv
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
                'start': start_timestamp,
                'end': end_timestamp
            },
            headers={
                'Authorization': api_key
            },
            timeout=120  # Increased timeout for large data
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

def save_to_csv(data, params, output_file='historical_weather_8am.csv'):
    """
    Saves the processed 8 AM data to a CSV file.

    :param data: List of dictionaries containing processed 8 AM data.
    :param params: List of weather parameters.
    :param output_file: Filename for the output CSV.
    """
    if not data:
        print("No 8 AM data to save.")
        return

    # Define CSV headers
    headers = ['date'] + params

    with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()

        for entry in data:
            writer.writerow(entry)

    print(f"Data successfully saved to '{output_file}'.")

def main():
    # Read the API key from apiKey.txt
    try:
        API_KEY = read_api_key('apiKey2.txt')
    except Exception as e:
        print(f"Failed to read API key: {e}")
        return

    # Define the location coordinates (e.g., Cape Town)
    latitude = -33.9249
    longitude = 18.4241

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
        'windSpeed'
    ]

    # Ensure parameters are unique and sorted
    unique_params = sorted(set(desired_params))

    # Convert the list of parameters into a comma-separated string
    params_string = ','.join(unique_params)

    # Define the date range (e.g., last 3 years)
    years_of_data = 3
    end_date = arrow.utcnow().floor('day')  # Today's date in UTC
    start_date = end_date.shift(years=-years_of_data)  # 3 years ago

    print(f"Fetching data from {start_date.format('YYYY-MM-DD')} to {end_date.format('YYYY-MM-DD')}")

    # Convert start and end dates to UTC timestamps
    start_timestamp = start_date.replace(hour=0, minute=0, second=0).to('UTC').timestamp()
    end_timestamp = end_date.replace(hour=23, minute=59, second=59).to('UTC').timestamp()

    # Fetch the data
    print("Making API request...")
    data = fetch_weather_data(latitude, longitude, params_string, start_timestamp, end_timestamp, API_KEY)

    if data:
        print("Extracting 8 AM data points...")
        # Extract 8 AM data points
        eight_am_data = extract_8am_data(data)

        print("Processing data to extract 'sg' values...")
        # Extract 'sg' values
        processed_data = extract_sg_values(eight_am_data, unique_params)

        print("Saving data to CSV...")
        # Save to CSV
        save_to_csv(processed_data, unique_params, output_file='historical_weather_8am.csv')
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()
