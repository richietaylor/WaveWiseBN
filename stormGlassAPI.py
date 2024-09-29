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
