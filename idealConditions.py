from urllib import request
from datetime import datetime, timedelta
import json


def lambda_handler(event, context):
    """Main program that calls run_prog and either returns OK status with
       results or error status with the error type in the return body"""

    return_dict = {
        "error": 0, "status": "start"
    }

    return_dict = run_prog(event, return_dict)

    if return_dict["error"] == 0:
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(return_dict),
            "isBase64Encoded": False
        }
    else:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(return_dict),
            "isBase64Encoded": False
        }



def run_prog(event, return_dict):
    """Runs individual functions checking for errors.  Gets location from
    the event object, translates into gps coordinates, translates into NOAA
    grid points, fetches weather forecast, parses weather forcast, checks for
    ideal conditions for activities and returns success or failure."""

    # PARSE PARAMETERS
    place, start_date, end_date, weekends = get_place(event)

    if not place or place == "Unknown":
        return_dict["status"] = "Error: Place error."
        return_dict["error"] = 1
        return return_dict

    if not start_date:
        return_dict["status"] = "Error: Start date error."
        return_dict["error"] = 1
        return return_dict

    if not end_date:
        return_dict["status"] = "Error: End date error."
        return_dict["error"] = 1
        return return_dict

    # FETCH AND PARSE WEATHER FORECAST
    return_dict["status"] = "coords"
    location = get_coords(place)
    if not location:
        return_dict["status"] = "Error: Location not found."
        return_dict["error"] = 1
        return return_dict

    x_coord = location[0]['lat']
    y_coord = location[0]['lon']

    return_dict["status"] = "points"
    points_json = get_grid_points(x_coord, y_coord)
    if not points_json:
        return_dict["status"] = "Error: Multiple locations found. Please be more specific."
        return_dict["error"] = 1
        return return_dict

    hourly_api_url = points_json["properties"]["forecastHourly"]
    city = points_json["properties"]["relativeLocation"]["properties"]["city"]
    state = points_json["properties"]["relativeLocation"]["properties"]["state"]

    return_dict["status"] = "forecast"
    forecast_json = get_weather(hourly_api_url)
    if not forecast_json:
        return_dict["status"] = "Error: Could not get weather forecast."
        return_dict["error"] = 1
        return return_dict

    return_dict["status"] = "parse"
    forecast_dict = parse_weather(forecast_json)
    if not forecast_dict:
        return_dict["status"] = "Error: Could not parse weather forecast."
        return_dict["error"] = 1
        return return_dict

    # GENERATE IDEAL CONDITIONS RETURN
    return_dict["status"] = "activities"
    activities_dict = generate_activities(forecast_dict, start_date, end_date, weekends)
    if not activities_dict:
        return_dict["status"] = "Error: Activity fetching error."
        return_dict["error"] = 1
        return return_dict
    else:
        return_dict["information"] = {"location": place, "city": city, "state": state, "latitude": x_coord,
                                      "longitude": y_coord}
        return_dict["results"] = activities_dict
        return_dict["status"] = "complete"
        return return_dict


def get_place(event):
    """Extracts request parameters from the lambda proxy integration"""

    try:
        place = event["queryStringParameters"]["location"]
        if not place:
            place = "Unknown"
    except (TypeError, KeyError):
        place = "Unknown"

    try:
        start_date = event["queryStringParameters"]["startDate"]
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start_date = datetime.today().date()
    except (TypeError, KeyError):
        start_date = datetime.today().date()

    try:
        end_date = event["queryStringParameters"]["endDate"]
        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_date = datetime.today().date() + timedelta(days=10)
    except (TypeError, KeyError):
        end_date = datetime.today().date() + timedelta(days=10)

    try:
        weekends = event["queryStringParameters"]["weekends"]
        if weekends:
            weekends = bool(weekends)
        else:
            weekends = False
    except (TypeError, KeyError):
        weekends = False

    return place, start_date, end_date, weekends


def get_coords(place):
    """Converts a text location (city, state) into GPS coordinates"""
    try:
        with request.urlopen("https://geocode.maps.co/search?q=" + place + "&country=US") as location_response:
            location_content = location_response.read()
            location_content.decode('utf-8')
            location_json = json.loads(location_content)
        return location_json
    except (Exception):
        return None


def get_grid_points(x_coord, y_coord):
    """Converts GPS coordinates into NOAA grid points"""
    try:
        with request.urlopen("https://api.weather.gov/points/" + str(x_coord) + "," + str(y_coord)) as points_response:
            response_content = points_response.read()
            response_content.decode('utf-8')
            points_json = json.loads(response_content)
        return points_json
    except (Exception):
        return None


def get_weather(hourly_api_url):
    """Gets hourly weather data from the NOAA grid point"""
    try:
        with request.urlopen(hourly_api_url) as weather_response:
            response_content = weather_response.read()
            response_content.decode('utf-8')
            forecast_json = json.loads(response_content)
        return forecast_json
    except (Exception):
        return None


def parse_weather(forecast_json):
    """Parses the weather forecast into a simplified output format"""
    try:
        forcast_hours = forecast_json["properties"]["periods"]
        forecast_dict = {}
        for count, value in enumerate(forcast_hours):
            # parse wind
            wind = value["windSpeed"].split(" ")
            wind = int(wind[0])
            # parse weather
            short_weather = ""
            weather = value["shortForecast"]
            if "Thunderstorms" in weather:
                short_weather = "Thunderstorms"
            elif "Rain" in weather or "Showers" in weather:
                short_weather = "Rainy"
            elif "Snow" in weather:
                short_weather = "Snow"
            elif "Ice" in weather or "Frost" in weather:
                short_weather = "Icy"
            elif "Cloudy" in weather:
                short_weather = "Cloudy"
            elif "Sunny" in weather:
                short_weather = "Sunny"
            elif "Clear" in weather:
                short_weather = "Clear"
            elif "Fog" in weather:
                short_weather = "Foggy"
            else:
                short_weather = "Other"
            # parse time
            dt = datetime.strptime(value["startTime"][0:16], "%Y-%m-%dT%H:%M")
            day = dt.weekday()
            date = dt.date()
            time = dt.time()
            forecast_dict[count] = {"day": day,
                                    "date": date.strftime("%Y-%m-%d"),
                                    "time": time.strftime("%H"),
                                    "daytime": value["isDaytime"],
                                    "temperature": value["temperature"],
                                    "wind_speed": wind,
                                    "humidity": value["relativeHumidity"]["value"],
                                    "weather": short_weather,
                                    }
        return forecast_dict
    except (Exception):
        return None


def generate_activities(forecast, start_date, end_date, weekends):
    """Defines a class structure to store and compare ideal weather conditions
    for various activities."""

    class Activity:
        def __init__(self, name, day, min_temp, max_temp, min_wind, max_wind, weather):
            self.name = name
            self.daytime_only = day
            self.min_temp = min_temp
            self.max_temp = max_temp
            self.min_wind = min_wind
            self.max_wind = max_wind
            self.weather = weather

        def get_windows(self, forecast, start_date, end_date, weekends_only):
            return_dict = {}
            records = 0
            for count in range(155):
                hour = forecast[count]
                date = datetime.strptime(hour['date'], "%Y-%m-%d").date()
                temp = hour['temperature']
                wind = hour['wind_speed']
                weather = hour['weather']
                is_daytime = True
                if self.daytime_only:
                    is_daytime = hour["daytime"]
                is_weekend = True
                if weekends_only:
                    is_weekend = hour["day"] == 5 or hour["day"] == 6
                if (
                        (date >= start_date) and  # equal or after start date
                        (date <= end_date) and  # equal or after end date
                        is_weekend and is_daytime and
                        (temp >= self.min_temp) and  # equal or above min temp
                        (temp <= self.max_temp) and  # equal or below max temp
                        (wind >= self.min_wind) and  # equal or above min wind speed
                        (wind <= self.max_wind) and  # equal or below max wind speed
                        (weather in self.weather)):  # weather conditions in allowed list
                    return_dict[records] = hour
                    records += 1

            return return_dict

    # instantiate classes with ideal weather data
    running = Activity("Running", True, 50, 90, 0, 15, ["Rainy", "Cloudy", "Sunny", "Clear"])
    fishing = Activity("Fishing", True, 50, 100, 0, 15, ["Cloudy", "Sunny", "Clear"])
    hiking = Activity("Hiking", True, 40, 85, 0, 15, ["Rainy", "Cloudy", "Sunny", "Clear"])
    cycling = Activity("Cycling", True, 50, 85, 0, 15, ["Cloudy", "Sunny", "Clear"])
    camping = Activity("Camping", False, 50, 90, 0, 15, ["Cloudy", "Sunny", "Clear"])
    hunting = Activity("Hunting", False, 40, 85, 0, 15, ["Rainy", "Cloudy", "Sunny", "Clear"])
    skiing = Activity("Skiing", False, 15, 40, 0, 10, ["Snow", "Cloudy", "Sunny", "Clear", "Icy"])
    water_sport = Activity("Water Sports", True, 65, 110, 0, 8, ["Cloudy", "Sunny", "Clear"])
    mountaineering = Activity("Mountaineering", False, 0, 40, 0, 10, ["Snow", "Cloudy", "Sunny", "Clear", "Icy"])

    # compare forecast data and generate ideal windows
    running_dict = running.get_windows(forecast, start_date, end_date, weekends)
    fishing_dict = fishing.get_windows(forecast, start_date, end_date, weekends)
    hiking_dict = hiking.get_windows(forecast, start_date, end_date, weekends)
    cycling_dict = cycling.get_windows(forecast, start_date, end_date, weekends)
    camping_dict = camping.get_windows(forecast, start_date, end_date, weekends)
    hunting_dict = hunting.get_windows(forecast, start_date, end_date, weekends)
    skiing_dict = skiing.get_windows(forecast, start_date, end_date, weekends)
    kayaking_dict = water_sport.get_windows(forecast, start_date, end_date, weekends)
    mountaineering_dict = mountaineering.get_windows(forecast, start_date, end_date, weekends)

    activities_dict = {"Running": running_dict, "Fishing": fishing_dict, "Hiking": hiking_dict, "Cycling": cycling_dict,
                       "Camping": camping_dict, "Hunting": hunting_dict, "Skiing": skiing_dict,
                       "Kayaking": kayaking_dict, "Mountaineering": mountaineering_dict}

    return activities_dict
