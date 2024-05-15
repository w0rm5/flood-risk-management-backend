from flask import Flask, request, g, jsonify
from flask_cors import CORS

# from flask_caching import Cache
from dotenv import load_dotenv
from werkzeug.exceptions import HTTPException

from models import Response, User
import auth
import flood_prediction.seasonal_trend as st
import datetime
import math
from datetime import datetime
# from flood_prediction.constants import WEATHER_STATIONS

load_dotenv()

config = {"DEBUG": True, "CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}

app = Flask(__name__)
# app.config.from_mapping(config)
# cache = Cache(app)

CORS(app)

ACCESS_TOKEN_HEADER = "token"
PUBLIC_ROUTES = ["/", "/user/login", "/user/register", "/data/flood-info"]


# @cache.cached(timeout=0, key_prefix="all_stations_data")
# def get_all_stations_data():
#     return st.get_all_stations_data()


# all_stations_data = get_all_stations_data()


def get_response(data=None, code=200, message="Success"):
    return jsonify(Response(message, code, data).__dict__), code


# def time_until_end_of_day():
#     dt = datetime.datetime.now()
#     tomorrow = dt + datetime.timedelta(days=1)
#     time_until_end_of_day = datetime.datetime.combine(tomorrow, datetime.time.min) - dt
#     return math.ceil(time_until_end_of_day.total_seconds())


def validate_date(date_text):
    try:
        if date_text != datetime.strptime(date_text, "%Y-%m-%d").strftime("%Y-%m-%d"):
            raise ValueError
        return True
    except ValueError:
        return False


@app.errorhandler(Exception)
def handle_error(e):
    code = 500
    message = e.__str__()
    if isinstance(e, HTTPException):
        code = e.code
        message = e.description
    return jsonify(Response(message=message, code=code).__dict__), code


@app.before_request
def check_token():
    if request.method == "OPTIONS" or request.path in PUBLIC_ROUTES:
        pass
    elif not ACCESS_TOKEN_HEADER in request.headers:
        raise get_response(None, 403, "Missing token")
    else:
        try:
            user_data = auth.jwt_decode(request.headers[ACCESS_TOKEN_HEADER])
            g.user_data = user_data
        except:
            raise get_response(None, 403, "Invalid token")


@app.get("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.post("/user/register")
def user_register():
    user = User(request.get_json())
    user.register()
    return get_response("user registered")


@app.post("/user/login")
def user_login():
    token = User.login(request.get_json())
    return get_response(token)


@app.get("/user/info")
def get_user_info():
    res = User.get_info(g.user_data)
    return get_response(res)


# @app.get("/data/flood-info")
# # @cache.cached(timeout=time_until_end_of_day(), query_string=True) # cache for the whole day
# def get_flood_info():
#     station_id = request.args.get("station_id")
#     if station_id is None or station_id == "":
#         return get_response(None, 400, "Please specified a station")
#     station_id = int(station_id)
#     station_data = all_stations_data[station_id]
#     date = request.args.get("date")
#     if date is None or date == "" or date not in station_data:
#         return get_response(station_data)
#     res = station_data[date]
#     return get_response(res)


@app.post("/data/flood-info")
def get_data():
    query = request.get_json()
    if "station_ids" not in query:
        return get_response(None, 400, "Please specified station ids")
    station_ids = query["station_ids"]
    if not isinstance(station_ids, list):
        return get_response(None, 400, "station_ids must be an array")
    if len(station_ids) == 0:
        return get_response(None, 400, "Please specified station ids")
    if "date" not in query:
        return get_response(None, 400, "Please specified date")
    date = query["date"]
    if not validate_date(date):
        return get_response(None, 400, "Date must be in YYYY-MM-DD format")
    # res = {}
    # for id in station_ids:
    #     if not isinstance(id, int):
    #         return get_response(None, 400, "All station ids must be integers")
    #     if id not in WEATHER_STATIONS:
    #         return get_response(None, 400, f"{id} is not a station id")
    #     station_data = all_stations_data[id]
    #     res[id] = station_data[date]
    # return get_response(res)
    return get_response(st.get_forecasted_data(station_ids, date))
