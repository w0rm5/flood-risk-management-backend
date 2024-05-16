from flask import Flask, request, g, jsonify, abort
from flask_cors import CORS

# from flask_caching import Cache
from dotenv import load_dotenv
from werkzeug.exceptions import HTTPException

from models import Response, User
import auth
import flood_prediction.seasonal_trend as st

import datetime

# import math
from datetime import datetime
from utilities import validateCSV

load_dotenv()

config = {"DEBUG": True, "CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}

app = Flask(__name__)
# app.config.from_mapping(config)
# cache = Cache(app)

CORS(app)

ACCESS_TOKEN_HEADER = "token"
PUBLIC_ROUTES = ["/", "/user/login", "/user/register", "/data/flood-info"]


def get_response(data=None, code=200, message="Success"):
    return jsonify(Response(message, code, data).__dict__), code


def validate_date(date_text):
    try:
        if date_text != datetime.strptime(date_text, "%Y-%m-%d").strftime("%Y-%m-%d"):
            raise ValueError
        return True
    except ValueError:
        return False


def check_if_admin():
    if g.user_data["is_admin"] == False:
        abort(401, "Unauthorized")


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
    return get_response(st.get_forecasted_data(station_ids, date))


@app.post("/file/upload_csv")
def uploadFile():
    check_if_admin()
    if "file" not in request.files:
        return get_response(None, 400, "No file uploaded")
    file = request.files["file"]
    code, message = validateCSV(file)
    return get_response(None, code, message)
