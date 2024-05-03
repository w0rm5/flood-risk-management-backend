from flask import Flask, request, g, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from werkzeug.exceptions import HTTPException, Forbidden, BadRequest

from models import Response, User
import auth

load_dotenv()


def get_ok_response(data=None):
    return jsonify(Response(data=data).__dict__)


app = Flask(__name__)

CORS(app)

ACCESS_TOKEN_HEADER = "token"
PUBLIC_ROUTES = ["/", "/user/login", "/user/register"]


@app.errorhandler(Exception)
def handle_error(e):
    code = 500
    if isinstance(e, HTTPException):
        code = e.code
    return jsonify(Response(message=e.description, code=code).__dict__), code


@app.before_request
def check_token():
    if request.method == "OPTIONS" or request.path in PUBLIC_ROUTES:
        pass
    elif not ACCESS_TOKEN_HEADER in request.headers:
        raise Forbidden("Missing token")
    else:
        try:
            user_data = auth.jwt_decode(request.headers[ACCESS_TOKEN_HEADER])
            g.user_data = user_data
        except:
            raise Forbidden("Invalid token")


@app.get("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.post("/user/register")
def user_register():
    user = User(request.get_json())
    user.register()
    return get_ok_response("user registered")


@app.post("/user/login")
def user_login():
    token = User.login(request.get_json())
    return get_ok_response(token)


@app.get("/user/info")
def get_user_info():
    res = User.get_info(g.user_data)
    return get_ok_response(res)


@app.get("/data/flood-info")
def get_flood_info():
    return get_ok_response()
