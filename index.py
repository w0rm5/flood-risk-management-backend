from flask import (Flask, request, jsonify)
from flask_cors import CORS
from dotenv import load_dotenv
from werkzeug.exceptions import HTTPException

from models import (Response, User)

load_dotenv()

def get_ok_response(data=None):
    return jsonify(Response(data=data).__dict__)

app = Flask(__name__)

CORS(app)

@app.errorhandler(Exception)
def handle_error(e):
    code = 500
    if isinstance(e, HTTPException):
        code = e.code
    return jsonify(Response(message=e.description, code=code).__dict__), code

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