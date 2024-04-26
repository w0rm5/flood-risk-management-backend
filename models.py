from bson import ObjectId
from werkzeug.exceptions import Conflict, NotFound, BadRequest, Unauthorized
from auth import hashPassword, check_password, jwt_encode
import db
import re

EMAIL_REGEX = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b")


def obj_id_to_str(obj_id):
    if isinstance(obj_id, list):
        return list(map(obj_id_to_str, obj_id))
    if isinstance(obj_id, ObjectId):
        return obj_id.__str__()
    if isinstance(obj_id, dict) and "_id" in obj_id:
        return obj_id_to_str(obj_id["_id"])
    return obj_id


def str_to_obj_id(str_id):
    if str_id is None:
        return None
    if isinstance(str_id, list):
        return list(map(str_to_obj_id, str_id))
    return ObjectId(str_id)


class Response:
    def __init__(self, message="Success", code=200, data=None):
        self.message = message
        self.code = code
        self.data = data


class BaseClass:
    def __init__(self, obj):
        if "_id" in obj:
            self._id = obj_id_to_str(obj["_id"])

    def __getitem__(self, attr):
        return getattr(self, attr)

    def __setitem__(self, attr, new_value):
        setattr(self, attr, new_value)

    def __contains__(self, attr):
        return hasattr(self, attr)


class User(BaseClass):
    def __init__(self, user):
        super().__init__(user)
        print(user)
        if "email" not in user:
            raise BadRequest("Email is required")
        if "districts" in user:
            if not isinstance(user["districts"], list):
                raise BadRequest("Districts must be a list")
            self.districts = user["districts"]
        if "password" in user:
            self.password = user["password"]
        if "confirm_password" in user:
            self.confirm_password = user["confirm_password"]
        if "display_name" in user:
            self.display_name = user["display_name"]

        self.is_admin = False
        if "is_admin" in user:
            self.is_admin = user["is_admin"]
        self.email = user["email"]

    def register(self):
        if not EMAIL_REGEX.fullmatch(self.email):
            raise BadRequest("Please input a valid email address")
        if "display_name" not in self:
            raise BadRequest("A display name is required")
        if "password" not in self:
            raise BadRequest("Password is required")
        if "confirm_password" not in self:
            raise BadRequest("Confirm password is required")
        if "districts" not in self:
            raise BadRequest("Districts are required")
        if self.confirm_password != self.password:
            raise BadRequest("Passwords don't match")
        found = db.find_one("users", {"email": self.email})
        if found is not None:
            raise Conflict("Email is already in use")
        self.password = hashPassword(self.password)
        db.insert_one("users", self.__dict__)

    @classmethod
    def login(cls, user):
        if "email" not in user:
            raise BadRequest("Email is required")
        if "password" not in user:
            raise BadRequest("Password is required")
        email, password = user["email"], user["password"]
        found_user = db.find_one("users", {"email": email})
        if found_user == None:
            raise Unauthorized("User not exist")
        if not check_password(password, found_user["password"]):
            raise Unauthorized("Incorrect password")
        return jwt_encode({"email": email, "is_admin": found_user["is_admin"]})
