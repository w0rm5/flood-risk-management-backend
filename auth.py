import os
import jwt
import bcrypt

JWT_SECRET = os.environ.get("JWT_SECRET")

def hashPassword(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode(), salt)
    return hashed.decode()

def check_password(password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed_password.encode())

def jwt_encode(obj: dict) -> str:
    return jwt.encode(obj, JWT_SECRET, algorithm="HS256")

def jwt_decode(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])