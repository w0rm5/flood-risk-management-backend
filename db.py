import os
from pymongo import MongoClient
from bson.objectid import ObjectId

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("DB_NAME")
clusters = MongoClient(MONGO_URI)
db = clusters[DB_NAME]


def count_doc(col_name: str):
    col = db[col_name]
    return col.count_documents(filter={})


def find_one(col_name: str, query: dict = None, projection: dict = None):
    col = db[col_name]
    return col.find_one(filter=query, projection=projection)


def find_by_id(col_name: str, id: str | ObjectId, projection: dict = None):
    return find_one(col_name, {"_id": ObjectId(id)}, projection)


def find(col_name: str, query: dict = None, projection: dict = None):
    col = db[col_name]
    return col.find(filter=query, projection=projection)


def insert_one(col_name: str, doc: dict):
    col = db[col_name]
    return col.insert_one(doc)


def insert_many(col_name: str, docs: list):
    col = db[col_name]
    return col.insert_many(docs)


def update_one(col_name: str, query: dict, update: dict, upsert=False):
    col = db[col_name]
    return col.update_one(query, {"$set": update}, upsert=upsert)


def update_by_id(col_name: str, id: str | ObjectId, update: dict, upsert=False):
    return update_one(col_name, {"_id": ObjectId(id)}, {"$set": update}, upsert)


def update_many(col_name: str, query: dict, update: dict, upsert=False):
    col = db[col_name]
    return col.update_many(query, {"$set": update}, upsert=upsert)


def delete_one(col_name: str, query: dict):
    col = db[col_name]
    return col.delete_one(query)


def delete_by_id(col_name: str, id: str | ObjectId):
    return delete_one(col_name, {"_id": ObjectId(id)})


def delete_many(col_name: str, query: dict):
    col = db[col_name]
    col.up
    return col.delete_many(query)


