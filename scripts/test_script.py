import mysql.connector
from pymongo import MongoClient
import redis

# 1. Connect to MySQL (The Source of Truth)
sql_db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mypassword",
    database="mpcs53001_final_project"
)
sql_cursor = sql_db.cursor()

# 2. Connect to MongoDB (Product Attributes/Specs)
# Handles diverse attributes for headphones, dresses, and vases
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["mpcs53001_final_project"]
product_collection = mongo_db["product_specs"]

# 3. Connect to Redis (Session & Recently Viewed)
# Ensures Sarah's journey is consistent across tablet and laptop
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

print("All database connections established successfully!")