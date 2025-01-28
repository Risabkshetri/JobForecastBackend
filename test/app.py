from pymongo import MongoClient
import pymongo
import os
from dotenv import load_dotenv
import certifi  

load_dotenv()

uri = os.environ["MONGO_URI"]


try:
    # Add tlsCAFile parameter with certifi.where()
    client = MongoClient(
        uri,
    server_api=pymongo.server_api.ServerApi(version="1"),
    tlsCAFile=certifi.where(),
    connectTimeoutMS=30000,
    socketTimeoutMS=None,
    connect=True,
    maxPoolSize=1
    )
    
    # Test the connection
    client.admin.command({'ping': 1})
    print("Connected successfully to MongoDB!")

except pymongo.errors.ConnectionFailure as e:
    print(f"Could not connect to MongoDB: {e}")
except pymongo.errors.ServerSelectionTimeoutError as e:
    print(f"Server selection timeout: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    client.close()