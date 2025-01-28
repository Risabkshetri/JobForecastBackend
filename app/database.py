from pymongo import MongoClient
from pymongo.database import Database
import certifi
from app.config import settings
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class MongoManager:
    _instance: Optional['MongoManager'] = None
    _client: Optional[MongoClient] = None
    _db: Optional[Database] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoManager, cls).__new__(cls)
        return cls._instance

    def initialize(self):
        if not self._client:
            try:
                self._client = MongoClient(
                    settings.MONGO_URI,
                    tlsCAFile=certifi.where(),
                    connectTimeoutMS=30000,
                    socketTimeoutMS=None,
                    connect=True,
                    maxPoolSize=1
                )
                # Test connection
                self._client.admin.command({'ping': 1})
                self._db = self._client[settings.DB_NAME]
                logger.info("Successfully connected to MongoDB")
            except Exception as e:
                logger.error(f"Failed to connect to MongoDB: {e}")
                raise

    @property
    def client(self) -> MongoClient:
        if not self._client:
            self.initialize()
        return self._client

    @property
    def db(self) -> Database:
        if not self._db:
            self.initialize()
        return self._db

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            logger.info("MongoDB connection closed")