from pymongo.collection import Collection
from app.models.forecastSchema import JobForecast
from bson import ObjectId
from typing import List, Optional
from datetime import datetime
from app.database import MongoManager
import time
import logging

logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self, collection_name: str = "forecasts", max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                self.db_manager = MongoManager()
                self.db_manager.initialize()  # Explicitly initialize the connection
                self.collection: Collection = self.db_manager.db[collection_name]
                # Setup indexes
                self._setup_indexes()
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to initialize database service after {max_retries} attempts: {str(e)}")
                    raise
                time.sleep(1)  # Wait before retrying
                
    def _setup_indexes(self):
        """Setup necessary indexes for better query performance"""
        try:
            # Create indexes if they don't exist
            existing_indexes = self.collection.index_information()
            
            indexes = [
                ("industry", 1),
                [("location.country", 1), ("location.region", 1)],
                [("timeframe.start_date", 1), ("timeframe.end_date", 1)],
                [("metadata.analysis_timestamp", 1)]
            ]
            
            for index in indexes:
                if isinstance(index, tuple):
                    index_name = f"{index[0]}_1"
                    if index_name not in existing_indexes:
                        self.collection.create_index([index])
                else:
                    # For compound indexes
                    index_name = "_".join(f"{field}_{direction}" for field, direction in index)
                    if index_name not in existing_indexes:
                        self.collection.create_index(index)
                        
        except Exception as e:
            logger.error(f"Error setting up indexes: {str(e)}")
            raise
    
    async def create_forecast(self, forecast: JobForecast) -> JobForecast:
        """Create a new job market forecast"""
        try:
            if self.collection is None:
                raise Exception("Database connection not initialized")
                
            forecast_dict = forecast.model_dump(exclude={"id"})
            result = self.collection.insert_one(forecast_dict)
            forecast.id = str(result.inserted_id)  # Convert ObjectId to string
            return forecast
            
        except Exception as e:
            logger.error(f"Error creating forecast: {str(e)}")
            raise

    async def get_forecasts(
        self,
        industry: Optional[str] = None,
        country: Optional[str] = None,
        region: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[JobForecast]:
        """Get all forecasts with optional filters"""
        try:
            if self.collection is None:
                raise Exception("Database connection not initialized")
                
            filter_conditions = {}
            if industry:
                filter_conditions["industry"] = industry
            if country:
                filter_conditions["location.country"] = country
            if region:
                filter_conditions["location.region"] = region
            if start_date:
                filter_conditions["timeframe.start_date"] = {"$gte": start_date}
            if end_date:
                filter_conditions["timeframe.end_date"] = {"$lte": end_date}

            forecasts = []
            cursor = self.collection.find(filter_conditions)
            
            for doc in cursor:
                doc["_id"] = str(doc["_id"])  # Convert ObjectId to string
                forecasts.append(JobForecast(**doc))
            
            return forecasts
            
        except Exception as e:
            logger.error(f"Error retrieving forecasts: {str(e)}")
            raise

    def __del__(self):
        """Cleanup when the service is destroyed"""
        try:
            if hasattr(self, 'db_manager'):
                self.db_manager.close()
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")