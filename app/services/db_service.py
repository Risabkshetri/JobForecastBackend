from pymongo.collection import Collection
from app.models.forecastSchema import JobForecast
from bson import ObjectId
from typing import List, Optional
from datetime import datetime
from app.database import MongoManager

class DatabaseService:
    def __init__(self, collection_name: str = "forecasts"):
        self.db_manager = MongoManager()
        self.collection: Collection = self.db_manager.db[collection_name]
        
        # Create indexes for better query performance
        self._setup_indexes()
    
    def _setup_indexes(self):
        """Setup necessary indexes for better query performance"""
        self.collection.create_index([("industry", 1)])
        self.collection.create_index([("location.country", 1), ("location.region", 1)])
        self.collection.create_index([("timeframe.start_date", 1), ("timeframe.end_date", 1)])
        self.collection.create_index([("metadata.analysis_timestamp", 1)])
    
    async def create_forecast(self, forecast: JobForecast) -> JobForecast:
        """Create a new job market forecast"""
        try:
            forecast_dict = forecast.model_dump(exclude={"id"})
            result = self.collection.insert_one(forecast_dict)
            forecast.id = result.inserted_id
            return forecast
            
        except Exception as e:
            raise Exception(f"Error creating forecast: {str(e)}")

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
            # Build filter conditions
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

            # Query database
            forecasts = []
            cursor = self.collection.find(filter_conditions)
            
            for doc in cursor:
                doc["_id"] = str(doc["_id"])
                forecasts.append(JobForecast(**doc))
            
            return forecasts
            
        except Exception as e:
            raise Exception(f"Error retrieving forecasts: {str(e)}")

    async def get_forecast(self, forecast_id: str) -> Optional[JobForecast]:
        """Get a specific forecast by ID"""
        try:
            doc = self.collection.find_one({"_id": ObjectId(forecast_id)})
            if doc:
                doc["_id"] = str(doc["_id"])
                return JobForecast(**doc)
            return None
            
        except Exception as e:
            raise Exception(f"Error retrieving forecast: {str(e)}")

    async def update_forecast(self, forecast_id: str, forecast: JobForecast) -> Optional[JobForecast]:
        """Update a specific forecast"""
        try:
            # Convert Pydantic model to dict and exclude id
            forecast_dict = forecast.model_dump(exclude={"id"})
            
            # Update the document
            result = self.collection.update_one(
                {"_id": ObjectId(forecast_id)},
                {"$set": forecast_dict}
            )
            
            if result.modified_count:
                return await self.get_forecast(forecast_id)
            return None
            
        except Exception as e:
            raise Exception(f"Error updating forecast: {str(e)}")

    async def delete_forecast(self, forecast_id: str) -> bool:
        """Delete a specific forecast"""
        try:
            result = self.collection.delete_one({"_id": ObjectId(forecast_id)})
            return result.deleted_count > 0
            
        except Exception as e:
            raise Exception(f"Error deleting forecast: {str(e)}")

    async def get_industry_trends(
        self,
        industry: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> dict:
        """Get aggregated trends for a specific industry"""
        try:
            pipeline = [
                {
                    "$match": {
                        "industry": industry,
                        **({
                            "timeframe.start_date": {
                                "$gte": start_date,
                                "$lte": end_date
                            }
                        } if start_date and end_date else {})
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_demand": {"$avg": "$demand_metrics.current_demand"},
                        "avg_confidence": {"$avg": "$market_summary.confidence_score"},
                        "total_forecasts": {"$sum": 1},
                        "salary_trends": {
                            "$push": {
                                "date": "$timeframe.start_date",
                                "median": "$salary_insights.median"
                            }
                        }
                    }
                }
            ]
            
            results = list(self.collection.aggregate(pipeline))
            return results[0] if results else {}
            
        except Exception as e:
            raise Exception(f"Error retrieving industry trends: {str(e)}")

    async def get_skill_demand(
        self,
        skill: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> dict:
        """Get demand metrics for a specific skill"""
        try:
            pipeline = [
                {
                    "$match": {
                        "$or": [
                            {"skills_analysis.required_skills": skill},
                            {"skills_analysis.emerging_skills": skill},
                            {"skills_analysis.complementary_skills": skill}
                        ],
                        **({
                            "timeframe.start_date": {
                                "$gte": start_date,
                                "$lte": end_date
                            }
                        } if start_date and end_date else {})
                    }
                },
                {
                    "$group": {
                        "_id": "$industry",
                        "avg_demand": {"$avg": "$demand_metrics.current_demand"},
                        "total_positions": {"$sum": "$demand_metrics.job_openings_estimate"},
                        "avg_salary": {"$avg": "$salary_insights.median"}
                    }
                },
                {
                    "$sort": {"avg_demand": -1}
                }
            ]
            
            results = list(self.collection.aggregate(pipeline))
            return {
                "skill": skill,
                "demand_by_industry": results,
                "total_industries": len(results),
                "total_positions": sum(r["total_positions"] for r in results)
            }
            
        except Exception as e:
            raise Exception(f"Error retrieving skill demand: {str(e)}")