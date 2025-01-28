from pymongo.collection import Collection
from app.models.reporSchema import GetReport
from bson import ObjectId
from typing import List, Optional
from app.database import MongoManager
import logging

logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self, collection_name: str = "reports"):
        try:
            self.db_manager = MongoManager()
            self.db_manager.initialize()
            self.collection: Collection = self.db_manager.db[collection_name]
        except Exception as e:
            logger.error(f"Failed to initialize database service: {str(e)}")
            raise
    
    async def create_report(self, report: GetReport) -> GetReport:
        try:
            if self.collection is None:
                raise Exception("Database connection not initialized")
            
            report_dict = report.model_dump(exclude={"id"})
            result = self.collection.insert_one(report_dict)
            report.id = str(result.inserted_id)
            return report
        except Exception as e:
            logger.error(f"Error creating report: {str(e)}")
            raise

    async def get_reports(self) -> List[GetReport]:
        try:
            if self.collection is None:
                raise Exception("Database connection not initialized")
            
            reports = []
            cursor = self.collection.find()
            for doc in cursor:
                doc["_id"] = str(doc["_id"])
                reports.append(GetReport(**doc))
            return reports
        except Exception as e:
            logger.error(f"Error retrieving reports: {str(e)}")
            raise

    async def get_report(self, report_id: str) -> Optional[GetReport]:
        try:
            if self.collection is None:
                raise Exception("Database connection not initialized")
            
            doc = self.collection.find_one({"_id": ObjectId(report_id)})
            if doc:
                doc["_id"] = str(doc["_id"])
                return GetReport(**doc)
            return None
        except Exception as e:
            logger.error(f"Error retrieving report: {str(e)}")
            raise

    async def update_report(self, report_id: str, report: GetReport) -> Optional[GetReport]:
        try:
            if self.collection is None:
                raise Exception("Database connection not initialized")
            
            report_dict = report.model_dump(exclude={"id"})
            result = self.collection.update_one(
                {"_id": ObjectId(report_id)},
                {"$set": report_dict}
            )
            if result.modified_count:
                return await self.get_report(report_id)
            return None
        except Exception as e:
            logger.error(f"Error updating report: {str(e)}")
            raise

    async def delete_report(self, report_id: str) -> bool:
        try:
            if self.collection is None:
                raise Exception("Database connection not initialized")
            
            result = self.collection.delete_one({"_id": ObjectId(report_id)})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting report: {str(e)}")
            raise

    def __del__(self):
        """Cleanup when the service is destroyed"""
        try:
            if hasattr(self, 'db_manager'):
                self.db_manager.close()
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")