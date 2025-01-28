from pymongo.collection import Collection
from app.models.reporSchema import GetReport
from bson import ObjectId
from typing import List, Optional
from app.database import MongoManager

class DatabaseService:
    def __init__(self, collection_name: str = "reports"):
        self.db_manager = MongoManager()
        self.collection: Collection = self.db_manager.db[collection_name]
    
    async def create_report(self, report: GetReport) -> GetReport:
        report_dict = report.model_dump(exclude={"id"})
        result = self.collection.insert_one(report_dict)
        report.id = result.inserted_id
        return report

    async def get_reports(self) -> List[GetReport]:
        reports = []
        cursor = self.collection.find()
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            reports.append(GetReport(**doc))
        return reports

    async def get_report(self, report_id: str) -> Optional[GetReport]:
        doc = self.collection.find_one({"_id": ObjectId(report_id)})
        if doc:
            doc["_id"] = str(doc["_id"])
            return GetReport(**doc)
        return None

    async def update_report(self, report_id: str, report: GetReport) -> Optional[GetReport]:
        report_dict = report.model_dump(exclude={"id"})
        result = self.collection.update_one(
            {"_id": ObjectId(report_id)},
            {"$set": report_dict}
        )
        if result.modified_count:
            return await self.get_report(report_id)
        return None

    async def delete_report(self, report_id: str) -> bool:
        result = self.collection.delete_one({"_id": ObjectId(report_id)})
        return result.deleted_count > 0