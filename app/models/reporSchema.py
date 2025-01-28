from pydantic import BaseModel, Field, ConfigDict, BeforeValidator, PlainSerializer
from typing import List, Optional, Annotated
from datetime import datetime
from bson import ObjectId

# Custom type for ObjectId handling
PyObjectId = Annotated[
    str,
    BeforeValidator(lambda x: str(x) if isinstance(x, ObjectId) else x),
    PlainSerializer(lambda x: str(x), return_type=str),
]
class Location(BaseModel):
    country: str
    region: str
    city: Optional[str] = None

class Timeframe(BaseModel):
    start_date: datetime
    end_date: datetime

class Metadata(BaseModel):
    analysis_timestamp: datetime = Field(default_factory=datetime.now)
    data_freshness: datetime
    geographic_scope: str
    industry_segment: str

class ReportRequest(BaseModel):
    industry: str
    country: str
    region: str
    report:str
    city: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    skills: Optional[List[str]] = None
    experience_level: str = "Mid"
    employment_type: str = "Full-time"
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True
    )

class GetReport(BaseModel):
    id: Optional[PyObjectId] = Field(default=None, alias="_id")
    industry: str
    location: Location
    timeframe: Timeframe
    report: str
    metadata: Metadata
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        alias_generator=None
    )

    def to_mongo(self) -> dict:
        """Convert the model to MongoDB format"""
        data = self.model_dump(by_alias=True, exclude_none=True)
        if data.get("_id") is not None:
            data["_id"] = ObjectId(data["_id"])
        return data
