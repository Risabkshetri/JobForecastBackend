from pydantic import BaseModel, Field, ConfigDict, BeforeValidator, PlainSerializer
from typing import List, Optional, Annotated, Literal
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
   


class MarketSummary(BaseModel):
    forecast: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    growth_trajectory: Literal["Declining", "Stable", "Growing", "Rapidly Growing"]

class DemandMetrics(BaseModel):
    current_demand: float = Field(ge=0, le=10)
    projected_demand: float = Field(ge=0, le=10)
    yoy_growth: float
    job_openings_estimate: int
    competition_level: Literal["Low", "Moderate", "High"]

class SkillsAnalysis(BaseModel):
    required_skills: List[str] = Field(min_items=3, max_items=3)
    emerging_skills: List[str] = Field(min_items=3, max_items=3)
    complementary_skills: List[str] = Field(min_items=3, max_items=3)
    skill_gap_score: float = Field(ge=0.0, le=1.0)

class SalaryInsights(BaseModel):
    range_low: int
    range_high: int
    median: int
    currency: str = Field(min_length=3, max_length=3)

class MarketFactors(BaseModel):
    positive_drivers: List[str] = Field(min_items=3, max_items=3)
    risk_factors: List[str] = Field(min_items=3, max_items=3)
    industry_trends: List[str] = Field(min_items=3, max_items=3)

class Metadata(BaseModel):
    analysis_timestamp: datetime = Field(default_factory=datetime.now)
    data_freshness: datetime
    geographic_scope: str
    industry_segment: str

class JobRequest(BaseModel):
    industry: str
    country: str
    region: str
    city: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    skills: Optional[List[str]] = None
    experience_level: str = "Mid"
    employment_type: str = "Full-time"
    market_summary: MarketSummary
    demand_metrics: DemandMetrics
    skills_analysis: SkillsAnalysis
    salary_insights: SalaryInsights
    market_factors: MarketFactors
    # metadata: Metadata
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True
    )

class JobForecast(BaseModel):
    id: Optional[PyObjectId] = Field(default=None, alias="_id")
    industry: str
    location: Location
    timeframe: Timeframe
    market_summary: MarketSummary
    demand_metrics: DemandMetrics
    skills_analysis: SkillsAnalysis
    salary_insights: SalaryInsights
    market_factors: MarketFactors
    metadata: Metadata
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        alias_generator=None,
        json_schema_extra={
            "example": {
                "industry": "Technology",
                "location": {
                    "country": "United States",
                    "region": "California",
                    "city": "San Francisco"
                },
                "timeframe": {
                    "start_date": "2025-01-01T00:00:00Z",
                    "end_date": "2025-12-31T00:00:00Z"
                },
                "market_summary": {
                    "forecast": "Tech sector shows robust growth with increasing demand for AI specialists",
                    "confidence_score": 0.85,
                    "growth_trajectory": "Growing"
                },
                "demand_metrics": {
                    "current_demand": 8,
                    "projected_demand": 9,
                    "yoy_growth": 15.5,
                    "job_openings_estimate": 50000,
                    "competition_level": "High"
                },
                "skills_analysis": {
                    "required_skills": ["Python", "Machine Learning", "Cloud Architecture"],
                    "emerging_skills": ["Quantum Computing", "Edge Computing", "Zero-Trust Security"],
                    "complementary_skills": ["Project Management", "System Design", "Data Ethics"],
                    "skill_gap_score": 0.7
                },
                "salary_insights": {
                    "range_low": 120000,
                    "range_high": 200000,
                    "median": 150000,
                    "currency": "USD"
                },
                "market_factors": {
                    "positive_drivers": [
                        "Digital transformation initiatives",
                        "AI adoption across industries",
                        "Remote work technology demand"
                    ],
                    "risk_factors": [
                        "Tech sector layoffs",
                        "Economic uncertainty",
                        "Rapid skill obsolescence"
                    ],
                    "industry_trends": [
                        "Increased focus on AI/ML",
                        "Rise of remote-first companies",
                        "Growing cybersecurity demands"
                    ]
                }
            }
        }
    )

    def to_mongo(self) -> dict:
        """Convert the model to MongoDB format"""
        data = self.model_dump(by_alias=True, exclude_none=True)
        if data.get("_id") is not None:
            data["_id"] = ObjectId(data["_id"])
        return data