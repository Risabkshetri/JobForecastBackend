from fastapi import HTTPException, status
from datetime import datetime
import json
from app.models.forecastSchema import (
    JobRequest, 
    JobForecast,
    Location,
    MarketSummary,
    DemandMetrics,
    SkillsAnalysis,
    SalaryInsights,
    MarketFactors,
    Timeframe,
    Metadata
)
# from app.services.llama_service import LLMService
from app.services.db_service import DatabaseService
from app.services.llama_service import LLMService
from typing import List, Optional
from bson import ObjectId, errors as bson_errors
import logging

class JobController:
    def __init__(self):
        self.llm_service = LLMService()
        self.db_service = DatabaseService()

    async def create_forecast(self, request: JobRequest) -> JobForecast:
        logging.debug(f"Received request body: {request}")
        try:
            # print(request.dict())
            # Set default timeframe if not provided
            if not request.start_date or not request.end_date:
                request.start_date = datetime.now()
                request.end_date = datetime.now().replace(year=datetime.now().year + 1)
    
            # Generate forecast using LLM
            # forecast_data = await self.llm_service.generate_forecast(
            #     industry=request.industry,
            #     country=request.country,
            #     region=request.region,
            #     city=request.city,
            #     start_date=request.start_date,
            #     end_date=request.end_date,
            #     skills=request.skills,
            #     experience_level=request.experience_level,
            #     employment_type=request.employment_type
            # )
            
            # Create the JobForecast object using proper nested models
            forecast = JobForecast(
                industry=request.industry,
                location=Location(
                    country=request.country,
                    region=request.region,
                    city=request.city
                ),
                timeframe=Timeframe(
                    start_date=request.start_date,
                    end_date=request.end_date
                ),
                market_summary=request.market_summary,
                demand_metrics=request.demand_metrics,
                skills_analysis=request.skills_analysis,
                salary_insights=request.salary_insights,
                market_factors=request.market_factors,
                # market_summary=MarketSummary(
                #     forecast=forecast_data['market_summary']['forecast'],
                #     confidence_score=forecast_data['market_summary']['confidence_score'],
                #     growth_trajectory=forecast_data['market_summary']['growth_trajectory']
                # ),
                # demand_metrics=DemandMetrics(
                #     current_demand=forecast_data['demand_metrics']['current_demand'],
                #     projected_demand=forecast_data['demand_metrics']['projected_demand'],
                #     yoy_growth=forecast_data['demand_metrics']['yoy_growth'],
                #     job_openings_estimate=forecast_data['demand_metrics']['job_openings_estimate'],
                #     competition_level=forecast_data['demand_metrics']['competition_level']
                # ),
                # skills_analysis=SkillsAnalysis(
                #     required_skills=forecast_data['skills_analysis']['required_skills'],
                #     emerging_skills=forecast_data['skills_analysis']['emerging_skills'],
                #     complementary_skills=forecast_data['skills_analysis']['complementary_skills'],
                #     skill_gap_score=forecast_data['skills_analysis']['skill_gap_score']
                # ),
                # salary_insights=SalaryInsights(
                #     range_low=forecast_data['salary_insights']['range_low'],
                #     range_high=forecast_data['salary_insights']['range_high'],
                #     median=forecast_data['salary_insights']['median'],
                #     currency=forecast_data['salary_insights']['currency']
                # ),
                # market_factors=MarketFactors(
                #     positive_drivers=forecast_data['market_factors']['positive_drivers'],
                #     risk_factors=forecast_data['market_factors']['risk_factors'],
                #     industry_trends=forecast_data['market_factors']['industry_trends']
                # ),
                metadata=Metadata(
                    analysis_timestamp=datetime.now(),
                    data_freshness=datetime.now(),
                    geographic_scope=f"{request.region}, {request.country}",
                    industry_segment=request.industry
                )
            )
            
            # Save to database using the to_mongo method
            # mongo_data = forecast.to_mongo()
            return await self.db_service.create_forecast(forecast)
            
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Validation error: {str(e)}"
            )
        except KeyError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Missing required field: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error creating forecast: {str(e)}"
            )
    async def get_forecasts(
        self,
        industry: Optional[str] = None,
        country: Optional[str] = None,
        region: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[JobForecast]:
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

            return await self.db_service.get_forecasts(filter_conditions)
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving forecasts: {str(e)}"
            )

    async def get_forecast(self, forecast_id: str) -> JobForecast:
        try:
            # Validate ObjectId format
            if not ObjectId.is_valid(forecast_id):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Invalid forecast ID format"
                )

            forecast = await self.db_service.get_forecast(forecast_id)
            if not forecast:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Forecast not found"
                )
            return forecast
            
        except bson_errors.InvalidId:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid forecast ID format"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving forecast: {str(e)}"
            )

    async def update_forecast(self, forecast_id: str, request: JobRequest) -> JobForecast:
        try:
            # Validate ObjectId format
            if not ObjectId.is_valid(forecast_id):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Invalid forecast ID format"
                )

            # Generate new forecast
            forecast_data = await self.llm_service.generate_forecast(
                industry=request.industry,
                country=request.country,
                region=request.region,
                city=request.city,
                start_date=request.start_date,
                end_date=request.end_date,
                skills=request.skills,
                experience_level=request.experience_level,
                employment_type=request.employment_type
            )
            
            # Create updated forecast object
            forecast = JobForecast(
                industry=request.industry,
                location=Location(
                    country=request.country,
                    region=request.region,
                    city=request.city
                ),
                timeframe=Timeframe(
                    start_date=request.start_date,
                    end_date=request.end_date
                ),
                market_summary=forecast_data['market_summary'],
                demand_metrics=forecast_data['demand_metrics'],
                skills_analysis=forecast_data['skills_analysis'], 
                salary_insights=forecast_data['salary_insights'], 
                market_factors=forecast_data['market_factors'],
                metadata=Metadata(
                    analysis_timestamp=datetime.now(),
                    data_freshness=datetime.now(),
                    geographic_scope=f"{request.region}, {request.country}",
                    industry_segment=request.industry
                )
            )
            
            # Update in database
            updated_forecast = await self.db_service.update_forecast(forecast_id, forecast)
            if not updated_forecast:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Forecast not found"
                )
            return updated_forecast
            
        except bson_errors.InvalidId:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid forecast ID format"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error updating forecast: {str(e)}"
            )

    async def delete_forecast(self, forecast_id: str) -> bool:
        try:
            # Validate ObjectId format
            if not ObjectId.is_valid(forecast_id):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Invalid forecast ID format"
                )

            deleted = await self.db_service.delete_forecast(forecast_id)
            if not deleted:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Forecast not found"
                )
            return True
            
        except bson_errors.InvalidId:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid forecast ID format"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error deleting forecast: {str(e)}"
            )

    async def get_industry_trends(
        self,
        industry: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> dict:
        """Get aggregated trends for a specific industry"""
        try:
            return await self.db_service.get_industry_trends(industry, start_date, end_date)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving industry trends: {str(e)}"
            )

    async def get_skill_demand(
        self,
        skill: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> dict:
        """Get demand metrics for a specific skill across industries"""
        try:
            return await self.db_service.get_skill_demand(skill, start_date, end_date)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving skill demand: {str(e)}"
            )