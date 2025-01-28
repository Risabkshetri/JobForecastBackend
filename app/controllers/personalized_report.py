from fastapi import HTTPException
from app.models.reporSchema import GetReport, ReportRequest, Location, Timeframe, Metadata
from datetime import datetime
from app.services.reportLlama import LLMService
from app.services.reportdb import DatabaseService

from typing import List

class ReportController:
    def __init__(self):
        self.llm_service = LLMService()
        self.db_service = DatabaseService()

    async def create_report(self, request: ReportRequest) -> GetReport:
        try:
            # print(request.dict())
            # Set default timeframe if not provided
            if not request.start_date or not request.end_date:
                request.start_date = datetime.now()
                request.end_date = datetime.now().replace(year=datetime.now().year + 1)

            # report_data = await self.llm_service.generate_report(
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
            report_data = ReportRequest(
                industry=request.industry,
                country=request.country,
                region=request.region,
                city=request.city,
                start_date=request.start_date,
                end_date=request.end_date,
                report=request.report,
                skills=request.skills,
                experience_level=request.experience_level,
                employment_type=request.employment_type
            )

            # print(report_data)
            
            # Create report object
            report = GetReport(
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
                # report = report_data['report'],
                report = report_data.report, 
                metadata=Metadata(
                    analysis_timestamp=datetime.now(),
                    data_freshness=datetime.now(),
                    geographic_scope=f"{request.region}, {request.country}",
                    industry_segment=request.industry
                )
            )
            
            # Save to database
            return await self.db_service.create_report(report)
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def get_reports(self) -> List[GetReport]:
        try:
            return await self.db_service.get_reports()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def get_report(self, report_id: str) -> GetReport:
        try:
            report = await self.db_service.get_report(report_id)
            if not report:
                raise HTTPException(status_code=404, detail="report not found")
            return report
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def update_report(self, report_id: str, request: ReportRequest) -> GetReport:
        try:
            # Generate new report
            report_data = await self.llm_service.generate_report(
                request.industry,
                request.location,
                request.timeframe,
                request.skills
            )
            
         # Create report object
            report = GetReport(
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
                report = report_data['report'],
                metadata=Metadata(
                    analysis_timestamp=datetime.now(),
                    data_freshness=datetime.now(),
                    geographic_scope=f"{request.region}, {request.country}",
                    industry_segment=request.industry
                )
            )
            
            
            # Update in database
            updated_report = await self.db_service.update_report(report_id, report)
            if not updated_report:
                raise HTTPException(status_code=404, detail="report not found")
            return updated_report
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def delete_report(self, report_id: str) -> bool:
        try:
            deleted = await self.db_service.delete_report(report_id)
            if not deleted:
                raise HTTPException(status_code=404, detail="report not found")
            return True
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
