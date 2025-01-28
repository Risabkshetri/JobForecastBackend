from fastapi import APIRouter, HTTPException
from app.models.reporSchema import ReportRequest, GetReport
from app.controllers.personalized_report import ReportController
from typing import List

router = APIRouter()
controller = ReportController()

@router.post("/report", response_model=GetReport)
async def create_report(request: ReportRequest):
    return await controller.create_report(request)

@router.get("/reports", response_model=List[GetReport])
async def get_reports():
    return await controller.get_reports()

@router.get("/report/{report_id}", response_model=GetReport)
async def get_forecast(forecast_id: str):
    return await controller.get_report(forecast_id)

@router.put("/report/{report_id}", response_model=GetReport)
async def update_forecast(forecast_id: str, request: ReportRequest):
    return await controller.update_report(forecast_id, request)

@router.delete("/report/{report_id}")
async def delete_report(forecast_id: str):
    return await controller.report(forecast_id)