from fastapi import APIRouter, HTTPException
from app.models.forecastSchema import JobRequest, JobForecast
from app.controllers.job_controller import JobController
from typing import List

router = APIRouter()
controller = JobController()

@router.post("/forecast", response_model=JobForecast)
async def create_forecast(request: JobRequest):
    return await controller.create_forecast(request)

@router.get("/forecasts", response_model=List[JobForecast])
async def get_forecasts():
    return await controller.get_forecasts()

@router.get("/forecast/{forecast_id}", response_model=JobForecast)
async def get_forecast(forecast_id: str):
    return await controller.get_forecast(forecast_id)

@router.put("/forecast/{forecast_id}", response_model=JobForecast)
async def update_forecast(forecast_id: str, request: JobRequest):
    return await controller.update_forecast(forecast_id, request)

@router.delete("/forecast/{forecast_id}")
async def delete_forecast(forecast_id: str):
    return await controller.delete_forecast(forecast_id)