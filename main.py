from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.job_routes import router as job_router
from app.routes.report_routes import router as report_router
from app.database import MongoManager
import logging
from app.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Job Market Forecasting API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "https://job-market-forecasting.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database connection
db_manager = MongoManager()

@app.on_event("startup")
async def startup_event():
    try:
        db_manager.initialize()
        logger.info("Database connection initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    db_manager.close()
    logger.info("Database connection closed")

app.include_router(job_router, prefix="/api/v1", tags=["jobs"])
app.include_router(report_router, prefix="/api/v1", tags=["reports"])

@app.get("/")
async def root():
    return {"message": "Job Market Forecasting API"}

if __name__ == "__main__":
    import uvicorn
    port = settings.PORT or 8000
    host = "0.0.0.0"
    uvicorn.run("main:app", host=host, port=port, reload=False)
