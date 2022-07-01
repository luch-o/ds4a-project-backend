from fastapi import FastAPI
from mangum import Mangum
from src.api.routes import router as data_router


app = FastAPI(docs_url="/data/docs")
app.include_router(data_router, prefix="/data")

handler = Mangum(app)
