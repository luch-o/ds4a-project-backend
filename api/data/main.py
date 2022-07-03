from fastapi import FastAPI
from mangum import Mangum
from api.data.routes import router as data_router


app = FastAPI(docs_url="/data/docs", openapi_url="/data/openapi.json")
app.include_router(data_router, prefix="/data")

handler = Mangum(app)
