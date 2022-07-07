from fastapi import FastAPI
from mangum import Mangum
from routes import router as predict_router
import uvicorn

app = FastAPI(docs_url="/predict/docs", openapi_url="/predict/openapi.json")
app.include_router(predict_router, prefix="/predict")

handler = Mangum(app)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
    