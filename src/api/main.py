from fastapi import FastAPI
from mangum import Mangum

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/departments")
async def departments():
    return {"message": "Endpoint to return a list of departmetns"}


@app.get("/municipalities")
async def municipalities():
    return {"message": "Endpoint to return a list of departmetns"}


@app.get("/departments_population")
async def departments_population(year: int):
    return {
        "message": "Endpoint to return the population of each departments in a given year",
        "year": year,
    }

@app.get("/municipalities_population")
async def municipalities_population(year: int):
    return {
        "message": "Endpoint to return the population of each municipality in a given year",
        "year": year,
    }

handler = Mangum(app)