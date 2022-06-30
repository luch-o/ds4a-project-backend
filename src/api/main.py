import os
import boto3
import psycopg2
from fastapi import FastAPI
from mangum import Mangum
from src.api.utils import get_secret
from src.api.queries import get_tables, list_departments, get_departments_population

SECRET_NAME = os.environ["SECRET_NAME"]

aws = boto3.Session()
db_secret = get_secret(aws, SECRET_NAME)
conn = psycopg2.connect(
    host=db_secret["host"],
    database=db_secret["dbname"],
    user=db_secret["username"],
    password=db_secret["password"],
)


app = FastAPI()


@app.get("/")
async def root():
    """
    Returns list of public tables in the database
    """
    return {"tables": get_tables(conn)}


@app.get("/departments")
async def departments():
    """
    Returns a list of departments
    """
    return {"departments": list_departments(conn)}


@app.get("/municipalities")
async def municipalities():
    return {"message": "Endpoint to return a list of departmetns"}


@app.get("/departments_population")
async def departments_population(year: int):
    """
    Returns population of every department for a given year
    """
    return {"data": get_departments_population(conn, year)}


@app.get("/municipalities_population")
async def municipalities_population(year: int):
    return {
        "message": "Endpoint to return the population of each municipality in a given year",
        "year": year,
    }


handler = Mangum(app)
