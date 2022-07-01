from fastapi import APIRouter, Path
from src.api.queries import (
    conn,
    get_tables,
    list_departments,
    get_departments_population,
)

router = APIRouter()


@router.get("/")
async def root():
    """
    Returns list of public tables in the database
    """
    return {"tables": get_tables(conn)}


@router.get("/departments", tags=["departments"])
async def departments():
    """
    Returns a list of departments
    """
    return {"departments": list_departments(conn)}


@router.get("/departments_population", tags=["departments"])
async def departments_population(
    year: int = Path(
        title="The year to get departmets population from", ge=2016, le=2022
    )
):
    """
    Returns population of every department for a given year
    """
    return {"data": get_departments_population(conn, year)}


@router.get("/municipalities")
async def municipalities():
    return {"message": "Endpoint to return a list of departmetns"}


@router.get("/municipalities_population")
async def municipalities_population(
    year: int = Path(
        title="The year to get departmets population from", ge=2016, le=2022
    )
):
    return {
        "message": "Endpoint to return the population of each municipality in a given year",
        "year": year,
    }
