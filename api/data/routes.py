from fastapi import APIRouter, Query
from api.data.queries import (
    conn,
    get_tables,
    list_departments,
    get_departments_population,
    get_municipalities_by_department,
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
    return {"data": list_departments(conn)}


@router.get("/departments_population", tags=["departments"])
async def departments_population(
    year: int = Query(
        title="The year to get departmets population from", ge=2016, le=2022
    )
):
    """
    Returns population of every department for a given year
    """
    return {"data": get_departments_population(conn, year)}


@router.get("/municipalities", tags=["municipalities"])
async def municipalities(department_id: int = Query(title="Department code")):
    """
    Return a list of municipalities belonging to a given department
    """
    return {"data": get_municipalities_by_department(conn, department_id)}


@router.get("/municipalities_population")
async def municipalities_population(
    department_id: int = Query(title="Department code"),
    year: int = Query(
        title="The year to get departmets population from", ge=2016, le=2022
    )
):
    return {
        "message": "NOT IMPLEMENTED",
        "year": year,
        "department_id": department_id,
    }
