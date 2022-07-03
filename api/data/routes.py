from fastapi import APIRouter, Query
from api.data.queries import (
    conn,
    get_tables,
    list_departments,
    get_departments_population,
    get_municipalities_by_department,
    get_municipalities_population,
    get_interfamily_violence_cases,
    get_suicide_cases,
    get_suicide_attempts,
)

router = APIRouter()


@router.get("/", tags=["root"])
async def root():
    """
    Development endpoint. Returns list of public tables in the database
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


@router.get("/municipalities_population", tags=["municipalities"])
async def municipalities_population(
    department_id: int = Query(title="Department code"),
    year: int = Query(
        title="The year to get departmets population from", ge=2016, le=2022
    ),
):
    """
    Returns the population of the municipalities in a department for a given year
    """
    return {"data": get_municipalities_population(conn, department_id, year)}


@router.get("/interfamily_violence", tags=["cases"])
async def iterfamily_violence(
    year: int = Query(
        title="The year to get departmets population from", ge=2016, le=2022
    )
):
    """
    Return the number of interfamily cases per municipality for a given year.
    Records also include population for that year and municipality coordinates
    """
    return {"data": get_interfamily_violence_cases(conn, year)}


@router.get("/suicides", tags=["cases"])
async def suicides(
    year: int = Query(
        title="The year to get departmets population from", ge=2016, le=2022
    )
):
    """
    Return the number of suicide cases per municipality for a given year.
    Records also include population for that year and municipality coordinates
    """
    return {"data": get_suicide_cases(conn, year)}


@router.get("/suicide_attempts", tags=["cases"])
async def suicide_attempts(
    year: int = Query(
        title="The year to get departmets population from", ge=2016, le=2022
    )
):
    """
    Return the number of suicide attempts per municipality for a given year.
    Records also include population for that year and municipality coordinates
    """
    return {"data": get_suicide_attempts(conn, year)}
