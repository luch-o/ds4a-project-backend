import os
import boto3
from typing import List, Dict, Union
from api.data.utils import get_secret
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection


SECRET_NAME = os.environ["SECRET_NAME"]

aws = boto3.Session()
db_secret = get_secret(aws, SECRET_NAME)
conn = psycopg2.connect(
    host=db_secret["host"],
    database=db_secret["dbname"],
    user=db_secret["username"],
    password=db_secret["password"],
)


def query_db(conn: connection, query: str, values: List = None) -> List[Dict]:
    """
    Generic function to query the database and return the records with
    python core datatypes
    """
    if values is None:
        values = []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, values)
        resultset = cursor.fetchall()
        conn.commit()
    except:
        conn.rollback()
        raise

    return [dict(row) for row in resultset]


def get_tables(conn: connection) -> List[Dict[str, str]]:
    """
    Debug query to list public tables in the database
    """
    query = """--sql
    SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """

    return query_db(conn, query)


def list_departments(conn: connection) -> List[Dict[str, Union[str, int]]]:
    """
    Query the list of departments
    """
    query = """--sql
    SELECT code, name FROM departments
    """

    return query_db(conn, query)


def get_departments_population(conn: connection, year: int):
    """
    Query the population of each department in a given year
    """
    query = """--sql
    SELECT 
        d.code as code,
        d.name as name,
        dph.men as men,
        dph.women as women, 
        men + women as total
    FROM departmentsPopulationHistory dph
    JOIN departments d ON dph.department_id = d.code
    WHERE dph.year=%s
    """

    return query_db(conn, query, [year])


def get_municipalities_by_department(conn: connection, department_id: int):
    """
    Query the list of municipalities that belong to a given department
    """
    query = """--sql
    SELECT 
        name,
        code,
        CASE
            WHEN latitude = 'NaN' THEN NULL
            ELSE latitude
        END AS lat,
        CASE
            WHEN longitude = 'NaN' THEN NULL
            ELSE longitude
        END AS lon
    FROM municipalities
    WHERE department_id = %s 
    """

    return query_db(conn, query, [department_id])


def get_municipalities_population(conn: connection, department_id: int, year: int):
    """
    Get the population for a given year of the municipalities that belong to a 
    given department
    """
    query = """--sql
    SELECT 
        m.name as name,
        m.code as code,
        mph.total as population
    FROM municipalities m
    JOIN municipalityPopulationHistory mph ON m.code = mph.municipality_id
    WHERE m.department_id = %s AND mph.year = %s 
    """

    return query_db(conn, query, [department_id, year])

def get_interfamily_violence_cases(conn: connection, year: int):
    """
    Query the number of interfamily violence cases that took place in each 
    municipality for a given year
    """
    query = """--sql
    SELECT 
        m.name as municipality_name,
        m.code as municipality_code,
        d.name as department_name,
        d.code as department_code,
        mph.total as population,
        iv.count as violence_cases,
        CASE
            WHEN m.latitude = 'NaN' THEN NULL
            ELSE m.latitude
        END AS latitude,
        CASE
            WHEN m.longitude = 'NaN' THEN NULL
            ELSE m.longitude
        END AS longitude
    FROM interfamilyViolence iv
    JOIN municipalities m 
        ON iv.municipality_id = m.code
    JOIN departments d
        ON m.department_id = d.code
    JOIN municipalityPopulationHistory mph 
        ON iv.municipality_id = mph.municipality_id AND iv.year = mph.year
    WHERE iv.year = %s
    """

    return query_db(conn, query, [year])

def get_suicide_cases(conn: connection, year:int):
    """
    Query the suicide cases for each municipality for a given year
    """
    query = """--sql
    SELECT 
        m.name as municipality_name,
        m.code as municipality_code,
        d.name as department_name,
        d.code as department_code,
        mph.total as population,
        s.count as suicides,
        CASE
            WHEN m.latitude = 'NaN' THEN NULL
            ELSE m.latitude
        END AS latitude,
        CASE
            WHEN m.longitude = 'NaN' THEN NULL
            ELSE m.longitude
        END AS longitude
    FROM suicides s
    JOIN municipalities m 
        ON s.municipality_id = m.code
    JOIN departments d
        ON m.department_id = d.code
    JOIN municipalityPopulationHistory mph 
        ON s.municipality_id = mph.municipality_id AND s.year = mph.year
    WHERE s.year = %s
    """

    return query_db(conn, query, [year])

def get_suicide_attempts(conn: connection, year:int):
    """
    Query the suicide attempts for each municipality for a given year
    """
    query = """--sql
    SELECT 
        m.name as municipality_name,
        m.code as municipality_code,
        d.name as department_name,
        d.code as department_code,
        mph.total as population,
        sum(sa.count) as suicide_attempts,
        CASE
            WHEN m.latitude = 'NaN' THEN NULL
            ELSE m.latitude
        END AS latitude,
        CASE
            WHEN m.longitude = 'NaN' THEN NULL
            ELSE m.longitude
        END AS longitude
    FROM suicideAttempts sa
    JOIN municipalities m
        ON sa.municipality_id = m.code
    JOIN departments d
        ON m.department_id = d.code
    JOIN municipalityPopulationHistory mph 
        ON sa.municipality_id = mph.municipality_id AND sa.year = mph.year
    WHERE sa.year = %s
    GROUP BY
        municipality_name,
        municipality_code,
        department_name,
        department_code,
        population
    """

    return query_db(conn, query, [year])
