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
    query = """--sql
    SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """

    return query_db(conn, query)


def list_departments(conn: connection) -> List[Dict[str, Union[str, int]]]:
    query = """--sql
    SELECT code, name FROM departments
    """

    return query_db(conn, query)


def get_departments_population(conn: connection, year: int):
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
