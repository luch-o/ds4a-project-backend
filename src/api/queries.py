from typing import List, Dict, Union
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection


def query_db(conn: connection, query: str, values: List=None) -> List[Dict]:
    if values is None:
        values = []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, values)
        resultset = cursor.fetchall()
        conn.commit()
    except:
        resultset = []
        conn.rollback()

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