import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch
from psycopg2.extensions import cursor, connection


class BaseTable:
    name: str
    create_statement: str
    insert_statement: str

    @classmethod
    def prepare_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    def create_table(cls, cur: cursor, conn: connection):

        try:
            cur.execute(cls.create_statement)
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise

    @classmethod
    def insert_data(cls, df: pd.DataFrame, cur: cursor, conn: connection):
        data = cls.prepare_data(df)
        rows = [list(row) for row in data.itertuples(index=False)]

        try:
            execute_batch(cur, cls.insert_statement, rows)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise


class Departments(BaseTable):
    name = "Departments"

    create_statement = """--sql
    CREATE TABLE IF NOT EXISTS departments (
        code INTEGER PRIMARY KEY,
        name VARCHAR(30) NOT NULL
    )
    """

    insert_statement = """--sql
    INSERT INTO departments (
        code, name
    )
    VALUES (%s, %s)
    """

    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        return df[["DP", "DPNOM"]].drop_duplicates()


class DepartmentsPopulationHistory(BaseTable):
    name = "DepartmentsPopulationHistory"

    create_statement = """--sql
    CREATE TABLE IF NOT EXISTS departmentsPopulationHistory (
        id SERIAL PRIMARY KEY,
        year INTEGER,
        women INTEGER,
        men INTEGER,
        department_id INTEGER REFERENCES departments(code)
    )
    """

    insert_statement = """--sql
    INSERT INTO departmentsPopulationHistory (
        year, women, men, department_id
    )
    VALUES (%s, %s, %s, %s)
    """

    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        return df[["AÑO", "Total Mujeres", "Total Hombres", "DP"]]


class Municipalities(BaseTable):
    name = "Municipalities"

    create_statement: str = """--sql
    CREATE TABLE IF NOT EXISTS municipalities (
        code INTEGER PRIMARY KEY,
        name VARCHAR(30) NOT NULL,
        latitude NUMERIC,
        longitude NUMERIC,
        department_id INTEGER REFERENCES departments(code)
    )
    """

    insert_statement: str = """--sql
    INSERT INTO municipalities (
        code, name, latitude, longitude, department_id
    )
    VALUES (%s, %s, %s, %s, %s)
    """

    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        return df[["COD_MUNICIPIO","MPNOM","LATITUD","LONGITUD","DP"]]


class MunicipalityPopulationHistory(BaseTable):
    name = "MunicipalityPopulationHistory"

    create_statement: str = """--sql
    CREATE TABLE IF NOT EXISTS municipalityPopulationHistory (
        id SERIAL PRIMARY KEY,
        year INTEGER,
        total INTEGER,
        municipality_id INTEGER REFERENCES municipalities(code)
    )
    """

    insert_statement: str = """--sql
    INSERT INTO municipalityPopulationHistory (
        year, total, municipality_id
    )
    VALUES (%s, %s, %s)
    """

    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        return df[["AÑO", "Total", "COD_MUNICIPIO"]]


class InterfamilyViolence(BaseTable):
    create_statement: str = """--sql
    CREATE TABLE IF NOT EXISTS interfamilyViolence (
        id SERIAL PRIMARY KEY,
        year INTEGER,
        count INTEGER,
        municipality_id INTEGER references municipalities(code)
    )
    """

    insert_statement: str = """--sql
    INSERT INTO interfamilyViolence (
        year, count
    )
    VALUES (%s, %s)
    """

    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        return df[["YEAR", "CANTIDAD", "CODE_MUNICIPIO"]]


class Suicides(BaseTable):
    create_statement: str = """--sql
    CREATE TABLE IF NOT EXISTS suicides (
        id SERIAL PRIMARY KEY,
        year INTEGER,
        count INTEGER,
        municipality_id INTEGER references municipalities(code)
    )
    """

    insert_statement: str = """--sql
    INSERT INTO suicides (
        year, count
    )
    VALUES (%s, %s)
    """

    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        return df[["YEAR", "CODE_MUNICIPIO", "SUI_COUNTER"]]


class SuicideAttempts(BaseTable):
    create_statement: str = """--sql
    CREATE TABLE IF NOT EXISTS suicideAttempts (
        id SERIAL PRIMARY KEY,
        year INTEGER,
        count INTEGER,
        municipality_id INTEGER references municipalities(code)
    )
    """

    insert_statement: str = """--sql
    INSERT INTO suicideAttempts (
        year, count
    )
    VALUES (%s, %s)
    """

    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        return df[["YEAR", "WEEK", "COUNTER_TRY", "CODE_MUNICIPIO"]]
