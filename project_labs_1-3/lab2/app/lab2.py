import os
from datetime import datetime
from typing import List

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from neo4j import GraphDatabase
from neo4j import exceptions as neo4j_exceptions
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mirea")
POSTGRES_DB = os.getenv("POSTGRES_DB", "university")

NEO4J_URI = f"bolt://{os.getenv('NEO4J_HOST', 'neo4j')}:7687"
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mireamirea")

app = FastAPI(title="Lab2 Service")

# --- ответ ---
class CourseRequirementItem(BaseModel):
    course_id: int = Field(..., description="ID курса")
    course_name: str = Field(..., description="Название курса")
    lecture_topic: str = Field(..., description="Тема лекции")
    tech_requirements: str = Field(..., description="Технические требования")
    student_count: int = Field(..., description="Количество студентов")
    current_capacity: int = Field(..., description="Текущая вместимость аудитории")
    is_suitable: bool = Field(..., description="Подходит ли аудитория")
    semester: int = Field(..., description="Семестр")
    year: int = Field(..., description="Год обучения")

# --- бд ---
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port="5432"
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise HTTPException(status_code=503, detail=f"PostgreSQL connection error: {e}")

def get_neo4j_driver():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        return driver
    except neo4j_exceptions.ServiceUnavailable as e:
        print(f"Error connecting to Neo4j: {e}")
        raise HTTPException(status_code=503, detail=f"Neo4j connection error: {e}")
    except neo4j_exceptions.AuthError as e:
        print(f"Neo4j Authentication Error: {e}. Check credentials. URI: {NEO4J_URI}, User: {NEO4J_USER}")
        raise HTTPException(status_code=503, detail=f"Neo4j authentication error: {e}")

# --- подсчет семестра ---
def get_semester_date_range(year: int, semester: int):

    if semester % 2 == 1:  # нечет (1, 3, 5, 7)
        start_date = f"{year}-09-01"
        end_date = f"{year}-12-31"
    else:  # чет (2, 4, 6, 8) are Spring
        start_date = f"{year}-01-01"
        end_date = f"{year}-06-30"
    return start_date, end_date

# --- вывод ---
@app.get("/course-requirements", response_model=List[CourseRequirementItem])
async def get_course_requirements(
    course_name: str = Query(..., description="Название лекционного курса"),
    semester: int = Query(..., ge=1, le=8, description="Номер семестра (1-8)"),
    year: int = Query(..., description="Год обучения")
):
    report_data = []
    
    try:
        if semester < 1 or semester > 8:
            raise HTTPException(status_code=400, detail="число семестра от 1 до 8.")
        if year < 2020 or year > 2030:
            raise HTTPException(status_code=400, detail="год от 2020 до 2030.")
        start_date, end_date = get_semester_date_range(year, semester)
        parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
        parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
        if parsed_start_date > parsed_end_date:
            raise HTTPException(status_code=400, detail="Invalid date range for semester.")
        
        sql_start_datetime = f"{start_date} 00:00:00"
        sql_end_datetime = f"{end_date} 23:59:59"
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format in semester calculation.")

    # 1) получаем курсы и лекции из постгреса
    pg_conn = None
    course_data = []
    lecture_ids = []
    try:
        pg_conn = get_pg_connection()
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql_query = """
            SELECT
                c.id AS course_id,
                c.name AS course_name,
                l.id AS lecture_id,
                l.name AS lecture_topic,
                l.text_requirements AS tech_requirements,
                s.auditorium,
                s.capacity AS current_capacity
            FROM courses c
            JOIN lectures l ON l.id_course = c.id
            JOIN schedule s ON s.id_lect = l.id
            WHERE c.name ILIKE %(course_name)s
              AND s.id IN (
                  SELECT sch.id
                  FROM schedule sch
                  JOIN visits v ON v.id_schedule = sch.id
                  WHERE v.visitTime BETWEEN %(start_datetime)s AND %(end_datetime)s
              )
            ORDER BY l.id;
            """
            params = {
                "course_name": f"%{course_name}%",
                "start_datetime": sql_start_datetime,
                "end_datetime": sql_end_datetime
            }
            cur.execute(sql_query, params)
            course_data = cur.fetchall()
            lecture_ids = [row['lecture_id'] for row in course_data]
            if not course_data:
                return []
            print(f"PostgreSQL found {len(course_data)} lectures for course: {course_name}")
    except psycopg2.Error as e:
        print(f"PostgreSQL query error: {e}")
        if pg_conn:
            pg_conn.rollback()
        raise HTTPException(status_code=500, detail=f"PostgreSQL query error: {e}")
    finally:
        if pg_conn:
            pg_conn.close()

    # 2: получаем кол-во студентов из neo4j
    neo4j_driver = None
    student_counts = {}
    try:
        neo4j_driver = get_neo4j_driver()
        with neo4j_driver.session(database="neo4j") as session:
            cypher_query = """
            MATCH (g:Group)-[att:ATTENDED]->(l:Lecture)
            WHERE l.id IN $lecture_ids
            AND datetime(att.visitTime) >= datetime($start_date)
            AND datetime(att.visitTime) <= datetime($end_date)
            MATCH (s:Student)-[:BELONGS_TO]->(g)
            RETURN l.id AS lecture_id, count(DISTINCT s) AS student_count
            """
            result = session.run(cypher_query, 
                                lecture_ids=lecture_ids,
                                start_date=start_date,
                                end_date=end_date)
            student_counts = {record["lecture_id"]: record["student_count"] for record in result}
            print(f"Neo4j found student counts: {student_counts}")
    except neo4j_exceptions.Neo4jError as e:
        print(f"Neo4j query error: {e}")
        raise HTTPException(status_code=500, detail=f"Neo4j query error: {e}")
    finally:
        if neo4j_driver:
            neo4j_driver.close()

    for row in course_data:
        lecture_id = row["lecture_id"]
        student_count = student_counts.get(lecture_id, 0)
        is_suitable = student_count <= row["current_capacity"] if student_count > 0 else False
        report_data.append(
            CourseRequirementItem(
                course_id=row["course_id"],
                course_name=row["course_name"],
                lecture_topic=row["lecture_topic"],
                tech_requirements=row["tech_requirements"] or "Нет требований",
                student_count=student_count,
                current_capacity=row["current_capacity"],
                is_suitable=is_suitable,
                semester=semester,
                year=year
            )
        )

    return report_data

@app.get("/")
async def root():
    return {"message": "Lab2 Service is running. Use /course-requirements for the report."}