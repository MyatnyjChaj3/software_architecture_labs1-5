import os
from datetime import datetime
from typing import Any, List, Optional

import psycopg2
import redis
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_exceptions
from fastapi import FastAPI, HTTPException, Query
from neo4j import GraphDatabase
from neo4j import exceptions as neo4j_exceptions
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field

# --- Подключение к бд ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mirea")
POSTGRES_DB = os.getenv("POSTGRES_DB", "university")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

NEO4J_URI = f"bolt://{os.getenv('NEO4J_HOST', 'neo4j')}:7687"
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mireamirea")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = int(os.getenv("ELASTICSEARCH_PORT", 9200))
ES_INDEX = "materials"

app = FastAPI(title="Lab1 Service")

# --- Ответ ---
class AttendanceReportItem(BaseModel):
    student_id: int = Field(..., description="ID студента")
    full_name: str = Field(..., description="ФИО студента")
    group_name: str = Field(..., description="Название группы")
    department: str = Field(..., description="Название кафедры")
    course_name: str = Field(..., description="Название курса")
    attendance_percentage: Optional[float] = Field(None, description="Процент посещаемости")
    period_start: str = Field(..., description="Начало периода")
    period_end: str = Field(..., description="Конец периода")
    matching_term: str = Field(..., description="Искомый термин")
    date_of_admission: str = Field(..., description="Дата поступления студента (YYYY-MM-DD)")

# --- БД ---
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

def get_redis_client():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
        raise HTTPException(status_code=503, detail=f"Redis connection error: {e}")

def get_es_client():
    try:
        es = Elasticsearch(hosts=[{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}])
        if not es.ping():
            raise ConnectionError("Elasticsearch ping failed")
        return es
    except ConnectionError as e:
        print(f"Error connecting to Elasticsearch: {e}")
        raise HTTPException(status_code=503, detail=f"Elasticsearch connection error: {e}")

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

@app.get("/visits", response_model=List[AttendanceReportItem])
async def generate_attendance_report(
    term: str = Query(..., description="Термин для поиска в описании курса"),
    start_date: str = Query(..., description="Дата начала периода (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Дата окончания периода (YYYY-MM-DD)"),
):
    lecture_ids = []
    student_ids_from_neo4j = []
    report_data = []

    try:
        parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
        parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
        if parsed_start_date > parsed_end_date:
            raise HTTPException(status_code=400, detail="Start date cannot be after end date.")
        
        sql_start_datetime = f"{start_date} 00:00:00"
        sql_end_datetime = f"{end_date} 23:59:59"

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    # 1. Берем ID лекций из Elasticsearch
    es_client = None
    try:
        es_client = get_es_client()
        es_query = {
            "query": {"match": {"lecture_text": term}},
            "_source": ["id_lect"],
            "size": 1000
        }
        res = es_client.search(index=ES_INDEX, body=es_query)
        lecture_ids = list(set([hit["_source"]["id_lect"] for hit in res["hits"]["hits"]]))
        if not lecture_ids:
            return []
        print(f"Elasticsearch found lecture_ids: {lecture_ids}")
    except es_exceptions.NotFoundError:
         print(f"Elasticsearch index '{ES_INDEX}' not found.")
         return []
    except Exception as e:
        print(f"Elasticsearch query error: {e}")
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")
    finally:
        if es_client:
            try:
                pass
            except Exception as e_close:
                print(f"Error closing Elasticsearch client: {e_close}")

    # 2. Получаем ID студентов из Neo4j
    neo4j_driver = None
    try:
        neo4j_driver = get_neo4j_driver()
        with neo4j_driver.session(database="neo4j") as session: 
            cypher_query = """
            MATCH (s:Student)-[:BELONGS_TO]->(g:Group)-[att:ATTENDED]->(l:Lecture)
            WHERE l.id IN $lecture_ids
            RETURN DISTINCT s.id AS student_id
            """
            result = session.run(cypher_query, lecture_ids=lecture_ids)
            student_ids_from_neo4j = [record["student_id"] for record in result]
            if not student_ids_from_neo4j:
                return []
        print(f"Neo4j found student_ids: {student_ids_from_neo4j}")
    except Exception as e:
        print(f"Neo4j query error: {e}")
        raise HTTPException(status_code=500, detail=f"Neo4j query error: {e}")
    finally:
        if neo4j_driver:
            neo4j_driver.close()

    # 3. Получаем данные о посещаемости из PostgreSQL
    pg_conn = None
    redis_client = None
    try:
        pg_conn = get_pg_connection()
        redis_client = get_redis_client()
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql_query = """
            WITH student_lecture_visits AS (
                SELECT
                    s.id AS student_id,
                    s.fio AS full_name,
                    g.name AS group_name,
                    k.name AS department_name,
                    c.name AS course_name,
                    l.id AS lecture_id,
                    l.name AS lecture_name,
                    COUNT(v.id) AS total_visits,
                    SUM(CASE WHEN v.status IN ('presence', 'late') THEN 1 ELSE 0 END) AS attended_visits
                FROM students s
                JOIN groups g ON s.id_group = g.id
                JOIN kafedras k ON g.id_kafedra = k.id
                JOIN visits v ON s.id = v.id_student
                JOIN schedule sch ON v.id_schedule = sch.id
                JOIN lectures l ON sch.id_lect = l.id
                JOIN courses c ON l.id_course = c.id
                WHERE s.id = ANY(%(student_ids)s)
                AND l.id = ANY(%(lecture_ids)s)
                AND v.visitTime BETWEEN %(start_datetime)s AND %(end_datetime)s
                GROUP BY s.id, s.fio, g.name, k.name, c.name, l.id, l.name
            )
            SELECT
                student_id,
                full_name,
                group_name,
                department_name,
                course_name,
                lecture_name,
                (attended_visits::FLOAT * 100.0 / NULLIF(total_visits, 0)) AS attendance_percentage
            FROM student_lecture_visits
            ORDER BY attendance_percentage ASC NULLS LAST
            LIMIT 10;
"""
            params = {
                "student_ids": student_ids_from_neo4j,
                "lecture_ids": lecture_ids,
                "start_datetime": sql_start_datetime,
                "end_datetime": sql_end_datetime,
            }
            cur.execute(sql_query, params)
            results = cur.fetchall()
            print(f"PostgreSQL results: {results}")

            for row in results:
                redis_key = f"student:{row['student_id']}:date_of_admission"
                date_of_admission = redis_client.get(redis_key)
                if not date_of_admission:
                    cur.execute("SELECT date_of_admission FROM students WHERE id = %s", (row["student_id"],))
                    db_result = cur.fetchone()
                    date_of_admission = db_result["date_of_admission"].strftime("%Y-%m-%d") if db_result else "Unknown"
                    redis_client.setex(redis_key, 3600, date_of_admission)

                report_data.append(
                    AttendanceReportItem(
                        student_id=row["student_id"],
                        full_name=row["full_name"],
                        group_name=row["group_name"],
                        department=row["department_name"],
                        course_name=row["course_name"],
                        attendance_percentage=row["attendance_percentage"],
                        period_start=start_date,
                        period_end=end_date,
                        matching_term=term,
                        date_of_admission=date_of_admission
                    )
                )
        return report_data
    except psycopg2.Error as e:
        print(f"PostgreSQL query error: {e}")
        pg_conn.rollback()
        raise HTTPException(status_code=500, detail=f"PostgreSQL query error: {e}")
    except redis.exceptions.RedisError as e:
        print(f"Redis query error: {e}")
        raise HTTPException(status_code=500, detail=f"Redis query error: {e}")
    except Exception as e:
        print(f"Unexpected error during processing: {e}")
        if pg_conn: pg_conn.rollback()
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
    finally:
        if pg_conn:
            pg_conn.close()
        if redis_client:
            redis_client.close()

@app.get("/")
async def root():
    return {"message": "Lab1 Service is running. Use /visits for the report."}