import json
import os
from datetime import datetime
from typing import List

import psycopg2
import redis
from fastapi import FastAPI, HTTPException, Query
from neo4j import GraphDatabase
from neo4j import exceptions as neo4j_exceptions
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field

# --- Database Configuration ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mirea")
POSTGRES_DB = os.getenv("POSTGRES_DB", "university")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

NEO4J_URI = f"bolt://{os.getenv('NEO4J_HOST', 'neo4j')}:7687"
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mireamirea")

app = FastAPI(title="Lab3 Service")

# --- Response Model ---
class GroupAttendanceItem(BaseModel):
    group_name: str = Field(..., description="Название группы")
    student_name: str = Field(..., description="ФИО студента")
    course_name: str = Field(..., description="Название лекционного курса")
    planned_hours: int = Field(..., description="Запланированное количество часов")
    attended_hours: int = Field(..., description="Посещенные часы")
    department: str = Field(..., description="Кафедра")
    date_of_admission: str = Field(..., description="Дата поступления студента (YYYY-MM-DD)")

# --- Database Connections ---
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port="5432",
            cursor_factory=RealDictCursor
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

# --- Main Endpoint ---
@app.get("/group", response_model=List[GroupAttendanceItem])
async def get_group_attendance(
    group_name: str = Query(..., description="Название группы")
):
    report_data = []

    # Step 1: Get group ID and special lectures (requirements = true) from PostgreSQL
    pg_conn = None
    group_id = None
    lecture_data = []
    try:
        pg_conn = get_pg_connection()
        with pg_conn.cursor() as cur:
            # Get group ID
            cur.execute(
                """
                SELECT g.id, k.name as department_name
                FROM groups g
                JOIN kafedras k ON g.id_kafedra = k.id
                WHERE g.name = %s
                """,
                (group_name,)
            )
            group_result = cur.fetchone()
            if not group_result:
                raise HTTPException(status_code=404, detail=f"Group {group_name} not found")
            group_id = group_result["id"]
            department_name = group_result["department_name"]

            # Get special lectures (requirements = true) and their courses
            cur.execute(
                """
                SELECT c.id as course_id, c.name as course_name, c.planned_hours, l.id as lecture_id
                FROM courses c
                JOIN lectures l ON l.id_course = c.id
                WHERE l.requirements = true
                """
            )
            lecture_data = cur.fetchall()
            if not lecture_data:
                return []
            print(f"PostgreSQL found {len(lecture_data)} special lectures for group {group_name}")
    except psycopg2.Error as e:
        print(f"PostgreSQL query error: {e}")
        if pg_conn:
            pg_conn.rollback()
        raise HTTPException(status_code=500, detail=f"PostgreSQL query error: {e}")
    finally:
        if pg_conn:
            pg_conn.close()

    lecture_ids = [row["lecture_id"] for row in lecture_data]
    course_info = {row["course_id"]: {
        "course_name": row["course_name"],
        "planned_hours": row["planned_hours"]
    } for row in lecture_data}

    # Step 2: Get students and schedule data from Neo4j
    neo4j_driver = None
    student_schedule_data = []
    try:
        neo4j_driver = get_neo4j_driver()
        with neo4j_driver.session(database="neo4j") as session:
            cypher_query = """
            MATCH (s:Student)-[:BELONGS_TO]->(g:Group {id: $group_id})
            MATCH (g)-[att:ATTENDED]->(l:Lecture)
            WHERE l.id IN $lecture_ids
            RETURN s.id AS student_id, l.id AS lecture_id, att.id_schedule AS schedule_id
            """
            result = session.run(cypher_query, group_id=group_id, lecture_ids=lecture_ids)
            student_schedule_data = [(record["student_id"], record["lecture_id"], record["schedule_id"]) for record in result]
            if not student_schedule_data:
                return []
            print(f"Neo4j found {len(student_schedule_data)} student-schedule records")
    except neo4j_exceptions.Neo4jError as e:
        print(f"Neo4j query error: {e}")
        raise HTTPException(status_code=500, detail=f"Neo4j query error: {e}")
    finally:
        if neo4j_driver:
            neo4j_driver.close()

    # Step 3: Calculate attendance in PostgreSQL
    pg_conn = None
    student_attendance_raw = {} # { (student_id, schedule_id): attended_hours }
    try:
        pg_conn = get_pg_connection()
        with pg_conn.cursor() as cur:
            # Собираем уникальные пары (student_id, schedule_id)
            unique_student_schedule_pairs = list(set((s_id, sch_id) for s_id, _, sch_id in student_schedule_data))

            if unique_student_schedule_pairs:
                values_placeholder = ','.join(cur.mogrify("(%s, %s)", (s_id, sch_id)).decode('utf-8')
                                            for s_id, sch_id in unique_student_schedule_pairs)

                pg_query_visits = f"""
                SELECT id_student, id_schedule, COUNT(*) AS attended_hours
                FROM visits
                WHERE (id_student, id_schedule) IN ({values_placeholder})
                AND status IN ('presence', 'late')
                GROUP BY id_student, id_schedule;
                """
                cur.execute(pg_query_visits)
                for row in cur.fetchall():
                    student_attendance_raw[(row["id_student"], row["id_schedule"])] = row["attended_hours"]
            else:
                print("No unique student-schedule pairs to query attendance for.")

        # Теперь агрегируем по lecture_id
        student_attendance = {} # { student_id: { course_id: total_attended_hours_for_course } }
        for student_id, lecture_id, schedule_id in student_schedule_data:
            # Находим соответствующий course_id для lecture_id
            current_course_id = next((data["course_id"] for data in lecture_data if data["lecture_id"] == lecture_id), None)

            if current_course_id:
                attended_hours_for_this_schedule = student_attendance_raw.get((student_id, schedule_id), 0)

                if student_id not in student_attendance:
                    student_attendance[student_id] = {}
                if current_course_id not in student_attendance[student_id]:
                    student_attendance[student_id][current_course_id] = 0

                # Добавляем часы, полученные для этой конкретной связки (студент, расписание)
                student_attendance[student_id][current_course_id] += attended_hours_for_this_schedule

    except psycopg2.Error as e:
        print(f"PostgreSQL query error during attendance calculation: {e}")
        if pg_conn:
            pg_conn.rollback()
        raise HTTPException(status_code=500, detail=f"PostgreSQL query error: {e}")
    finally:
        if pg_conn:
            pg_conn.close()

    # Step 4: Get student details and date_of_admission from Redis
    redis_client = None
    student_info = {}
    try:
        redis_client = get_redis_client()
        pg_conn = None
        try:
            pg_conn = get_pg_connection()
            with pg_conn.cursor() as cur:
                for student_id in student_attendance.keys():
                    # Попытка получить данные студента из Redis
                    key = f"student:{student_id}"
                    student_data = redis_client.get(key)
                    if student_data:
                        student_info[student_id] = json.loads(student_data)
                    else:
                        student_info[student_id] = {"fio": "Unknown"}

                    # Получаем date_of_admission из Redis
                    date_key = f"student:{student_id}:date_of_admission"
                    date_of_admission = redis_client.get(date_key)
                    if not date_of_admission:
                        # Если данных нет в Redis, получаем из PostgreSQL и кэшируем
                        cur.execute("SELECT date_of_admission FROM students WHERE id = %s", (student_id,))
                        db_result = cur.fetchone()
                        date_of_admission = db_result["date_of_admission"].strftime("%Y-%m-%d") if db_result else "Unknown"
                        redis_client.setex(date_key, 3600, date_of_admission)  # Кэшируем на 1 час
                    student_info[student_id]["date_of_admission"] = date_of_admission
            print(f"Retrieved {len(student_info)} student records with date_of_admission")
        except psycopg2.Error as e:
            print(f"PostgreSQL query error for date_of_admission: {e}")
            if pg_conn:
                pg_conn.rollback()
            raise HTTPException(status_code=500, detail=f"PostgreSQL query error: {e}")
        finally:
            if pg_conn:
                pg_conn.close()
    except redis.exceptions.RedisError as e:
        print(f"Redis query error: {e}")
        raise HTTPException(status_code=500, detail=f"Redis query error: {e}")
    finally:
        if redis_client:
            redis_client.close()

    # Step 5: Compile report data
    for student_id, courses in student_attendance.items():
        student_name = student_info.get(student_id, {}).get("fio", "Unknown")
        date_of_admission = student_info.get(student_id, {}).get("date_of_admission", "Unknown")
        for course_id, attended_hours in courses.items():
            if course_id in course_info:
                report_data.append(
                    GroupAttendanceItem(
                        group_name=group_name,
                        student_name=student_name,
                        course_name=course_info[course_id]["course_name"],
                        planned_hours=course_info[course_id]["planned_hours"],
                        attended_hours=attended_hours or 0,
                        department=department_name,
                        date_of_admission=date_of_admission
                    )
                )

    print(f"Generated report with {len(report_data)} entries")
    return report_data

# --- Root Endpoint ---
@app.get("/")
async def root():
    return {"message": "Lab3 Service is running. Use /group for the report."}