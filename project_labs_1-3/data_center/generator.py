import json
import os
import random
import time
from datetime import date, datetime, timedelta

import psycopg2
import redis
from elasticsearch import Elasticsearch, helpers
from faker import Faker
from faker.providers import BaseProvider
from neo4j import GraphDatabase
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_values
from pymongo import MongoClient


# Кастомные названия
class UniversityProvider(BaseProvider):
    def university_name(self):
        return "РТУ МИРЭА"

class InstituteProvider(BaseProvider):
    def institute_name(self):
        institutes = [
            "Институт перспективных технологий и индустриального программирования",
            "Институт информационных технологий",
            "Институт искусственного интеллекта",
            "Институт кибербезопасности и цифровых технологий",
            "Институт радиоэлектроники и информатики",
            "Институт тонких химических технологий им. М. В. Ломоносова",
            "Институт технологий управления",
            "Институт молодёжной политики и международных отношений",
            "Институт международного образования"
        ]
        return random.choice(institutes)

class DepartmentProvider(BaseProvider):
    def department_name(self):
        prefixes = ['Кафедра', 'Базовая кафедра', 'Специализированная кафедра']
        fields = [
            'Программной инженерии', 'Кибербезопасности', 'Искусственного интеллекта',
            'Радиоэлектроники', 'Химических технологий', 'Управления проектами',
            'Международных отношений', 'Информатики', 'Цифровых технологий',
            'Машинного обучения', 'Электроники', 'Сетевых технологий'
        ]
        return f"{random.choice(prefixes)} {random.choice(fields)}"

class SpecialtyProvider(BaseProvider):
    def specialty(self):
        specialties = [
            ("09.03.01", "Информатика и вычислительная техника"),
            ("09.03.02", "Информационные системы и технологии"),
            ("09.03.03", "Прикладная информатика"),
            ("09.03.04", "Программная инженерия"),
            ("10.03.01", "Информационная безопасность"),
            ("11.03.01", "Радиотехника"),
            ("11.03.02", "Инфокоммуникационные технологии"),
            ("11.03.03", "Конструирование радиоэлектронных средств"),
            ("12.03.01", "Приборостроение"),
            ("15.03.01", "Машиностроение"),
            ("15.03.04", "Автоматизация технологических процессов"),
            ("15.03.05", "Конструкторско-технологическое обеспечение"),
            ("18.03.01", "Химическая технология"),
            ("18.03.02", "Энерго- и ресурсосберегающие процессы"),
            ("27.03.01", "Стандартизация и метрология"),
            ("27.03.04", "Управление в технических системах"),
            ("38.03.01", "Экономика"),
            ("38.03.02", "Менеджмент"),
            ("38.03.05", "Бизнес-информатика"),
            ("40.03.01", "Юриспруденция"),
            ("41.03.05", "Международные отношения"),
            ("42.03.01", "Реклама и связи с общественностью"),
            ("45.03.02", "Лингвистика"),
            ("51.03.01", "Культурология"),
            ("54.03.01", "Дизайн"),
            ("01.03.02", "Прикладная математика и информатика"),
            ("01.03.04", "Прикладная математика"),
            ("02.03.03", "Математическое обеспечение и администрирование"),
            ("09.03.05", "Кибербезопасность"),
            ("10.03.02", "Информационно-аналитические системы безопасности"),
            ("11.03.04", "Электроника и наноэлектроника"),
            ("12.03.04", "Биотехнические системы и технологии"),
            ("15.03.06", "Мехатроника и робототехника"),
            ("18.03.03", "Технология материалов"),
            ("27.03.05", "Инноватика"),
            ("38.03.03", "Управление персоналом"),
            ("38.03.04", "Государственное и муниципальное управление"),
            ("40.03.02", "Правовое обеспечение национальной безопасности"),
            ("41.03.01", "Зарубежное регионоведение"),
            ("45.03.01", "Филология")
        ]
        return random.choice(specialties)

class GroupProvider(BaseProvider):
    def group_name(self):
        letters = ''.join(random.choice('АБВГДЕЖЗИКЛМНОПРСТУФХЦЧШЩЭЮЯ') for _ in range(4))
        number = random.randint(0, 99)
        year = random.randint(20, 25)
        return f"{letters}-{number:02d}-{year:02d}"

fake = Faker('ru_RU')
fake.add_provider(UniversityProvider)
fake.add_provider(InstituteProvider)
fake.add_provider(DepartmentProvider)
fake.add_provider(SpecialtyProvider)
fake.add_provider(GroupProvider)

# Подключение к базам данных
pg_conn = psycopg2.connect(
    host="postgres", port="5432", database="university",
    user="postgres", password="mirea"
)
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
es = Elasticsearch(hosts=["http://elasticsearch:9200"])
ES_INDEX = "materials"
mongo_client = MongoClient("mongodb://admin:mirea@mongodb:27017/")
mongo_db = mongo_client["university"]
neo4j_driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "mireamirea"))

# Генерация случайных дат
def generate_random_date(start_date, end_date):
    days_between = (end_date - start_date).days
    times = ['09:00', '11:00', '13:00', '15:00']
    random_days = random.randint(0, days_between)
    random_date = start_date + timedelta(days=random_days)
    random_time = random.choice(times)
    random_datetime = datetime.strptime(f"{random_date.strftime('%Y-%m-%d')} {random_time}", '%Y-%m-%d %H:%M')
    return random_datetime

def get_week_start(input_date):
    if isinstance(input_date, str):
        try:
            given_date = datetime.strptime(input_date, '%Y-%m-%d %H:%M:%S').date()
        except ValueError:
            given_date = datetime.strptime(input_date, '%Y-%m-%d').date()
    else:
        given_date = input_date.date() if isinstance(input_date, datetime) else input_date
    day_of_week = given_date.isoweekday()
    delta = timedelta(days=day_of_week - 1)
    monday_date = given_date - delta
    return monday_date

start_date_semester = datetime.strptime("2025-01-10", "%Y-%m-%d")
end_date_semester = datetime.strptime("2025-12-20", "%Y-%m-%d")

# Чтение SQL-файла
def read_sql(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Файл {filepath} не найден")
    with open(filepath, 'r') as f:
        sql_queries = f.read()
    queries = [q.strip() for q in sql_queries.split(";") if q.strip()]
    return queries

# Создание таблиц
def create_tables():
    try:
        queries = read_sql("postgres.sql")
        pg_conn.autocommit = True
        with pg_conn.cursor() as cur:
            for q in queries:
                print(f"Выполняется запрос: {q}")
                cur.execute(q)
            
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_class
                    WHERE relname = 'visits' AND relkind = 'p'
                );
            """)
            is_partitioned = cur.fetchone()[0]
            print(f"Таблица visits партиционирована: {is_partitioned}")
            
            if is_partitioned:
                date_start_partition = datetime.strptime("2025-01-06", "%Y-%m-%d") # Убедимся, что это Monday
                date_end_partition = datetime.strptime("2025-12-28", "%Y-%m-%d") # Убедимся, что это Sunday + 1 day for range

                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_name LIKE 'visits_2025_%';
                """)
                existing_partitions = cur.fetchall()
                for part in existing_partitions:
                    print(f"Удаление существующей партиции: {part[0]}")
                    cur.execute(f"DROP TABLE IF EXISTS {part[0]} CASCADE;")

                i = 1
                current_partition_start_date = date_start_partition
                while current_partition_start_date < date_end_partition:
                    start_date_str = current_partition_start_date.strftime('%Y-%m-%d')
                    next_partition_start_date = current_partition_start_date + timedelta(days=7)
                    end_date_str = next_partition_start_date.strftime('%Y-%m-%d')
                    
                    partition_name = f"visits_2025_w{i}"
                    print(f"Создание партиции {partition_name}: FOR VALUES FROM ('{start_date_str}') TO ('{end_date_str}')")
                    cur.execute(
                        f"CREATE TABLE {partition_name} PARTITION OF visits "
                        f"FOR VALUES FROM ('{start_date_str}') TO ('{end_date_str}');"
                    )
                    current_partition_start_date = next_partition_start_date
                    i += 1
            
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'institutes');")
            exists = cur.fetchone()[0]
            print(f"Таблица institutes существует: {exists}")
        print("Таблицы и партиции успешно созданы или уже существовали")
    except psycopg2.Error as e:
        print(f"Ошибка psycopg2 при создании таблиц: {e}")
        raise
    except Exception as e:
        print(f"Общая ошибка при создании таблиц: {e}")
        raise
    finally:
        pg_conn.autocommit = False

def clear_all_data():
    try:
        # Очистка Redis
        redis_client.flushdb()
        print("Redis очищен")

        # Очистка Elasticsearch
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                if es.indices.exists(index=ES_INDEX):
                    es.indices.delete(index=ES_INDEX)
                    print(f"Elasticsearch индекс '{ES_INDEX}' удалён")
                else:
                    print(f"Elasticsearch индекс '{ES_INDEX}' не существует")
                break
            except Exception as e:
                if attempt < max_attempts - 1:
                    print(f"Попытка {attempt + 1} подключения к Elasticsearch не удалась: {e}")
                    time.sleep(5)
                else:
                    print(f"Не удалось подключиться к Elasticsearch после {max_attempts} попыток: {e}")

        # Очистка MongoDB
        mongo_db["universities"].delete_many({})
        print("MongoDB коллекция 'universities' очищена")

        # Очистка Neo4j
        with neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Neo4j очищен")

        # Очистка PostgreSQL
        pg_conn.autocommit = True
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name LIKE 'visits_2025_w%';
            """)
            partitions = cur.fetchall()
            for partition in partitions:
                partition_name = partition[0]
                cur.execute(f"DROP TABLE IF EXISTS {partition_name} CASCADE;")
                print(f"Удалена партиция {partition_name}")

            cur.execute("""
                DROP TABLE IF EXISTS visits CASCADE;
                DROP TABLE IF EXISTS schedule CASCADE;
                DROP TABLE IF EXISTS materials CASCADE;
                DROP TABLE IF EXISTS lectures CASCADE;
                DROP TABLE IF EXISTS courses CASCADE;
                DROP TABLE IF EXISTS students CASCADE;
                DROP TABLE IF EXISTS groups CASCADE;
                DROP TABLE IF EXISTS kafedra_specialties CASCADE;
                DROP TABLE IF EXISTS specialties CASCADE;
                DROP TABLE IF EXISTS kafedras CASCADE;
                DROP TABLE IF EXISTS institutes CASCADE;
                DROP TABLE IF EXISTS universities CASCADE;
            """)
            print("Все таблицы в PostgreSQL удалены")
    except Exception as e:
        print(f"Ошибка при очистке данных: {e}")
    finally:
        pg_conn.autocommit = False

# Функции вставки данных
def insert_universities(cur):
    university = {"name": fake.university_name()}
    sql_query = "INSERT INTO universities (name) VALUES (%s) RETURNING id, name;"
    cur.execute(sql_query, (university["name"],))
    fetched_universities = cur.fetchall()
    return [{"id": r[0], "name": r[1]} for r in fetched_universities]

def insert_institutes(cur, university_id):
    institutes_props = [{"name": fake.institute_name(), "id_university": university_id} for _ in range(9)]
    values = [(inst["name"], inst["id_university"]) for inst in institutes_props]
    sql_query = "INSERT INTO institutes (name, id_university) VALUES %s RETURNING id, name, id_university;"
    try:
        results_tuples = execute_values(cur, sql_query, values, fetch=True)
        print(f"Вставлено институтов: {len(results_tuples)}")
        return [{"id": r[0], "name": r[1], "id_university": r[2]} for r in results_tuples]
    except Exception as e:
        print(f"Ошибка в insert_institutes: {e}")
        raise

def insert_kafedras(cur, institutes):
    kafedras_props = []
    for inst in institutes:
        inst_id = inst["id"]
        for _ in range(3):
            kafedras_props.append({"name": fake.department_name(), "id_institutes": inst_id})
    
    values = [(k["name"], k["id_institutes"]) for k in kafedras_props]
    sql_query = "INSERT INTO kafedras (name, id_institutes) VALUES %s RETURNING id, name, id_institutes;"
    results_tuples = execute_values(cur, sql_query, values, fetch=True)
    print(f"Вставлено кафедр: {len(results_tuples)}")
    return [{"id": r[0], "name": r[1], "id_institutes": r[2]} for r in results_tuples]

def insert_specialties(cur):
    specialties_props = [fake.specialty() for _ in range(40)]
    values = [(s[1], s[0]) for s in specialties_props]
    sql_query = "INSERT INTO specialties (name, code) VALUES %s RETURNING id, name, code;"
    results_tuples = execute_values(cur, sql_query, values, fetch=True)
    print(f"Вставлено специальностей: {len(results_tuples)}")
    return [{"id": r[0], "name": r[1], "code": r[2]} for r in results_tuples]

def insert_kafedra_specialties(cur, kafedras, specialties):
    values = []
    for kaf in kafedras:
        if not specialties: continue
        num_selected = random.randint(1, min(5, len(specialties)))
        selected_specs = random.sample(specialties, num_selected)
        for spec in selected_specs:
            values.append((kaf["id"], spec["id"]))
    if not values:
        print("Нет данных для вставки в kafedra_specialties.")
        return
    sql_query = "INSERT INTO kafedra_specialties (id_kafedra, id_specialty) VALUES %s;"
    execute_values(cur, sql_query, values)

def insert_groups(cur, kafedras):
    groups_props = []
    start_year = datetime.strptime("2021-09-01", "%Y-%m-%d").date()

    for _ in range(100):
        kaf = random.choice(kafedras)

        current_start_year = start_year.year - random.randint(0,3)
        current_end_year = current_start_year + 4
        
        group_start_date = date(current_start_year, 9, 1)
        group_end_date = date(current_end_year, 7, 1)

        groups_props.append({
            "name": fake.group_name(),
            "id_kafedra": kaf["id"],
            "startYear": group_start_date,
            "endYear": group_end_date
        })
    values = [(g["name"], g["id_kafedra"], g["startYear"], g["endYear"]) for g in groups_props]
    sql_query = "INSERT INTO groups (name, id_kafedra, startYear, endYear) VALUES %s RETURNING id, name, id_kafedra;"
    results_tuples = execute_values(cur, sql_query, values, fetch=True)
    print(f"Вставлено групп: {len(results_tuples)}")
    
    final_groups_list = []
    if len(results_tuples) == len(groups_props):
        for i, r_tuple in enumerate(results_tuples):
            final_groups_list.append({"id": r_tuple[0], "name": r_tuple[1], "id_kafedra": r_tuple[2]})
    else:
        final_groups_list = [{"id": r_tuple[0], "name": r_tuple[1], "id_kafedra": r_tuple[2]} for r_tuple in results_tuples]
        print(f"Предупреждение: Несоответствие количества вставленных групп. Ожидалось {len(groups_props)}, вставлено {len(results_tuples)}")

    return final_groups_list


def insert_students(cur, groups):
    students_props = []
    admission_date_base = datetime.strptime("2021-09-01", "%Y-%m-%d").date()
    total_students = 1500
    
    if not groups:
        print("Нет групп для добавления студентов.")
        return []

    students_per_group = total_students // len(groups) if len(groups) > 0 else 0
    remaining_students = total_students % len(groups) if len(groups) > 0 else 0
    
    current_student_count = 0
    for i, grp in enumerate(groups):
        group_id = grp["id"]
        group_start_date = grp.get("startYear", admission_date_base)

        num_students_in_this_group = students_per_group + (1 if i < remaining_students else 0)
        
        for _ in range(num_students_in_this_group):
            students_props.append({
                "fio": fake.name(),
                "id_group": group_id,
                "date_of_admission": group_start_date,
            })
            current_student_count +=1
            if current_student_count >= total_students: break
        if current_student_count >= total_students: break

    if not students_props:
        print("Нет студентов для вставки.")
        return []

    values = [(s["fio"], s["id_group"], s["date_of_admission"]) for s in students_props]
    sql_query = "INSERT INTO students (fio, id_group, date_of_admission) VALUES %s RETURNING id;"
    
    returned_ids_tuples = execute_values(cur, sql_query, values, fetch=True)
    print(f"Вставлено студентов: {len(returned_ids_tuples)}")

    if len(returned_ids_tuples) == len(students_props):
        for i, id_tuple in enumerate(returned_ids_tuples):
            students_props[i]["id"] = id_tuple[0]
    '''
    else:
        print(f"Предупреждение: Количество возвращенных ID студентов ({len(returned_ids_tuples)}) не совпадает с количеством для вставки ({len(students_props)})")
        updated_students_list = []
        temp_students_map = { (s["fio"], s["id_group"], s["date_of_admission"]): s for s in students_props}
    '''
    return students_props

def insert_courses(cur, kafedras, specialties):
    courses_props = []
    for _ in range(100):
        if not kafedras or not specialties: break
        kaf = random.choice(kafedras)
        spec = random.choice(specialties)
        courses_props.append({
            "name": fake.job(),
            "id_kafedra": kaf["id"],
            "id_specialty": spec["id"],
            "planned_hours": random.randint(36, 144)
        })
    if not courses_props:
        print("Нет курсов для вставки.")
        return []
        
    values = [(c["name"], c["id_kafedra"], c["id_specialty"], c["planned_hours"]) for c in courses_props]
    sql_query = "INSERT INTO courses (name, id_kafedra, id_specialty, planned_hours) VALUES %s RETURNING id, id_kafedra, id_specialty;"
    results_tuples = execute_values(cur, sql_query, values, fetch=True)
    print(f"Вставлено курсов: {len(results_tuples)}")
    return [{"id": r[0], "id_kafedra": r[1], "id_specialty": r[2]} for r in results_tuples]


def insert_lectures(cur, courses):
    lectures_props = []
    target_lectures = 900

    if not courses:
        print("Нет курсов для создания лекций.")
        return []

    lectures_per_course = max(1, target_lectures // len(courses))
    remainder_lectures = target_lectures % len(courses)

    for i, course in enumerate(courses):
        course_id = course["id"]
        num_lectures_for_this_course = lectures_per_course + (1 if i < remainder_lectures else 0)
        
        for _ in range(num_lectures_for_this_course):
            reqs_list = [fake.word() for _ in range(random.randint(0, 3))] 
            reqs = ", ".join(reqs_list) if reqs_list else None
            lectures_props.append({
                "name": f"Лекция: {fake.catch_phrase().title()}",
                "id_course": course_id,
                "duration_hours": 2, # По условию таблицы
                "requirements": random.choice([True, False]),
                "text_requirements": reqs
            })
    
    if not lectures_props:
        print("Нет лекций для вставки.")
        return []

    values = [(l["name"], l["id_course"], l["duration_hours"], l["requirements"], l["text_requirements"]) for l in lectures_props]
    sql_query = "INSERT INTO lectures (name, id_course, duration_hours, requirements, text_requirements) VALUES %s RETURNING id, id_course;"
    results_tuples = execute_values(cur, sql_query, values, fetch=True)
    print(f"Вставлено лекций: {len(results_tuples)}")
    return [{"id": r[0], "id_course": r[1]} for r in results_tuples]

def insert_materials(cur, lectures):
    materials_props = []
    
    if not lectures:
        print("Нет лекций для создания материалов.")
        return []

    for lec in lectures:
        lec_id = lec["id"]
        num_materials = random.randint(1, 2)
        for _ in range(num_materials):
            materials_props.append({
                "name": f"Материал: {fake.bs().title()}",
                "id_lect": lec_id
            })
            if len(materials_props) >= 1500:
                break
        if len(materials_props) >= 1500:
             break
    
    if not materials_props:
        print("Нет материалов для вставки.")
        return []
        
    values = [(m["name"], m["id_lect"]) for m in materials_props]
    sql_query = "INSERT INTO materials (name, id_lect) VALUES %s RETURNING id, id_lect;"
    results_tuples = execute_values(cur, sql_query, values, fetch=True)
    print(f"Вставлено материалов: {len(results_tuples)}")
    return [{"id": r[0], "id_lect": r[1]} for r in results_tuples]


def insert_schedule(cur, groups, lectures):
    schedules_props = [] 
    
    if not groups or not lectures:
        print("Нет групп или лекций для создания расписания.")
        return []

    for group in groups:
        group_id = group["id"]
        num_schedules_per_group = random.randint(5, 7)
        
        available_lectures_for_group = random.sample(lectures, min(len(lectures), num_schedules_per_group * 2))

        for i in range(num_schedules_per_group):
            if not available_lectures_for_group: break
            lec = random.choice(available_lectures_for_group)
            
            schedules_props.append({
                "id_lect": lec["id"],
                "id_group": group_id,
                "auditorium": f"{random.choice('АБВГД')}-{random.randint(100, 599)}",
                "capacity": random.randint(20, 120),
            })
    
    if not schedules_props:
        print("Нет записей для вставки в расписание.")
        return []

    values = [(s["auditorium"], s["id_lect"], s["id_group"], s["capacity"]) for s in schedules_props]
    sql_query = """INSERT INTO schedule (auditorium, id_lect, id_group, capacity) VALUES %s RETURNING id;"""
    
    returned_ids_tuples = execute_values(cur, sql_query, values, fetch=True) # [(id1,), (id2,), ...]
    print(f"Вставлено записей в расписание: {len(returned_ids_tuples)}")

    if len(returned_ids_tuples) == len(schedules_props):
        for i, id_tuple in enumerate(returned_ids_tuples):
            schedules_props[i]["id"] = id_tuple[0]
    else:
        print(f"Предупреждение: Количество возвращенных ID расписаний ({len(returned_ids_tuples)}) не совпадает с количеством для вставки ({len(schedules_props)})")

    return schedules_props


def insert_visits(cur, students, schedules):
    visits_to_insert = [] # Список кортежей для execute_values
    
    if not students or not schedules:
        print("Нет студентов или расписаний для генерации посещений.")
        return 0

    group_students_map = {}
    for st in students:
        group_id = st["id_group"]
        if group_id not in group_students_map:
            group_students_map[group_id] = []
        group_students_map[group_id].append(st["id"])

    group_schedules_map = {}
    for sch in schedules:
        group_id = sch["id_group"]
        sch_id = sch["id"]
        if group_id not in group_schedules_map:
            group_schedules_map[group_id] = []
        group_schedules_map[group_id].append(sch_id)

    temp_visits_set = set()

    for group_id, student_ids_in_group in group_students_map.items():
        schedule_ids_for_group = group_schedules_map.get(group_id, [])
        if not schedule_ids_for_group:
            continue # Нет расписаний для этой группы

        for st_id in student_ids_in_group:
            num_visits_for_student = random.randint(40, 50)
            
            student_visit_count = 0
            for _ in range(num_visits_for_student):
                schedule_id_for_visit = random.choice(schedule_ids_for_group)
                visit_time = generate_random_date(start_date_semester, end_date_semester)
                week_start = get_week_start(visit_time)
                status = random.choice(['presence', 'absence', 'late'])
                
                visit_key = (st_id, schedule_id_for_visit, visit_time.isoformat())
                if visit_key not in temp_visits_set:
                    visits_to_insert.append(
                        (st_id, schedule_id_for_visit, visit_time, week_start, status)
                    )
                    temp_visits_set.add(visit_key)
                    student_visit_count +=1

    if not visits_to_insert:
        print("Нет данных для вставки в visits.")
        return 0
    
    print(f"Подготовлено {len(visits_to_insert)} уникальных записей о посещаемости для вставки.")

    # Пакетная вставка
    batch_size = 10000
    total_inserted_count = 0
    
    sql_query = """
    INSERT INTO visits (id_student, id_schedule, visitTime, week_start, status)
    VALUES %s
    ON CONFLICT DO NOTHING 
    RETURNING id; 
    """

    for i in range(0, len(visits_to_insert), batch_size):
        batch = visits_to_insert[i:i + batch_size]
        try:
            inserted_ids_this_batch = execute_values(cur, sql_query, batch, fetch=True)
            
            actual_inserted_in_batch = len(inserted_ids_this_batch)
            total_inserted_count += actual_inserted_in_batch
            
            print(f"Вставлено {actual_inserted_in_batch} записей посещаемости (в текущей пачке). Всего вставлено: {total_inserted_count}")
        except Exception as e:
            print(f"Ошибка при вставке партии посещений: {e}")
            continue
    
    print(f"Итого вставлено {total_inserted_count} записей посещаемости.")
    return total_inserted_count

# Функции отображения данных
def display_universities(cur):
    cur.execute("SELECT id, name FROM universities ORDER BY id;")
    universities = cur.fetchall()
    print("\nУниверситеты:")
    print("-" * 60)
    print(f"{'ID':<5} | {'Название':<50}")
    print("-" * 60)
    for uni in universities:
        print(f"{uni[0]:<5} | {uni[1]:<50}")
    print("-" * 60)

def display_institutes(cur):
    cur.execute("""
        SELECT i.id, i.name, u.name as university_name 
        FROM institutes i 
        JOIN universities u ON i.id_university = u.id 
        ORDER BY i.id;
    """)
    institutes = cur.fetchall()
    print("\nИнституты:")
    print("-" * 100)
    print(f"{'ID':<5} | {'Название':<50} | {'Университет':<40}")
    print("-" * 100)
    for inst in institutes:
        print(f"{inst[0]:<5} | {inst[1]:<50} | {inst[2]:<40}")
    print("-" * 100)

def display_kafedras(cur):
    cur.execute("""
        SELECT k.id, k.name, i.name as institute_name 
        FROM kafedras k 
        JOIN institutes i ON k.id_institutes = i.id 
        ORDER BY k.id;
    """)
    kafedras = cur.fetchall()
    print("\nКафедры:")
    print("-" * 100)
    print(f"{'ID':<5} | {'Название':<50} | {'Институт':<40}")
    print("-" * 100)
    for kaf in kafedras:
        print(f"{kaf[0]:<5} | {kaf[1]:<50} | {kaf[2]:<40}")
    print("-" * 100)

def display_specialties(cur):
    cur.execute("SELECT id, code, name FROM specialties ORDER BY id;")
    specialties = cur.fetchall()
    print("\nСпециальности:")
    print("-" * 100)
    print(f"{'ID':<5} | {'Код':<10} | {'Название':<40}")
    print("-" * 100)
    for spec in specialties:
        print(f"{spec[0]:<5} | {spec[1]:<10} | {spec[2]:<40}")
    print("-" * 100)

def display_courses(cur):
    cur.execute("""
        SELECT c.id, c.name, k.name as kafedra_name, s.name as specialty_name, c.planned_hours
        FROM courses c
        JOIN kafedras k ON c.id_kafedra = k.id
        JOIN specialties s ON c.id_spec = s.id
        ORDER BY c.id LIMIT 20;
    """)
    courses = cur.fetchall()
    print("\nКурсы (первые 20):")
    print("-" * 120)
    print(f"{'ID':<5} | {'Название':<30} | {'Кафедра':<30} | {'Специальность':<30} | {'Часы':<5}")
    print("-" * 120)
    for course in courses:
        print(f"{course[0]:<5} | {course[1]:<30} | {course[2]:<30} | {course[3]:<30} | {course[4]:<5}")
    print("-" * 120)

def display_lectures(cur):
    cur.execute("""
        SELECT l.id, l.name, c.name as course_name, l.duration_hours, l.requirements, l.text_requirements
        FROM lectures l
        JOIN courses c ON l.id_course = c.id
        ORDER BY l.id LIMIT 20;
    """)
    lectures = cur.fetchall()
    print("\nЛекции (первые 20):")
    print("-" * 120)
    print(f"{'ID':<5} | {'Название':<30} | {'Курс':<30} | {'Часы':<5} | {'Требования':<10} | {'Текст треб.':<30}")
    print("-" * 120)
    for lec in lectures:
        print(f"{lec[0]:<5} | {lec[1]:<30} | {lec[2]:<30} | {lec[3]:<5} | {str(lec[4]):<10} | {(lec[5] or ''):<30}")
    print("-" * 120)


def display_materials(cur):
    cur.execute("""
        SELECT m.id, m.name, l.name as lecture_name
        FROM materials m
        JOIN lectures l ON m.id_lect = l.id
        ORDER BY m.id LIMIT 20;
    """)
    materials = cur.fetchall()
    print("\nМатериалы лекций (первые 20):")
    print("-" * 80)
    print(f"{'ID':<5} | {'Название':<30} | {'Лекция':<40}")
    print("-" * 80)
    for mat in materials:
        print(f"{mat[0]:<5} | {mat[1]:<30} | {mat[2]:<40}")
    print("-" * 80)

def display_groups(cur):
    cur.execute("""
        SELECT g.id, g.name, k.name as kafedra_name, g.startYear, g.endYear
        FROM groups g
        JOIN kafedras k ON g.id_kafedra = k.id
        ORDER BY g.id LIMIT 20;
    """)
    groups = cur.fetchall()
    print("\nГруппы (первые 20):")
    print("-" * 120)
    print(f"{'ID':<5} | {'Название':<15} | {'Кафедра':<40} | {'Начало':<10} | {'Конец':<10}")
    print("-" * 120)
    for grp in groups:
        print(f"{grp[0]:<5} | {grp[1]:<15} | {grp[2]:<40} | {str(grp[3]):<10} | {str(grp[4]):<10}")
    print("-" * 120)


def display_students(cur):
    cur.execute("""
        SELECT s.id, s.fio, g.name as group_name, s.date_of_admission
        FROM students s
        JOIN groups g ON s.id_group = g.id
        ORDER BY s.id LIMIT 20;
    """)
    students = cur.fetchall()
    print("\nСтуденты (первые 20):")
    print("-" * 100)
    print(f"{'ID':<5} | {'ФИО':<30} | {'Группа':<15} | {'Дата поступления':<15}")
    print("-" * 100)
    for st in students:
        print(f"{st[0]:<5} | {st[1]:<30} | {st[2]:<15} | {str(st[3]):<15}")
    print("-" * 100)

def display_schedule(cur):
    cur.execute("""
        SELECT s.id, s.auditorium, s.capacity, g.name as group_name, l.name as lecture_name
        FROM schedule s
        JOIN groups g ON s.id_group = g.id
        JOIN lectures l ON s.id_lect = l.id
        ORDER BY s.id LIMIT 20;
    """)
    schedules = cur.fetchall()
    print("\nРасписание (первые 20 записей):")
    print("-" * 100)
    print(f"{'ID':<5} | {'Аудитория':<10} | {'Вместимость':<10} | {'Группа':<15} | {'Лекция':<30}")
    print("-" * 100)
    for sch in schedules:
        print(f"{sch[0]:<5} | {sch[1]:<10} | {sch[2]:<10} | {sch[3]:<15} | {sch[4]:<30}")
    print("-" * 100)

def display_visits(cur):
    cur.execute("""
        SELECT v.id, s.fio, g.name as group_name, l.name as lecture_name, v.visitTime, v.week_start, v.status
        FROM visits v
        JOIN students s ON v.id_student = s.id
        JOIN schedule sch ON v.id_schedule = sch.id
        JOIN groups g ON sch.id_group = g.id
        JOIN lectures l ON sch.id_lect = l.id
        ORDER BY v.id DESC, v.week_start DESC
        LIMIT 20;
    """)
    visits = cur.fetchall()
    print("\nПосещаемость:")
    print("-" * 140)
    print(f"{'ID':<5} | {'Студент':<30} | {'Группа':<15} | {'Лекция':<30} | {'Время':<20} | {'Начало недели':<15} | {'Статус':<10}")
    print("-" * 140)
    for v_row in visits: 
        visit_time_str = v_row[4].strftime('%Y-%m-%d %H:%M:%S') if isinstance(v_row[4], datetime) else str(v_row[4])
        week_start_str = v_row[5].strftime('%Y-%m-%d') if isinstance(v_row[5], date) else str(v_row[5])
        print(f"{v_row[0]:<5} | {v_row[1]:<30} | {v_row[2]:<15} | {v_row[3]:<30} | {visit_time_str:<20} | {week_start_str:<15} | {v_row[6]:<10}")
    print("-" * 140)

def display_all_data(cur):
    display_universities(cur)
    display_institutes(cur)
    display_kafedras(cur)
    display_specialties(cur)
    display_courses(cur)
    display_lectures(cur)
    display_materials(cur)
    display_groups(cur)
    display_students(cur)
    display_schedule(cur)
    display_visits(cur)

# Дублирование данных
def fetch_all(query, params=None):
    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return cur.fetchall()

def duplicate_students_to_redis():
    query = "SELECT id, fio, id_group, date_of_admission FROM students;"
    students = fetch_all(query)
    if not students:
        print("Нет студентов для дублирования в Redis.")
        return
    for student in students:
        date_adm_str = student["date_of_admission"].isoformat() if isinstance(student["date_of_admission"], (date, datetime)) else student["date_of_admission"]
        key = f"student:{student['id']}"
        value = json.dumps({
            "fio": student["fio"],
            "id_group": student["id_group"],
            "date_of_admission": date_adm_str
        }, ensure_ascii=False)
        redis_client.set(key, value)
    print(f"Дублировано {len(students)} студентов в Redis.")


def duplicate_lecture_materials_to_es():
    query = """
        SELECT m.id, m.name, m.id_lect, l.name as lecture_name
        FROM materials m
        JOIN lectures l ON m.id_lect = l.id;
    """
    materials = fetch_all(query)
    if not materials:
        print("Нет материалов для дублирования в Elasticsearch.")
        return

    actions = [
        {
            "_index": ES_INDEX,
            "_id": material["id"],
            "_source": {
                "id": material["id"],
                "id_lect": material["id_lect"],
                "name": material["name"],
                "lecture_text": f"Материал для лекции: {material['lecture_name']}. {fake.text(max_nb_chars=300)}", # Уменьшил текст
                "created_at": datetime.now().isoformat()
            }
        }
        for material in materials
    ]
    if actions:
        try:

            if not es.indices.exists(index=ES_INDEX):
                es.indices.create(index=ES_INDEX)
                print(f"Создан индекс Elasticsearch: '{ES_INDEX}'")
            helpers.bulk(es, actions)
            print(f"Дублировано {len(materials)} материалов лекций в Elasticsearch (индекс '{ES_INDEX}').")
        except Exception as e:
            print(f"Ошибка при записи в Elasticsearch: {e}")
    else:
        print("Нет материалов для записи в Elasticsearch (после формирования actions).")


def duplicate_universities_to_mongo():
    universities = fetch_all("SELECT id, name FROM universities;")
    institutes = fetch_all("SELECT id, name, id_university FROM institutes;")
    kafedras = fetch_all("SELECT id, name, id_institutes FROM kafedras;")
    
    if not universities:
        print("Нет университетов для дублирования в MongoDB.")
        return

    uni_dict = {}
    for uni in universities:
        uni_dict[uni["id"]] = {
            "mongo_id": uni["id"],
            "name": uni["name"],
            "institutes": []
        }
    
    inst_dict = {}
    for inst in institutes:
        inst_dict[inst["id"]] = {
            "mongo_id": inst["id"],
            "name": inst["name"],
            "kafedras": []
        }

        if inst["id_university"] in uni_dict:
             uni_dict[inst["id_university"]]["institutes"].append(inst_dict[inst["id"]])

    for kaf in kafedras:

        if kaf["id_institutes"] in inst_dict:
            inst_dict[kaf["id_institutes"]]["kafedras"].append({
                "mongo_id": kaf["id"],
                "name": kaf["name"]
            })
    
    collection = mongo_db["universities"]
    collection.delete_many({})
    documents = list(uni_dict.values())
    if documents:
        collection.insert_many(documents)
        print(f"Дублировано {len(documents)} университетов с институтами и кафедрами в MongoDB.")
    else:
        print("Нет документов для вставки в MongoDB 'universities'.")


def duplicate_relationships_to_neo4j():
    students = fetch_all("SELECT id, fio, id_group FROM students;")
    groups = fetch_all("SELECT id, name, id_kafedra FROM groups;")
    lectures = fetch_all("SELECT id, name, id_course FROM lectures;")
    visits_limit = 50000 
    visits = fetch_all(f"""
        SELECT v.id_student,v.id_schedule, sch.id_lect as lecture_id, sch.id_group, v.visitTime as visittime, v.status
        FROM visits v
        JOIN schedule sch ON v.id_schedule = sch.id
        ORDER BY random()
        LIMIT {visits_limit};
    """)

    if not students and not groups and not lectures:
        print("Недостаточно данных для дублирования в Neo4j.")
        return

    print(f"Получено {len(visits)} записей visits.")
    if visits:
        print(f"Ключи первой записи visits: {list(visits[0].keys())}")
        print(f"Пример первой записи: {visits[0]}")
    else:
        print("Таблица visits пуста или запрос не вернул данных.")

    def run_tx(tx, query, params=None):
        tx.run(query, params or {})

    with neo4j_driver.session(database="neo4j") as session:
        session.run("MATCH (n) DETACH DELETE n")
        print("Neo4j очищен перед дублированием.")

        # Создаем узлы
        for s_data in students:
            session.execute_write(run_tx, 
                "MERGE (s:Student {id: $id}) SET s.fio = $fio, s.groupId = $groupId", 
                {"id": s_data["id"], "fio": s_data["fio"], "groupId": s_data["id_group"]})
        print(f"Neo4j: Создано/обновлено {len(students)} узлов Student.")

        for g_data in groups:
            session.execute_write(run_tx, 
                "MERGE (g:Group {id: $id}) SET g.name = $name, g.kafedraId = $kafedraId",
                {"id": g_data["id"], "name": g_data["name"], "kafedraId": g_data["id_kafedra"]})
        print(f"Neo4j: Создано/обновлено {len(groups)} узлов Group.")

        for l_data in lectures:
            session.execute_write(run_tx, 
                "MERGE (l:Lecture {id: $id}) SET l.name = $name, l.courseId = $courseId",
                {"id": l_data["id"], "name": l_data["name"], "courseId": l_data["id_course"]})
        print(f"Neo4j: Создано/обновлено {len(lectures)} узлов Lecture.")

        # Создаем связи Student -> Group
        for s_data in students:
            session.execute_write(run_tx,
                "MATCH (st:Student {id: $studentId}), (gr:Group {id: $groupId}) MERGE (st)-[:BELONGS_TO]->(gr)",
                {"studentId": s_data["id"], "groupId": s_data["id_group"]}
            )
        print(f"Neo4j: Созданы связи BELONGS_TO (Student->Group).")

        # Создаем связи Group -> Lecture (ATTENDED)
        processed_visits_links = 0
        for v_data in visits:
            if "visittime" not in v_data:
                print(f"Ошибка: visittime отсутствует в записи: {v_data}")
                continue
            visit_time_iso = v_data["visittime"].isoformat() if isinstance(v_data["visittime"], datetime) else str(v_data["visittime"])
            session.execute_write(run_tx,
                """
                MATCH (gr:Group {id: $groupId})
                MATCH (lec:Lecture {id: $lectureId})
                MERGE (gr)-[r:ATTENDED {visitTime: datetime($visitTime)}]->(lec)
                SET r.status = $status,
                    r.id_schedule = $id_schedule
                """,
                {
                    "groupId": v_data["id_group"],
                    "lectureId": v_data["lecture_id"],
                    "visitTime": visit_time_iso,
                    "status": v_data["status"],
                    "id_schedule": v_data["id_schedule"]
                }
            )
            processed_visits_links += 1
        print(f"Neo4j: Создано/обновлено {processed_visits_links} связей ATTENDED (Group->Lecture).")

    print(f"Данные успешно дублированы в Neo4j: {len(students)} студентов, {len(groups)} групп, {len(lectures)} лекций, {processed_visits_links} посещений (связей).")

# Основной процесс
try:
    print("Генерация данных для РТУ МИРЭА")
    clear_all_data()
    create_tables()

    with pg_conn:
        with pg_conn.cursor() as cur:
            print("Вставка университетов...")
            universities = insert_universities(cur)
            
            print("Вставка институтов...")
            institutes = insert_institutes(cur, universities[0]["id"]) if universities else []
            
            print("Вставка кафедр...")
            kafedras = insert_kafedras(cur, institutes) if institutes else []
            
            print("Вставка специальностей...")
            specialties = insert_specialties(cur)
            
            print("Вставка связей кафедра-специальность...")
            if kafedras and specialties:
                insert_kafedra_specialties(cur, kafedras, specialties)
            
            print("Вставка групп...")
            groups = insert_groups(cur, kafedras) if kafedras else []
            
            print("Вставка студентов...")
            students = insert_students(cur, groups) if groups else []
            
            print("Вставка курсов...")
            courses = insert_courses(cur, kafedras, specialties) if kafedras and specialties else []
            
            print("Вставка лекций...")
            lectures = insert_lectures(cur, courses) if courses else []
            
            print("Вставка материалов...")
            materials = insert_materials(cur, lectures) if lectures else [] # Ожидается 2500-7500
            
            print("Вставка расписания...")
            schedules = insert_schedule(cur, groups, lectures) if groups and lectures else [] # Ожидается 1000-1500
            
            print("Вставка посещаемости...")
            if students and schedules:
                num_visits_inserted = insert_visits(cur, students, schedules) # Ожидается ~100K-150K
                print(f"Всего записей о посещаемости вставлено в PostgreSQL: {num_visits_inserted}")
            else:
                print("Пропущена вставка посещаемости: нет студентов или расписаний.")

            print("Данные успешно сгенерированы в PostgreSQL!")
            
            print("\n--- Отображение части данных из PostgreSQL ---")
            display_groups(cur)
            display_students(cur)
            display_lectures(cur)
            display_materials(cur)
            display_schedule(cur)
            display_visits(cur)

    print("\n--- Дублирование данных в другие БД ---")
    duplicate_students_to_redis()
    duplicate_lecture_materials_to_es()
    duplicate_universities_to_mongo()
    duplicate_relationships_to_neo4j()

    print("\n--- Генерация и дублирование данных завершены успешно! ---")

except psycopg2.Error as db_err:
    print(f"Ошибка базы данных PostgreSQL: {db_err}")

except Exception as e:
    print(f"Произошла ошибка во время генерации данных: {e}")
finally:
    if pg_conn and not pg_conn.closed:
        pg_conn.close()
        print("Соединение с PostgreSQL закрыто.")
    if neo4j_driver:
        neo4j_driver.close()
        print("Соединение с Neo4j закрыто.")