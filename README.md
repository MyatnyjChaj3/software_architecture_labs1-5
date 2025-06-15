## Практические работы (1-3)

- Нахождение:

```text
cd full_project/practices_before_labs/
docker-compose.yml
```

- Запуск:

```text
docker-compose up --build
```

- Все примеры создания структур и вставки данных находятся в папке ```code```:

```text
cd full_project/practices_before_labs/code/

postgres.sql
neo4j.sql
redis.sql
mongo.js
elastic.txt
```

- Запуск контейнеров:
  1) для Postgres, Redis и MongoDB используется стороннее приложение DBeaver, в котором надо создать для каждого контейнера отдельное подключение.
  2) для Elasticsearch используется Kibana (в Kibana пункт Dev Tools)
  3) у Neo4j есть свой сайт

# Лабораторные 1-3

Структура проекта full_project/project_labs_1-3:

```plain
project_labs_1-3/
├── api_gateway/
│   ├── app/
|   |   └── api-gateway.py
|   ├── requirements.txt
│   └── Dockerfile
├── data_center/
│   ├── postgres.sql
|   ├── generator.py
|   ├── requirements.txt
│   └── Dockerfile
├── lab1/
│   ├── app/
|   |   └── lab1.py
|   ├── requirements.txt
│   └── Dockerfile
├── lab2/
│   ├── app/
|   |   └── lab2.py
|   ├── requirements.txt
│   └── Dockerfile
├── lab3/
│   ├── app/
|   |   └── lab3.py
|   ├── requirements.txt
│   └── Dockerfile
└── docker-compose.yml
```

- api_gateway - реализация API Gateway с набором API методов для вызова с использованием протокола HTTP(s) 1-3 лабораторных работ с указанными входными параметрами, а также token (JWT) для аутентификации пользователя для доступа к вызываемым методам. 
- data_center - генерация данных для БД
- lab1, lab2, lab3 - отдельные контейнеры вызова методов по данным заданиям

Подключение к лабам:

- Получение токена (cmd):

```text
curl.exe -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "username=user1&password=password" http://localhost:8000/token
```

- В расширении браузера "Modheader" добавляем запрос:

![alt text](image.png)

```text
Authorization Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.
```

- Вход на сервисы лаб: http://localhost:800*/docs

\* 1, 2 или 3 в зависимости от номера лабы

- Очистка контейнеров: docker system prune -f и docker-compose down -v

## Лабораторная 1

**Задание на лабораторную работу №1:**

Выполнить запрос к структуре хранения информации о группах учащихся, курсах обучения, лекционной программе и составу лекционных курсов и практических занятий, а также структуре связей между курсами, специальностями, студентами кафедры и данными о посещении студентами занятий, для извлечения **отчета о 10 студентах с минимальным процентом посещения лекция, содержащих заданный термин или фразу, за определенный период обучения. Состав полей должен включать Полную информацию о студенте, процент посещения, период отчета, термин в занятиях курса.**

- Используемые БД: Elasticsearch, redis, neo4j, postgres

Пример поиска:

1) ElasticSearch (получаем id лекций с определенным термином):

```text
GET materials/_search
{
  "query": {
    "match": {
      "lecture_text": "Механический"
    }
  }
}
```

2) Neo4j (получаем id студентов, которые должны посещать данные лекции *):

```sql
MATCH (s:Student)-[:BELONGS_TO]->(g:Group)-[:ATTENDED]->(l:Lecture)
WHERE l.id IN [\*,\*,\*,\*,\*]
RETURN DISTINCT s.id AS student_id
```

3) Postgres (среди полученных Id находим 10 студентов с минимальным посещением лекций за определенный период)

```sql
WITH student_visit AS (
                        SELECT s.id as student_id, s.fio as fulln, l.id as lecture_id, l.name as lecture_name, COUNT(v.id) as total_lectures, SUM(CASE WHEN v.status IN ('presence', 'late') THEN 1 ELSE 0 END) as attended_lectures
                        FROM students s
                        JOIN visits v ON s.id = v.id_student
                        JOIN schedule sc ON v.id_schedule  = sc.id
                        JOIN lectures l ON sc.id_lect  = l.id
                        WHERE s.id = ANY(ARRAY[241,242,243,244,245,246,247,248,249,250,169, 170, 174, 175, 679,166,167,171,177,676,678])
                        AND l.id = ANY(ARRAY[548, 412, 786, 652, 728, 678])
                        AND v.visittime  BETWEEN '2025-01-01 13:00:00.000' AND '2025-09-11 13:00:00.000'
                        GROUP BY s.id, s.fio, l.id, l.name
                    )
                    
SELECT student_id, fulln, lecture_name, CAST(attended_lectures AS FLOAT) / NULLIF(total_lectures, 0) * 100 as attendance_percentage
                    FROM student_visit
                    ORDER BY attendance_percentage ASC
                    LIMIT 10  
```
4) Redis (получаем оставшуюся информацию о студентах)

```text
GET student:241
...
```
http://localhost:8000/lab1/visits?term=Механический&start_date=2025-01-15&end_date=2025-12-31

## Лабораторная 2

**Задание на лабораторную работу №2:**

Выполнить запрос к структуре хранения информации о группах учащихся, курсах обучения, лекционной программе и составу лекционных курсов и практических занятий, а также структуре связей между курсами, специальностями, студентами кафедры и данными о посещении студентами занятий, для извлечения **отчета о необходимом объеме аудитории для проведения занятий по курсу заданного семестра и года обучения с требованиями в описании к использованию технических средств. В качестве результата необходимо вывести полную информацию о курсе, лекции и количестве слушателей.**

- Используемые БД: neo4j, postgres

1) Postgres (информация о курсе и лекциях:)

```sql
SELECT
    c.id AS course_id,
    c.name AS course_name,
    l.id AS lecture_id,
    l.name AS lecture_topic,
    COALESCE(l.text_requirements, 'Нет требований') AS tech_requirements,
    s.auditorium,
    s.capacity AS current_capacity
FROM courses c
JOIN lectures l ON l.id_course = c.id
JOIN schedule s ON s.id_lect = l.id
WHERE c.name ILIKE 'Повар'
  AND s.id IN (
      SELECT sch.id
      FROM schedule sch
      JOIN visits v ON v.id_schedule = sch.id
      WHERE v.visitTime BETWEEN '2025-01-01 00:00:00' AND '2025-06-30 23:59:59'
  )
ORDER BY l.id;   
```
2) Neo4j (подсчет количества студентов):

```sql
MATCH (g:Group)-[att:ATTENDED]->(l:Lecture)
WHERE l.id IN [101, 102]
AND datetime(att.visitTime) >= datetime('2025-01-01')
AND datetime(att.visitTime) <= datetime('2025-06-30')
MATCH (s:Student)-[:BELONGS_TO]->(g)
RETURN l.id AS lecture_id, count(DISTINCT s) AS student_count;
```

http://localhost:8000/lab2/course-requirements?course_name=Повар&semester=1&year=2025

## Лабораторная 3

**Задание на лабораторную работу №3:**

Выполнить запрос к структуре хранения информации о группах учащихся, курсах обучения, лекционной программе и составу лекционных курсов и практических занятий, а также структуре связей между курсами, специальностями, студентами кафедры и данными о посещении студентами занятий, для извлечения **отчета по заданной группе учащихся с указанием объема прослушанных часов лекций а также необходимого объема запланированных часов, в рамках всех курсов для каждого студента группы.  Предполагается, что одна лекция равна 2-м академическим часам. В отчет должны попасть только лекции, которые содержат тег специальной дисциплины кафедры.  В качестве результата необходимо вывести полную информацию о группе, студенте, курсе, количестве запланированных часов и посещенных часов занятий.**

- Используемые БД: redis, neo4j, postgres

1) Postgres (получаем информацию о специальной лекции (с оборудованием requirement = true) и id группы)

```sql
select c.id as course_id, c.name as course_name, c.planned_hours, l.id as lecture_id 
from courses c
join lectures l on l.id_course = c.id 
where l.requirements = true

SELECT g.id, k.name as department_name
                FROM groups g
                JOIN kafedras k ON g.id_kafedra = k.id
                WHERE g.name = %s
```

2) Neo4j (получаем студентов и расписание):

```sql
MATCH (s:Student)-[:BELONGS_TO]->(g:Group {id: 1})
            MATCH (g)-[att:ATTENDED]->(l:Lecture)
            WHERE l.id IN [739,136,138,141,142]
            RETURN s.id AS student_id, l.id AS lecture_id, att.id_schedule AS schedule_id
```

3) происходит подсчет в postgres и забираются данные о студентах из redis

http://localhost:8000/lab3/group-attendance?group_name=

