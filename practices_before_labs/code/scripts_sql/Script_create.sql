
--Вузы
CREATE TABLE "universities" (
	"id" integer PRIMARY KEY,
	"name" text                   --название
);

--Институты
CREATE TABLE "institutes" (
  "id" integer PRIMARY KEY,
  "id_university" integer,            
  "name" text                     --название
);
ALTER TABLE "institutes" ADD FOREIGN KEY ("id_university") REFERENCES "universities" ("id");

--Кафедры
CREATE TABLE "kafedras" (
  "id" integer PRIMARY KEY,
  "id_institutes" integer,
  "name" text                     --название
);
ALTER TABLE "kafedras" ADD FOREIGN KEY ("id_institutes") REFERENCES "institutes" ("id");

--Группы
CREATE TABLE "groups" (
  "id" integer PRIMARY KEY,
  "id_kafedra" integer,
  "name" text,               --название
  "startYear" date,          --начало обучения
  "endYear" date             --конец обучения
);
ALTER TABLE "groups" ADD FOREIGN KEY ("id_kafedra") REFERENCES "kafedras" ("id");

--Студенты
CREATE TABLE "students" (
  "id" integer PRIMARY KEY,
  "id_group" integer,
  "fio" text,
  "date_of_admission" date,    -- дата поступления
  "email" text                 -- почта
);
ALTER TABLE "students" ADD FOREIGN KEY ("id_group") REFERENCES "groups" ("id");


--специальности
CREATE TABLE "specialties" (
  "id" integer PRIMARY KEY,
  "name" text,                 --название
  "code" text                  --код специальности   
);

--курсы
CREATE TABLE "courses" (
  "id" integer PRIMARY KEY,
  "id_kadefra" integer,
  "id_spec" integer,
  "name" text,                 -- название
  "term" date                  -- семестр
);
ALTER TABLE "courses" ALTER COLUMN "term" TYPE TEXT;
ALTER TABLE "courses" ADD FOREIGN KEY ("id_kadefra") REFERENCES "kafedras" ("id");
ALTER TABLE "courses" ADD FOREIGN KEY ("id_spec") REFERENCES "specialties" ("id");


--лекции
CREATE TABLE "lectures" (
  "id" integer PRIMARY KEY,
  "id_course" integer,
  "name" text,                 -- название
  "requirments" boolean        -- требованние
);
ALTER TABLE "lectures" ADD FOREIGN KEY ("id_course") REFERENCES "courses" ("id");

--материалы
CREATE TABLE "materials" (
  "id" integer PRIMARY KEY,
  "id_lect" integer,
  "name" text                   -- название
);
ALTER TABLE "materials" ADD FOREIGN KEY ("id_lect") REFERENCES "lectures" ("id");

--расписания
CREATE TABLE "schedule" (
  "id" integer PRIMARY KEY,
  "id_lect" integer,
  "id_group" integer,
  "startTime" timestamptz,      -- начало
  "endTime" timestamptz         -- конец пары
);
ALTER TABLE "schedule" ADD FOREIGN KEY ("id_lect") REFERENCES "lectures" ("id");
ALTER TABLE "schedule" ADD FOREIGN KEY ("id_group") REFERENCES "groups" ("id");

--посещения
CREATE TABLE "visits" (
  "id" integer PRIMARY KEY,
  "code_student" integer,
  "id_rasp" integer,
  "visitTime" timestamptz        -- время пребытия в вуз
);
ALTER TABLE "visits" ADD FOREIGN KEY ("code_student") REFERENCES "students" ("id");
ALTER TABLE "visits" ADD FOREIGN KEY ("id_rasp") REFERENCES "schedule" ("id");

-- Промежуточная таблица для связи "многие ко многим" между kafedras и specialties
CREATE TABLE kafedra_specialties (
    id_kafedra integer,
    id_specialty integer,
    PRIMARY KEY (id_kafedra, id_specialty), -- Составной первичный ключ
    FOREIGN KEY (id_kafedra) REFERENCES kafedras (id),
    FOREIGN KEY (id_specialty) REFERENCES specialties (id)
);


