
--Вузы/университет
CREATE TABLE IF NOT EXISTS universities (
	id SERIAL PRIMARY KEY,
	name text NOT NULL                  --название
);

--Институты
CREATE TABLE IF NOT EXISTS institutes (
  id SERIAL PRIMARY KEY,
  id_university integer NOT NULL,            
  name text NOT NULL                     --название
);
ALTER TABLE institutes ADD FOREIGN KEY (id_university) REFERENCES universities (id) ON DELETE CASCADE;

--Кафедры
CREATE TABLE IF NOT EXISTS kafedras (
  id SERIAL PRIMARY KEY,
  id_institutes integer NOT NULL,
  name text NOT NULL                    --название
);
ALTER TABLE kafedras ADD FOREIGN KEY (id_institutes) REFERENCES institutes (id) ON DELETE CASCADE;

--Группы
CREATE TABLE IF NOT EXISTS groups (
  id SERIAL PRIMARY KEY,
  id_kafedra integer NOT NULL,
  name text NOT NULL,               --название
  startYear date NOT NULL,            --начало обучения
  endYear date NOT NULL            --конец обучения
);
ALTER TABLE groups ADD FOREIGN KEY (id_kafedra) REFERENCES kafedras (id) ON DELETE CASCADE;

--Студенты
CREATE TABLE IF NOT EXISTS students (
  id SERIAL PRIMARY KEY,
  fio text NOT NULL,
  id_group integer NOT NULL,
  date_of_admission date NOT NULL    -- дата поступления
);
ALTER TABLE students ADD FOREIGN KEY (id_group) REFERENCES groups (id) ON DELETE CASCADE;


--специальности
CREATE TABLE IF NOT EXISTS specialties (
  id SERIAL PRIMARY KEY,
  name text NOT NULL,                 --название
  code VARCHAR(30) NOT NULL                  --код специальности   
);

--курсы лекций
CREATE TABLE IF NOT EXISTS courses (
  id SERIAL PRIMARY KEY,
  id_kafedra integer NOT NULL,
  id_specialty integer NOT NULL,
  name text NOT NULL,                 -- название
  planned_hours INT NOT NULL
);
ALTER TABLE courses ADD FOREIGN KEY (id_kafedra) REFERENCES kafedras (id) ON DELETE CASCADE;
ALTER TABLE courses ADD FOREIGN KEY (id_specialty) REFERENCES specialties (id) ON DELETE CASCADE;


--лекции
CREATE TABLE IF NOT EXISTS lectures (
  id SERIAL PRIMARY KEY,
  id_course integer NOT NULL,
  name text NOT NULL,                 -- название
  duration_hours INT NOT NULL DEFAULT 2 CHECK (duration_hours=2), --длительность лекции
  requirements boolean NOT NULL DEFAULT TRUE,  -- технические требования
  text_requirements text -- описание требований
);
ALTER TABLE lectures ADD FOREIGN KEY (id_course) REFERENCES courses (id) ON DELETE CASCADE;

--материалы лекции
CREATE TABLE IF NOT EXISTS materials (
  id SERIAL PRIMARY KEY,
  id_lect integer NOT NULL,
  name text NOT NULL                   -- название
);
ALTER TABLE materials ADD FOREIGN KEY (id_lect) REFERENCES lectures (id) ON DELETE CASCADE;

--расписания
CREATE TABLE IF NOT EXISTS schedule (
  id SERIAL PRIMARY KEY,
  id_lect integer NOT NULL,
  id_group integer NOT NULL,
  auditorium VARCHAR(30) NOT NULL,
  capacity INT NOT NULL --вместимость аудитории
);
ALTER TABLE schedule ADD FOREIGN KEY (id_lect) REFERENCES lectures (id) ON DELETE CASCADE;
ALTER TABLE schedule ADD FOREIGN KEY (id_group) REFERENCES groups (id) ON DELETE CASCADE;
 
--посещения
CREATE TABLE IF NOT EXISTS visits (
    id SERIAL,
    id_student integer NOT NULL,
    id_schedule integer NOT NULL,
    visitTime TIMESTAMP NOT NULL,
    week_start DATE NOT NULL,
    status VARCHAR(10) CHECK (status IN ('presence','absence','late')) NOT NULL,
    PRIMARY KEY (id, week_start)
) PARTITION BY RANGE (week_start);
ALTER TABLE visits ADD FOREIGN KEY (id_student) REFERENCES students (id) ON DELETE CASCADE;
ALTER TABLE visits ADD FOREIGN KEY (id_schedule) REFERENCES schedule (id) ON DELETE CASCADE;

-- Промежуточная таблица для связи "многие ко многим" между kafedras и specialties
CREATE TABLE IF NOT EXISTS kafedra_specialties (
    id_kafedra integer,
    id_specialty integer,
    PRIMARY KEY (id_kafedra, id_specialty), -- Составной первичный ключ
    FOREIGN KEY (id_kafedra) REFERENCES kafedras (id) ON DELETE CASCADE,
    FOREIGN KEY (id_specialty) REFERENCES specialties (id) ON DELETE CASCADE
);


