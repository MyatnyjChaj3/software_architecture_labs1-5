-- Добавление университета РТУ МИРЭА
INSERT INTO universities (id, name) VALUES (1, 'РТУ МИРЭА');

-- Добавление института ИКБ
INSERT INTO institutes (id, id_university, name) VALUES (1, 1, 'Институт ИКБ');

-- Добавление кафедры # Разработка программных решений и системное программирование
INSERT INTO kafedras (id, id_institutes, name) VALUES (1, 1, 'Разработка программных решений и системное программирование');

-- Добавление группы (пример: ПО-201)
INSERT INTO groups (id, id_kafedra, name, "startYear", "endYear")
VALUES (1, 1, 'БСБО-01-22', '2022-09-01', '2026-07-01');

-- Добавление студента
INSERT INTO students (id, id_group, fio, date_of_recipient)
VALUES (1, 1, 'Ищенко Анастасия Михайловна', '2022-09-01');

-- Добавление специальности
INSERT INTO specialties (id, name, code)
VALUES (1, 'Информационные системы и технологии', '09.03.02');

-- Добавление курса
INSERT INTO courses (id, id_kadefra, id_spec, name, term)
VALUES (1, 1, 1, 'Проектирование архитектуры программного обеспечения', '2025-2026');

-- Добавление лекции
INSERT INTO lectures (id, id_course, name, requirments)
VALUES (1, 1, 'Введение в архитектуру ПО', true);

-- Добавление материалов к лекции
INSERT INTO materials (id, id_lect, name)
VALUES (1, 1, 'Проектирование архитектуры ПО. Лекция 1.');

-- Добавление расписания
INSERT INTO schedule (id, id_lect, id_group, "startTime", "endTime")
VALUES (1, 1, 1, '2023-02-10 10:40:00', '2025-02-10 12:10:00');

-- Добавление посещения
INSERT INTO visits (id, code_student, id_rasp, "visitTime")
VALUES (1, 1, 1, '2023-02-10 10:45:00');