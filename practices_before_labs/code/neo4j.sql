
CREATE (:Lecture {id: 1, name: 'Введение в архитектуру ПО', requirments: true});

CREATE (:Group {id: 1, name: 'БСБО-01-22', startYear: '2022-09-01', endYear: '2026-07-01'});

CREATE (:Student {id: 1, fio: 'Ищенко Анастасия Михайловна', date_of_admission: '2022-09-01'});

MATCH (s:Student {id: 1}), (g:Group {id: 1})
CREATE (s)-[:IS_IN_GROUP]->(g);

MATCH (g:Group {id: 1}), (l:Lecture {id: 1})
CREATE (g)-[:ATTENDED {visitTime: '2023-09-01T10:41:00'}]->(l);