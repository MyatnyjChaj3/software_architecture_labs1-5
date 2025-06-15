db.universities.insertOne({
    _id: 1,
    name: "РТУ МИРЭА",
    institutes: [
      {
        _id: 1,
        name: "Институт ИКБ",
        departments: [
          {
            _id: 1,
            name: "Разработка безопасного ПО"
          }
        ]
      }
    ]
  });
  
  db.universities.find({"institutes.departments.name":"Разработка безопасного ПО"})