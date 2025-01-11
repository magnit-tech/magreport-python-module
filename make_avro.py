import fastavro

# Определим схему Avro
schema = {
    "type": "record",
    "name": "Example",
    "fields": [
        {"name": "column1", "type": "int"},
        {"name": "column2", "type": "int"}
    ]
}

# Создаем данные для записи
records = [
    {"column1": 1, "column2": 2},
    {"column1": 3, "column2": 4},
    {"column1": 5, "column2": 6}
]

# Записываем данные в Avro файл
filename = "example.avro"
with open(filename, 'wb') as out:
    fastavro.writer(out, schema, records)

print(f"Avro файл '{filename}' создан с записями.")

# Читаем и выводим содержимое Avro файла
with open(filename, 'rb') as f:
    for record in fastavro.reader(f):
        print(record)
