import fastavro
import random

# Определяем схему Avro для тестовых данных
schema = {
    "type": "record",
    "name": "TestData",
    "fields": [
        {"name": "column1", "type": "int"},
        {"name": "column2", "type": "int"}
    ]
}

# Создаем 10 тестовых записей с случайными значениями от 1 до 10
records = []
for i in range(1000000):
    record = {
        "column1": random.randint(1, 100),
        "column2": random.randint(1, 100)
    }
    records.append(record)

filename = "testmillion.avro"
with open(filename, "wb") as out:
    fastavro.writer(out, schema, records)

print(f"Avro файл '{filename}' создан с записями:")
for record in records:
    print(record)
