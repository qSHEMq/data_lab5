import msgpack
import json
from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["task1"]
collection = db["eminem"]

# Чтение данных из файла
file_path = "D:\\data_lab5\\data\\task_1_item.msgpack"
with open(file_path, "rb") as file:
    data = msgpack.unpack(file)

# Вставка данных в MongoDB
collection.insert_many(data)

# Запросы

# 1. Первые 10 записей, отсортированных по убыванию по полю salary
top_10_salary = list(collection.find().sort("salary", -1).limit(10))
print("Первые 10 по Salary:")
print(json.dumps(top_10_salary, default=str, ensure_ascii=False, indent=4))

# 2. Первые 15 записей с age < 30, отсортированные по убыванию по полю salary
young_top_15_salary = list(
    collection.find({"age": {"$lt": 30}}).sort("salary", -1).limit(15)
)
print("\nПервые 15 возраст меньше 30 по Salary:")
print(json.dumps(young_top_15_salary, default=str, ensure_ascii=False, indent=4))

# 3. Первые 10 записей из произвольного города и трех профессий, сортировка по возрастанию age
city = "Краков"
professions = ["Врач", "Психолог", "Инженер"]
complex_filter = list(
    collection.find({"city": city, "job": {"$in": professions}})
    .sort("age", 1)
    .limit(10)
)
print("\nПервые 10 по Кракову и определённым профессиям, по age:")
print(json.dumps(complex_filter, default=str, ensure_ascii=False, indent=4))

# 4. Количество записей по сложному фильтру
count_filter = collection.count_documents(
    {
        "age": {"$gte": 20, "$lte": 35},
        "year": {"$gte": 2019, "$lte": 2022},
        "$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}},
        ],
    }
)
print("\nКоличество записей по фильтрации:")
print(json.dumps({"Количество": count_filter}, ensure_ascii=False, indent=4))
