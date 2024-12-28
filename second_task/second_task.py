import json
from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["task1"]  # Имя базы данных
collection = db["eminem"]  # Имя коллекции


def load_data_from_json(file_path):
    """Чтение данных из JSON файла и вставка их в коллекцию."""
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    collection.insert_many(data)


def get_salary_stats():
    """Возвращает минимальную, среднюю и максимальную зарплату."""
    return collection.aggregate(
        [
            {
                "$group": {
                    "_id": None,
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"},
                }
            }
        ]
    )


def get_job_counts():
    """Возвращает количество данных по профессиям."""
    return collection.aggregate([{"$group": {"_id": "$job", "count": {"$sum": 1}}}])


def get_salary_by_city():
    """Возвращает минимальную, среднюю и максимальную зарплату по городу."""
    return collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$city",
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"},
                }
            }
        ]
    )


def get_salary_by_job():
    """Возвращает минимальную, среднюю и максимальную зарплату по профессии."""
    return collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$job",
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"},
                }
            }
        ]
    )


def get_age_by_city():
    """Возвращает минимальный, средний и максимальный возраст по городу."""
    return collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$city",
                    "min_age": {"$min": "$age"},
                    "avg_age": {"$avg": "$age"},
                    "max_age": {"$max": "$age"},
                }
            }
        ]
    )


def get_age_by_job():
    """Возвращает минимальный, средний и максимальный возраст по профессии."""
    return collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$job",
                    "min_age": {"$min": "$age"},
                    "avg_age": {"$avg": "$age"},
                    "max_age": {"$max": "$age"},
                }
            }
        ]
    )


def get_max_salary_min_age():
    """Возвращает максимальную зарплату при минимальном возрасте."""
    return collection.aggregate(
        [
            {
                "$group": {
                    "_id": {"min_age": {"$min": "$age"}},
                    "max_salary": {"$max": "$salary"},
                }
            }
        ]
    )


def get_min_salary_max_age():
    """Возвращает минимальную зарплату при максимальном возрасте."""
    return collection.aggregate(
        [
            {
                "$group": {
                    "_id": {"max_age": {"$max": "$age"}},
                    "min_salary": {"$min": "$salary"},
                }
            }
        ]
    )


def get_age_by_city_salary_condition():
    """Возвращает минимальный, средний и максимальный возраст по городу с условием по зарплате."""
    return collection.aggregate(
        [
            {"$match": {"salary": {"$gt": 50000}}},
            {
                "$group": {
                    "_id": "$city",
                    "min_age": {"$min": "$age"},
                    "avg_age": {"$avg": "$age"},
                    "max_age": {"$max": "$age"},
                }
            },
            {"$sort": {"avg_age": -1}},
        ]
    )


def get_salary_in_specified_ranges():
    """Возвращает минимальную, среднюю и максимальную зарплату в заданных диапазонах."""
    return collection.aggregate(
        [
            {
                "$match": {
                    "age": {"$gt": 18, "$lt": 25},
                    "salary": {"$gt": 50, "$lt": 65},
                }
            },
            {
                "$group": {
                    "_id": None,
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"},
                }
            },
        ]
    )


def custom_query():
    """Произвольный запрос с $match, $group, $sort."""
    return collection.aggregate(
        [
            {"$match": {"city": "Ла-Корунья"}},
            {
                "$group": {
                    "_id": "$job",
                    "total_salary": {"$sum": "$salary"},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"total_salary": -1}},
        ]
    )


# Основная часть программы
if __name__ == "__main__":
    file_path = "D:\\data_lab5\\data\\task_2_item.json"
    load_data_from_json(file_path)

    print("\nSalary Stats:")
    for stat in get_salary_stats():
        print(json.dumps(stat, ensure_ascii=False, indent=4))

    print("\nJob Counts:")
    for job in get_job_counts():
        print(json.dumps(job, ensure_ascii=False, indent=4))

    print("\nSalary by City:")
    for city in get_salary_by_city():
        print(json.dumps(city, ensure_ascii=False, indent=4))

    print("\nSalary by Job:")
    for job in get_salary_by_job():
        print(json.dumps(job, ensure_ascii=False, indent=4))

    print("\nAge by City:")
    for city in get_age_by_city():
        print(json.dumps(city, ensure_ascii=False, indent=4))

    print("\nAge by Job:")
    for job in get_age_by_job():
        print(json.dumps(job, ensure_ascii=False, indent=4))

    print("\nMax Salary at Min Age:")
    for result in get_max_salary_min_age():
        print(json.dumps(result, ensure_ascii=False, indent=4))

    print("\nMin Salary at Max Age:")
    for result in get_min_salary_max_age():
        print(json.dumps(result, ensure_ascii=False, indent=4))

    print("\nAge by City with Salary Condition:")
    for city in get_age_by_city_salary_condition():
        print(json.dumps(city, ensure_ascii=False, indent=4))

    print("\nSalary in Specified Ranges:")
    for result in get_salary_in_specified_ranges():
        print(json.dumps(result, ensure_ascii=False, indent=4))

    print("\nCustom Query Result:")
    for result in custom_query():
        print(json.dumps(result, ensure_ascii=False, indent=4))
