import pandas as pd
from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")
db = client["task1"]
collection = db["eminem"]


def load_data_from_csv(file_path):
    """Чтение данных из CSV файла и вставка их в коллекцию."""
    data = pd.read_csv(file_path, delimiter=";")
    collection.insert_many(data.to_dict(orient="records"))


def remove_documents():
    """Удалить документы с зарплатой < 25000 или > 175000."""
    result = collection.delete_many(
        {"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]}
    )
    print(f"Удалено документов: {result.deleted_count}")


def increase_age():
    """Увеличить возраст всех документов на 1."""
    collection.update_many({}, {"$inc": {"age": 1}})


def increase_salary_for_jobs(jobs):
    """Поднять зарплату на 5% для заданных профессий."""
    collection.update_many({"job": {"$in": jobs}}, {"$mul": {"salary": 1.05}})


def increase_salary_for_cities(cities):
    """Поднять зарплату на 7% для заданных городов."""
    collection.update_many({"city": {"$in": cities}}, {"$mul": {"salary": 1.07}})


def increase_salary_complex_predicate(city, jobs, age_range):
    """Поднять зарплату на 10% для выборки по сложному предикату."""
    collection.update_many(
        {
            "city": city,
            "job": {"$in": jobs},
            "age": {"$gte": age_range[0], "$lte": age_range[1]},
        },
        {"$mul": {"salary": 1.10}},
    )


def remove_by_custom_predicate(predicate):
    """Удалить записи по произвольному предикату."""
    result = collection.delete_many(predicate)
    print(f"Удалено документов по произвольному предикату: {result.deleted_count}")


if __name__ == "__main__":
    file_path = "data/task_3_item.csv"
    load_data_from_csv(file_path)

    # Удаление документов с зарплатой < 25000 или > 175000
    remove_documents()

    # Увеличение возраста всех документов на 1
    increase_age()

    # Пример профессий для повышения зарплаты на 5%
    jobs_to_increase_salary = ["Инженер", "Строитель"]
    increase_salary_for_jobs(jobs_to_increase_salary)

    # Пример городов для повышения зарплаты на 7%
    cities_to_increase_salary = ["Ла-Корунья", "Эль-Пуэрто-де-Санта-Мария"]
    increase_salary_for_cities(cities_to_increase_salary)

    # Пример сложного предиката для повышения зарплаты на 10%
    complex_city = "Ла-Корунья"
    complex_jobs = ["Инженер", "Строитель"]
    complex_age_range = (30, 50)
    increase_salary_complex_predicate(complex_city, complex_jobs, complex_age_range)

    # Пример произвольного предиката для удаления
    custom_predicate = {"year": {"$lt": 2000}}  # Удалить документы с годом < 2000
    remove_by_custom_predicate(custom_predicate)

    print("Операции выполнены.")
