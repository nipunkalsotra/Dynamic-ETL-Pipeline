import os
import polars as pl
from pymongo import MongoClient
from datetime import datetime


def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    return client["etl_project"]


def load_to_mongodb(df: pl.DataFrame, collection_name="transformed_data"):
    if df.is_empty():
        return
    db = get_db()
    collection = db[collection_name]
    records = df.to_dicts()
    collection.insert_many(records)


def backup_to_parquet(df: pl.DataFrame, backup_dir="backups"):
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(backup_dir, f"backup_{timestamp}.parquet")
    df.write_parquet(file_path)
    return file_path


def load_data(df: pl.DataFrame, collection_name="transformed_data"):
    load_to_mongodb(df, collection_name)
    backup_path = backup_to_parquet(df)
    print(f"✅ Data loaded to MongoDB and backed up at {backup_path}")
