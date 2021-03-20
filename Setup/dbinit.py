# main.py is adapted from the SQLite-in-Python-1-Example1.py File from a lab
# File name: SQLite-in-Python-1-Example1.py
# Lab Title: Lab (Python & SQLite - Part 1)
# Link to Source: https://drive.google.com/file/d/11ukUPkVpdjjNPhocDZin5cdZUlHvELFZ/view
# Date Posted: 10:03 AM Feb 25, 2021
# Author: Professor Mario Nascimento

# Referenced the official python documentation to read csv
# https://docs.python.org/3/library/csv.html

from typing import List
import sqlite3
import csv
import os
import random

Connection = sqlite3.Connection
DB_PATH = "../SQLiteDBs/main.db"
V100_DB_PATH = "../SQLiteDBs/A4v100.db"
V1K_DB_PATH = "../SQLiteDBs/A4v1k.db"
V10K_DB_PATH = "../SQLiteDBs/A4v10k.db"
V100K_DB_PATH = "../SQLiteDBs/A4v100k.db"
V1M_DB_PATH = "../SQLiteDBs/A4v1M.db"

COUNTRY_DATA = "../Data/data_csv.csv"
UPC_DATA = "../Data/upc_corpus.csv"


def connect() -> Connection:
    # Returns a connection to the database provided at the path.
    db_path = exact_path(DB_PATH)
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    # To enable foreign keys for SQLite
    cursor.execute(' PRAGMA foreign_keys=ON; ')
    connection.commit()
    return connection


def exact_path(path) -> str:
    curr = os.path.dirname(__file__)
    load_path = os.path.join(curr, path)
    return load_path


def load_data(path: str) -> List[str]:
    data_path = exact_path(path)
    data = []
    added = set()
    with open(data_path) as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            if(is_valid_row(added, row, path)):
                data.append(row)
                added.add(row[0])
        csvfile.close()
    return data[1:]


def is_valid_row(added, row, path)-> bool:
    if path is UPC_DATA:
        return is_valid_upc(added, row)
    else:
        return row[0] not in added


def is_valid_upc(added, row)->bool:
    return row[0] != 'null' and row[0] != '' and row[0] not in added and row[0].isdigit() and int(row[0]) <= 2**63-1


def main() -> None:
    country_data = load_data(COUNTRY_DATA)
    upc_data = load_data(UPC_DATA)
    make_main()
    populate(country_data, upc_data)
    make_copies()

def make_main() -> None:
    create_table_query = '''
    CREATE TABLE Parts (
        partNumber INTEGER,
        -- a UPC code
        partPrice INTEGER,
        -- in the [1, 100] range
        needsPart INTEGER,
        -- a UPC code
        madeIn TEXT,
        -- a country (2 letters) code
        PRIMARY KEY(partNumber)
    );
    '''
    connection = connect()
    connection.cursor().execute(create_table_query)

def make_copies() -> None:
    query = '''
    ATTACH DATABASE :file_name AS new_db;
    '''
    query1 = '''
    CREATE TABLE new_db.Parts (
        partNumber INTEGER,
        -- a UPC code
        partPrice INTEGER,
        -- in the [1, 100] range
        needsPart INTEGER,
        -- a UPC code
        madeIn TEXT,
        -- a country (2 letters) code
        PRIMARY KEY(partNumber)
    );
    '''
    query2 = '''
    INSERT INTO
        new_db.Parts
    SELECT
        *
    FROM
        Parts
    limit :amount;
    '''
    paths = [V100_DB_PATH, V1K_DB_PATH, V10K_DB_PATH, V100K_DB_PATH, V1M_DB_PATH]
    amounts = [100, 1000, 10000, 100000, 1000000]
    for i in range(0,5):
        connection = connect()
        connection.execute(query,{"file_name": exact_path(paths[i])}).execute(query1).execute(query2,{"amount": amounts[i]})
        connection.commit()
        connection.close()


def populate(country_data, upc_data) -> None:
    connection = connect()
    query = '''
    INSERT INTO Parts
        VALUES(:partNumber, :partPrice, :needsPart, :madeIn);
    '''

    upc_data_len = len(upc_data)
    country_data_len = len(country_data)
    random.shuffle(country_data)
    random.shuffle(upc_data)
    insertions = []
    for i in range(0, upc_data_len):  # Populate with 1 million entries
        rand_country_i = random.randint(0, country_data_len-1)
        country_code = country_data[rand_country_i][1]
        part_price = random.randint(0, 100)
        needs_part_i = random.randint(0, upc_data_len-1)
        part_number = upc_data[i][0]
        needs_part = upc_data[needs_part_i][0]

        insertions.append({
            "partNumber": part_number, 
            "partPrice": part_price, 
            "needsPart": needs_part, 
            "madeIn": country_code
        })
    connection.executemany(query, insertions)

    connection.commit()


if __name__ == "__main__":
    main()
