#!/usr/bin/env python3

import time
import csv
import os
import random
import sqlite3
from sqlite3 import Connection
from typing import List

COUNTRY_DATA = "../Data/data_csv.csv"
V100_DB_PATH = "../SQLiteDBs/A4v100.db"
V1K_DB_PATH = "../SQLiteDBs/A4v1k.db"
V10K_DB_PATH = "../SQLiteDBs/A4v10k.db"
V100K_DB_PATH = "../SQLiteDBs/A4v100k.db"
V1M_DB_PATH = "../SQLiteDBs/A4v1M.db"


# Using uncorrelated sub query to find the max price and outer query to select 1 part number
QUERY_4 = '''
        select
            p1.partNumber
        from
            Parts p1
        where
            p1.madeIn = :countrycode
            and p1.partPrice = (
                select
                    max(partPrice)
                from
                    Parts p2
                where
                    p2.madeIn = :countrycode
            )
        limit   1;
    '''
# Creates a covering index that sqlite will use to efficiently find madeIn and their part price
CREATE_INDEX_QUERY = '''
    CREATE INDEX idxPartPriceMadeIn ON Parts ( madeIn, partPrice );
'''

DROP_INDEX_QUERY = '''
    DROP INDEX idxPartPriceMadeIn;
'''
country_list = None

def main():
    options = {"100":V100_DB_PATH,"1000": V1K_DB_PATH, "10000": V10K_DB_PATH, "100000": V100K_DB_PATH, "1000000": V1M_DB_PATH}
    
    print("Avg times and sizes for Query 4 without index\n")
    run_trials(options)
    
    print("Creating index for each database")

    update_index(options, CREATE_INDEX_QUERY)
    
    print("Avg times and sizes for Query 4 with index\n")
    run_trials(options)
    
    print("Dropping index for each database\n")

    update_index(options, DROP_INDEX_QUERY)
    print("Done!")

def update_index(options, query):
    for option in options:
        path = options[option]
        connection = connect(path)
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        connection.close()

def run_trials(options):
    for option in options:
        print("Avg time for {} entries".format(option))
        avg_time(options[option])
        print("Size of database {}".format(os.stat(options[option]).st_size))
        print("\n")

    



def connect(path) -> Connection:
    # Returns a connection to the database provided at the path.
    db_path = exact_path(path)
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    # To enable foreign keys for SQLite
    cursor.execute(' PRAGMA foreign_keys=ON; ')
    connection.commit()
    return connection


def exact_path(path) -> str:
    # Used to convert relative path to absolute path.
    curr = os.path.dirname(__file__)
    load_path = os.path.join(curr, path)
    return load_path


def run_query(path) -> None:
    connection = connect(path)
    cursor = connection.cursor()
    country_code = get_random_country_code()
    cursor.execute(QUERY_4,{
                "countrycode": country_code})
    connection.commit()
    connection.close()


def load_country_code_data() -> List[str]:
    global country_list
    if(country_list is None):
        data_path = exact_path(COUNTRY_DATA)
        data = []
        added = set()
        with open(data_path) as csvfile:
            datareader = csv.reader(csvfile)
            for row in datareader:
                if(row[0] not in added):
                    data.append(row)
                    added.add(row[0])
            csvfile.close()
        country_list = data[1:]
    return country_list


def get_random_country_code() -> str:
    country_code_list = load_country_code_data()
    rand_country_i = random.randint(0, len(country_code_list)-1)
    country_code = country_code_list[rand_country_i][1]
    return country_code

def avg_time(path) -> None:
    total_time = 0
    for i in range(0,100):
        t_start = time.process_time()
        run_query(path)
        t_taken = time.process_time() - t_start
        total_time += t_taken
    print("Avg time: {}".format(total_time/100))


if __name__ == "__main__":
    main()
