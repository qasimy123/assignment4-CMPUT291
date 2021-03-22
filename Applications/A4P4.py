

#!/usr/bin/env python3
import time
import os
import sqlite3
from sqlite3 import Connection
from typing import List

V100_DB_PATH = "../SQLiteDBs/A4v100.db"
V1K_DB_PATH = "../SQLiteDBs/A4v1k.db"
V10K_DB_PATH = "../SQLiteDBs/A4v10k.db"
V100K_DB_PATH = "../SQLiteDBs/A4v100k.db"
V1M_DB_PATH = "../SQLiteDBs/A4v1M.db"

# Q5: Find the quantity of parts that are not used in any other part, your query must use EXISTS.
QUERY_5 = '''
    select
        count(partNumber)
    from
        Parts p
    where
        exists (
            select
                1
            from
                Parts p2
            where
                p.partNumber = p2.partNumber
                and p2.partNumber not in (
                    select
                        needsPart
                    from
                        Parts p2
                )
    );
    '''

# Q6: Find the quantity of parts that are not used in any other part, your query must use NOT IN.
QUERY_6 = '''
    select
        count(partNumber)
    from
        Parts p
    where
        p.partNumber not in (
            select
                needsPart
            from
                Parts p2
        );
    '''
# Creates a covering index that sqlite will use to efficiently find  partNumber and needsPart
CREATE_INDEX_QUERY = '''
    CREATE INDEX idxPartNumberNeedsPart on Parts ( needsPart, partNumber );
'''

DROP_INDEX_QUERY = '''
    DROP INDEX idxPartNumberNeedsPart;
'''
country_list = None


def main():
    options = {"100": V100_DB_PATH, "1000": V1K_DB_PATH,
               "10000": V10K_DB_PATH, "100000": V100K_DB_PATH, "1000000": V1M_DB_PATH}

    print("Avg times and sizes for Query 5 without index\n")
    run_trials(options, QUERY_5)

    print("Avg times and sizes for Query 6 without index\n")
    run_trials(options, QUERY_6)

    print("Creating index for each database")

    update_index(options, CREATE_INDEX_QUERY)

    print("Avg times and sizes for Query 6 with index\n")
    run_trials(options, QUERY_6)

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


def run_trials(options, query):
    for option in options:
        print("Avg time for {} entries".format(option))
        avg_time(options[option], query)
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


def run_query(path, query) -> None:
    connection = connect(path)
    cursor = connection.cursor()
    cursor.execute(query, {})
    connection.commit()
    connection.close()


def avg_time(path, query) -> None:
    total_time = 0
    for i in range(0, 100):
        t_start = time.process_time()
        run_query(path, query)
        t_taken = time.process_time() - t_start
        total_time += t_taken
    print("Avg time: {}".format(total_time/100))


if __name__ == "__main__":
    main()
