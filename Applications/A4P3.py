import time
import os
import random
import sqlite3
from sqlite3 import Connection
from typing import List

V100_DB_PATH = "../SQLiteDBs/A4v100.db"
V1K_DB_PATH = "../SQLiteDBs/A4v1k.db"
V10K_DB_PATH = "../SQLiteDBs/A4v10k.db"
V100K_DB_PATH = "../SQLiteDBs/A4v100k.db"
V1M_DB_PATH = "../SQLiteDBs/A4v1M.db"


# Q4: Find the most expensive part made in a randomly selected country (code) that exist in Parts. 

# select
#     p1.partNumber
# from
#     Parts p1
# where
#     p1.madeIn = :countrycode
#     and p1.partPrice = (
#         select
#             max(partPrice)
#         from
#             Parts p2
#         where
#             p2.madeIn = :countrycode
#     )
# limit   1;

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

# Creates an index for Q4

# CREATE INDEX idxPartPriceMadeIn ON Parts ( madeIn, partPrice );


CREATE_INDEX_QUERY = '''
    CREATE INDEX idxPartPriceMadeIn ON Parts ( madeIn, partPrice );
'''

# Drops the index for Q4

# DROP INDEX idxPartPriceMadeIn;

DROP_INDEX_QUERY = '''
    DROP INDEX idxPartPriceMadeIn;
'''
country_list = None


def main():
    options = {"100": V100_DB_PATH, "1K": V1K_DB_PATH,
               "10K": V10K_DB_PATH, "100K": V100K_DB_PATH, "1M": V1M_DB_PATH}
    print("Executing Part 3\n")

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
    global country_list
    for option in options:
        print("Avg time for {} entries".format(option))
        avg_time(options[option])
        print("Size of database {}".format(os.stat(options[option]).st_size))
        print("\n")
        country_list = None


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
    country_code = get_random_country_code(path)
    cursor.execute(QUERY_4, {
        "countrycode": country_code})
    connection.commit()
    connection.close()


def load_country_code_data(path) -> List[str]:
    global country_list
    if(country_list is None):
        connection = connect(path)
        cursor = connection.cursor()

        cursor.execute(
            '''
            select
                madeIn
            from
                Parts;
            '''
        )
        rows = (cursor.fetchall())
        country_list = [', '.join(map(str, x)) for x in rows]
        connection.commit()
        connection.close()

    return country_list


def get_random_country_code(path) -> str:
    country_code_list = load_country_code_data(path)
    rand_country_i = random.randint(0, len(country_code_list)-1)
    country_code = country_code_list[rand_country_i]
    return country_code


def avg_time(path) -> None:
    total_time = 0
    for i in range(0, 100):
        t_start = time.process_time()
        run_query(path)
        t_taken = time.process_time() - t_start
        total_time += t_taken
    # to get the average for total_time
    total_time = total_time/100
    # display in ms
    print("Avg time: {} ms".format(total_time*1000))


if __name__ == "__main__":
    main()