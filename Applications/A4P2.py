import time
import os
import sqlite3
from sqlite3 import Connection

V100_DB_PATH = "../SQLiteDBs/A4v100.db"
V1K_DB_PATH = "../SQLiteDBs/A4v1k.db"
V10K_DB_PATH = "../SQLiteDBs/A4v10k.db"
V100K_DB_PATH = "../SQLiteDBs/A4v100k.db"
V1M_DB_PATH = "../SQLiteDBs/A4v1M.db"

# Q3: Considering the set of countries that exist in relation Parts, find the average price of the parts made in each country 

# select
#     avg(partPrice)
# from
#     Parts
# group by
#     madeIn;

QUERY_3 = '''
        select
            avg(partPrice)
        from
            Parts
        group by
            madeIn;
    '''

# creating an index on Parts for MadeIn
# CREATE INDEX idxMadeIn ON Parts ( MadeIn );

CREATE_INDEX_QUERY = '''
        CREATE INDEX idxMadeIn ON Parts (MadeIn);
    '''
# drop the index 
# DROP INDEX idxMadeIn;

DROP_INDEX_QUERY = '''
        DROP INDEX idxMadeIn;
    '''
# drop the index query if it exists
# DROP INDEX IF EXISTS idxMadeIn;

DROP_INDEX_QUERY_IF_EXISTS = '''
         DROP INDEX IF EXISTS idxMadeIn;
    '''

Connection = sqlite3.Connection


def connect(path) -> Connection:

    # Returns a connection to the database provided at the path.
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    # To enable foreign keys for SQLite
    cursor.execute(' PRAGMA foreign_keys=ON; ')
    connection.commit()
    return connection


def main():
    options = {"100": V100_DB_PATH, "1K": V1K_DB_PATH,
            "10K": V10K_DB_PATH, "100K": V100K_DB_PATH, "1M": V1M_DB_PATH}
    print("Executing Part 2\n")

    update_index(options, DROP_INDEX_QUERY_IF_EXISTS)

    print("Avg times and sizes for Query 3 without index\n")
    run_trials(options)

    print("Creating index for each database\n")
    update_index(options, CREATE_INDEX_QUERY)

    print("Avg times and sizes for Query 3 with index\n")
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


def run_query(path) -> None:
    connection = connect(path)
    cursor = connection.cursor()
    cursor.execute(QUERY_3)
    connection.commit()
    connection.close()


if __name__ == "__main__":
    main()
