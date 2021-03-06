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

# Q1: Given a randomly selected UPC code U from the UPC database that exist in 
# Parts find the price of part in Parts that has partNumber = U

# num = partNumber in this query:

# select
#     partPrice
# from
#     Parts
# where
#     partNumber = :num;

QUERY_1 = '''
        select
            partPrice
        from
            Parts
        where
            partNumber = :num;
    '''
# Q2: Given a randomly selected UPC code U from the UPC database 
# that exist in Parts find the price of part in Parts that has needsPart = U

# num = needsPart in this query:

# select
#     partPrice
# from
#     Parts
# where 
#     needsPart = :num;

QUERY_2 = '''
        select
            partPrice
        from
            Parts
        where 
            needsPart = :num;
    '''
    
# creating an index on Parts for needsPart
# CREATE INDEX idxNeedsPart ON Parts ( needsPart );

CREATE_INDEX_QUERY = '''
        CREATE INDEX idxNeedsPart ON Parts ( needsPart );
    '''
# drop the index
# DROP INDEX idxNeedsPart;

DROP_INDEX_QUERY = '''
        DROP INDEX idxNeedsPart;
    '''

# drop the index if it exists
# DROP INDEX IF EXISTS idxNeedsPart;

DROP_INDEX_QUERY_IF_EXISTS = '''
         DROP INDEX IF EXISTS idxNeedsPart;
    '''

# setting globals to use list in memory
part_number_list = None
needs_part_list = None

# setting a connection to sqlite3
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
    print("Executing Part 1\n")

    options = {"100": V100_DB_PATH, "1K": V1K_DB_PATH,
               "10K": V10K_DB_PATH, "100K": V100K_DB_PATH, "1M": V1M_DB_PATH}

    # Drop index if it exists
    update_index(options, DROP_INDEX_QUERY_IF_EXISTS)

    print("Avg times and sizes for Query 1 without index\n")
    run_PartNumber_trials(options)

    print("Avg times and sizes for Query 2 without index\n")
    run_NeedPart_trials(options)

    print("Creating index for each database\n")
    update_index(options, CREATE_INDEX_QUERY)

    print("Avg times and sizes for Query 1 with index\n")
    run_PartNumber_trials(options)

    print("Avg times and sizes for Query 2 with index\n")
    run_NeedPart_trials(options)

    print('Dropping index for each database')
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


def get_PartNumber(path):


    # returns a random NeedPart number from the db:

        # select
        #     needsPart
        # from
        #     Parts;

    global part_number_list

    if part_number_list is None:
        connection = connect(path)
        cursor = connection.cursor()

        cursor.execute(
            '''
            select
                partNumber
            from
                Parts;
            '''
        )
        rows = (cursor.fetchall())
        part_number_list = [', '.join(map(str, x)) for x in rows]

        connection.commit()
        connection.close()

    part_number = random.choice(part_number_list)
    return part_number


def get_NeedsPart(path):

    # returns a random NeedPart number from the db:

            # select
            #     needsPart
            # from
            #     Parts;

    global needs_part_list

    if needs_part_list is None:
        connection = connect(path)

        cursor = connection.cursor()
        cursor.execute(
            '''
            select
                needsPart
            from
                Parts;
            '''
        )
        rows = (cursor.fetchall())
        needs_part_list = [', '.join(map(str, x)) for x in rows]
        connection.commit()
        connection.close()

    needs_part_number = random.choice(needs_part_list)
    return needs_part_number


def get_price_of_PartNumber(part_num, path):

    connection = connect(path)
    cursor = connection.cursor()

    cursor.execute(QUERY_1,{
            "num": part_num}
    )
    connection.commit()
    connection.close()


def get_price_of_NeedsPart(needs_part_num, path):

    connection = connect(path)
    cursor = connection.cursor()
    
    cursor.execute(QUERY_2,{
            "num": needs_part_num}
    )
    connection.commit()
    connection.close()


def run_PartNumber_query(path) -> None:
    connection = connect(path)
    cursor = connection.cursor()
    part_number = get_PartNumber(path)
    part_number_price = get_price_of_PartNumber(part_number, path)
    cursor.execute(QUERY_1, {
        "num": part_number_price})
    connection.commit()
    connection.close()


def run_NeedPart_query(path) -> None:
    connection = connect(path)
    cursor = connection.cursor()
    needs_part = get_NeedsPart(path)
    needs_part_price = get_price_of_NeedsPart(needs_part, path)
    cursor.execute(QUERY_2, {
        "num": needs_part_price})
    connection.commit()
    connection.close()


def avg_time_PartNumber(path) -> None:
    total_time = 0
    for i in range(0, 100):
        t_start = time.process_time()
        run_PartNumber_query(path)
        t_taken = time.process_time() - t_start
        total_time += t_taken
    # to get the average for total_time
    total_time = total_time/100
    # display in ms
    print("Avg time: {} ms".format(total_time*1000))


def avg_time_NeedPart(path) -> None:
    total_time = 0
    for i in range(0, 100):
        t_start = time.process_time()
        run_NeedPart_query(path)
        t_taken = time.process_time() - t_start
        total_time += t_taken
    # to get the average for total_time
    total_time = total_time/100
    # display in ms
    print("Avg time: {} ms".format(total_time*1000))


def run_PartNumber_trials(options):
    for option in options:
        print("Avg time for {} entries".format(option))
        avg_time_PartNumber(options[option])
        print("Size of database {}".format(os.stat(options[option]).st_size))
        print("\n")


def run_NeedPart_trials(options):
    global needs_part_list
    global part_number_list
    for option in options:
        print("Avg time for {} entries".format(option))
        avg_time_NeedPart(options[option])
        print("Size of database {}".format(os.stat(options[option]).st_size))
        print("\n")
        needs_part_list = None
        part_number_list = None


if __name__ == "__main__":
    main()
