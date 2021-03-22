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
MAIN_DB_PATH = "../SQLiteDBs/main.db"


QUERY_1 = '''
        select
            partPrice
        from
            Parts
        where
            partNumber = :num;
    '''

QUERY_2 = '''
        select
            partPrice
        from
            Parts
        where 
            needsPart = :num;
    '''

CREATE_INDEX_QUERY = '''
        CREATE INDEX idxNeedsPart ON Parts ( needsPart );
    '''

DROP_INDEX_QUERY = '''
        DROP INDEX idxNeedsPart;
    '''

DROP_INDEX_QUERY_IF_EXISTS = '''
         DROP INDEX IF EXISTS idxNeedsPart;
    '''

#setting globals to use list in memory
part_number_list = None
needs_part_list = None


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
    print("Implement P1 here")

    options = {"100": V100_DB_PATH, "1000": V1K_DB_PATH,
               "10000": V10K_DB_PATH, "100000": V100K_DB_PATH, "1000000": V1M_DB_PATH}

    #Drop index if it exists
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


def get_PartNumber():

    global part_number_list

    if part_number_list is None:
        connection = connect(MAIN_DB_PATH)
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

def get_NeedsPart():

    global needs_part_list

    if needs_part_list is None:
        connection = connect(MAIN_DB_PATH)

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

def get_price_of_PartNumber(part_num):

    connection = connect(MAIN_DB_PATH)
    cursor = connection.cursor()
    #Finding price of part given its part number
    cursor.execute(
        '''
        select
            partPrice
        from
            Parts
        where
            partNumber = :num;
        ''',{
            "num":part_num
        }
    )
    connection.commit()
    connection.close()


def get_price_of_NeedsPart(needs_part_num):

    connection = connect(MAIN_DB_PATH)
    cursor = connection.cursor()
    #Finding price of part given the part required
    cursor.execute(
        '''
        select
            partPrice
        from
            Parts
        where
            needsPart = :num;
        ''',{
            "num":needs_part_num
        }
    )
    connection.commit()
    connection.close()
    
def run_PartNumber_query(path) -> None:
    connection = connect(path)
    cursor = connection.cursor()
    part_number = get_PartNumber()
    part_number_price = get_price_of_PartNumber(part_number)
    cursor.execute(QUERY_1, {
        "num": part_number_price})
    connection.commit()
    connection.close()

def run_NeedPart_query(path) -> None:
    connection = connect(path)
    cursor = connection.cursor()
    needs_part = get_NeedsPart()
    needs_part_price = get_price_of_NeedsPart(needs_part)
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
    print("Avg time: {}".format(total_time/100))

def avg_time_NeedPart(path) -> None:
    total_time = 0
    for i in range(0, 100):
        t_start = time.process_time()
        run_NeedPart_query(path)
        t_taken = time.process_time() - t_start
        total_time += t_taken
    print("Avg time: {}".format(total_time/100))

def run_PartNumber_trials(options):
    for option in options:
        print("Avg time for {} entries".format(option))
        avg_time_PartNumber(options[option])
        print("Size of database {}".format(os.stat(options[option]).st_size))
        print("\n")


def run_NeedPart_trials(options):
    for option in options:
        print("Avg time for {} entries".format(option))
        avg_time_NeedPart(options[option])
        print("Size of database {}".format(os.stat(options[option]).st_size))
        print("\n")     
 
if __name__ == "__main__":
    main()