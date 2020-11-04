from pprint import pprint
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


def select_all_departures(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM departures")
    data = cursor.fetchall()
    return data


def select_all_stations(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM stations")
    data = cursor.fetchall()
    return data


# Only for testing (Use the Jupyter Notebook for Visualization)
if __name__ == "__main__":
    database = 'here-api/main.db'
    conn = create_connection(database)

    departures = select_all_departures(conn)
    stations = select_all_stations(conn)

    pprint(departures)
    pprint(stations)

    conn.close()
