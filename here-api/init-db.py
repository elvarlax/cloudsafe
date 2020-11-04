import sqlite3

conn = sqlite3.connect('main.db')
cursor = conn.cursor()

stmt = """CREATE TABLE stations (id INTEGER PRIMARY KEY, id_overpass TEXT NOT NULL UNIQUE, 
        name_overpass TEXT, coordinates_overpass TEXT NOT NULL UNIQUE, id_here TEXT UNIQUE, 
        name_here TEXT, coordinates_here TEXT UNIQUE, no_data INTEGER, duplicate INTEGER);"""

cursor.execute(stmt)

stmt = """CREATE TABLE departures (id INTEGER PRIMARY KEY, station_id INTEGER NOT NULL, 
        bus TEXT NOT NULL, headsign TEXT, day TEXT NOT NULL, time TEXT NOT NULL, 
        FOREIGN KEY(station_id) REFERENCES stations(id));"""

cursor.execute(stmt)

conn.commit()
cursor.close()
conn.close()
