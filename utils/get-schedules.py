"""
Fetches the schedules from Here API for each bus station from table 'stations' and saves the data 
in the table 'departures'. It also updates some fields in the 'stations' table.
This data can be further linked to each bus in order to get their estimated location.
It needs a valid OAuth2 token provided by Here after a successfull authentication.
"""

import datetime
import sqlite3

import requests
from tqdm import tqdm

token = 'replace-me'
dates = {'saturday': '2020-10-31', 'sunday': '2020-11-01', 'weekday': '2020-11-02'}

headers = {'Authorization': 'Bearer ' + token}
params = {'maxPlaces': 1, 'modes': 'bus', 'maxPerBoard': 50}
url = 'https://transit.hereapi.com/v8/departures'

conn = sqlite3.connect('../data/main.db')
cursor = conn.cursor()

stmt_stations = 'SELECT id, coordinates_overpass FROM stations ORDER BY id;'
stmt_station_update = """UPDATE stations SET id_here = ?, name_here = ?, coordinates_here = ?, 
                        no_data = 0, duplicate = 0 WHERE id = ?;"""
stmt_departures = """INSERT INTO departures (station_id, bus, headsign, day, time)
                    VALUES (?, ?, ?, ?, ?);"""
stmt_station_check_stream = 'SELECT id_here FROM stations WHERE id = ?;'
stmt_station_check_duplicate = 'SELECT count(*) FROM stations WHERE id_here = ? OR coordinates_here = ?;'
stmt_count_check = 'SELECT count(*) FROM departures WHERE station_id = ? AND day = ?;'
stmt_station_no_data = 'UPDATE stations SET no_data = 1 WHERE id = ?;'
stmt_station_set_duplicate = 'UPDATE stations SET duplicate = 1 WHERE id = ?;'

cursor.execute(stmt_stations)
stations = cursor.fetchall()

for day, date in tqdm(dates.items()):
    min_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    max_time = min_time + datetime.timedelta(days=1)
    
    for station in tqdm(stations):
        params['in'] = station[1]
        params['time'] = min_time
        
        while params['time'] < max_time:
            cursor.execute(stmt_count_check, (station[0], day))
            
            if cursor.fetchone()[0] > 1440:
                raise Exception('Something went wrong! Too many departures for station {}!'.format(station[0]))

            params['time'] = params['time'].isoformat()
            response = requests.get(url, headers=headers, params=params)
            
            try:
                data = response.json()['boards'][0]
            except:
                cursor.execute(stmt_station_no_data, (station[0],))
                break

            cursor.execute(stmt_station_check_stream, (station[0],))
            id_here = cursor.fetchone()[0]
            
            if id_here is None:
                coordinates_here = ','.join(map(str, [data['place']['location']['lat'], data['place']['location']['lng']]))
                cursor.execute(stmt_station_check_duplicate, (data['place']['id'], coordinates_here))
                
                if cursor.fetchone()[0]:
                    cursor.execute(stmt_station_set_duplicate, (station[0],))
                    break
                
                station_data = (data['place']['id'], data['place']['name'], coordinates_here, station[0])
                cursor.execute(stmt_station_update, station_data)
           
            elif id_here != data['place']['id']:
                raise Exception('Here ID mismatch for station {}!'.format(station[0]))
            
            for departure in data['departures']:
                if datetime.datetime.fromisoformat(departure['time']).replace(tzinfo=None) >= max_time:
                    break
                
                departure_data = (station[0], departure['transport']['name'], departure['transport']['headsign'], day, departure['time'][11:16])
                cursor.execute(stmt_departures, departure_data)

            params['time'] = datetime.datetime.fromisoformat(data['departures'][-1]['time']).replace(tzinfo=None) + datetime.timedelta(minutes=1)
            conn.commit()

cursor.close()
conn.close()
