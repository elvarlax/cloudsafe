import numpy 
import folium
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
# https://www.jpytr.com/post/analysinggeographicdatawithfolium/
def get_geojson_grid(upper_right, lower_left, n=6):
    """Returns a grid of geojson rectangles, and computes the exposure in each section of the grid based on the vessel data.

    Parameters
    ----------
    upper_right: array_like
        The upper right hand corner of "grid of grids" (the default is the upper right hand [lat, lon] of the USA).

    lower_left: array_like
        The lower left hand corner of "grid of grids"  (the default is the lower left hand [lat, lon] of the USA).

    n: integer
        The number of rows/columns in the (n,n) grid.

    Returns
    -------

    list
        List of "geojson style" dictionary objects   
    """

    all_boxes = []

    lat_steps = numpy.linspace(lower_left[0], upper_right[0], n+1)
    lon_steps = numpy.linspace(lower_left[1], upper_right[1], n+1)

    lat_stride = lat_steps[1] - lat_steps[0]
    lon_stride = lon_steps[1] - lon_steps[0]

    for lat in lat_steps[:-1]:
        for lon in lon_steps[:-1]:
            # Define dimensions of box in grid
            upper_left = [lon, lat + lat_stride]
            upper_right = [lon + lon_stride, lat + lat_stride]
            lower_right = [lon + lon_stride, lat]
            lower_left = [lon, lat]

            # Define json coordinates for polygon
            coordinates = [
                upper_left,
                upper_right,
                lower_right,
                lower_left,
                upper_left
            ]

            geo_json = {"type": "FeatureCollection",
                        "properties":{
                            "lower_left": lower_left,
                            "upper_right": upper_right
                        },
                        "features":[]}

            grid_feature = {
                "type":"Feature",
                "geometry":{
                    "type":"Polygon",
                    "coordinates": [coordinates],
                }
            }

            geo_json["features"].append(grid_feature)

            all_boxes.append(geo_json)

    return all_boxes

def get_coords(row):
    if row['coordinates_here']:
        coords = row['coordinates_here'].split(',')
        return coords[0], coords[1]
    return None, None

def get_copenhagen_grid(stations):
    coords = stations.iloc[0].coordinates_here.split(',')
    m = folium.Map([coords[0], coords[1]], zoom_start=10)
    stations[['latitude', 'longitude']] = stations.apply(get_coords, axis=1, result_type="expand")
    frequencies = stations.name_overpass.value_counts()
    rows = []
    for station in frequencies.keys():
        first = stations.loc[stations['name_overpass'] == station].iloc[0]
        rows.append({'station_name': station,
                    'longitude': first.longitude,
                    'latitude': first.latitude,
                    'bus_count': frequencies.get(station)})

    final = pd.DataFrame(rows)
    final['longitude'] = final['longitude'].astype(float)
    final['latitude'] = final['latitude'].astype(float)
    top_right = [final['latitude'].max(), final['longitude'].max()]
    bottom_left = [final['latitude'].min(), final['longitude'].min()]
    grid = get_geojson_grid(top_right, bottom_left, n=10)
    counts = []
    for box in grid:
        upper_right = box["properties"]["upper_right"]
        lower_left = box["properties"]["lower_left"]
        items = final.loc[(final['latitude'] <= upper_right[1]) &
                        (final['longitude'] <= upper_right[0]) &
                        (final['longitude'] >= lower_left[0]) &
                        (final['latitude'] >= lower_left[1])]
        counts.append(len(items))
    max_count = max(counts)
    for i, geo_json in enumerate(grid):
        color = plt.cm.Reds(counts[i] / max_count)
        color = mpl.colors.to_hex(color)
        gj = folium.GeoJson(geo_json,
                            style_function=lambda feature, 
                            color=color: {
                                'fillColor': color,
                                'color': "black",
                                'weight': 2,
                                'dashArray': '5, 5',
                                'fillOpacity': 0.55,
                            })

        m.add_child(gj)
    return m