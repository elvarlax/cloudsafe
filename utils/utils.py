
__author__      = "Ahmed Khalil"
__date__        = "05.11.2019"
__version__     = "0.0.1"

import pandas as pd
import osmnx as ox
import requests, json
import random, time
import pandas as pd
from geopy import Nominatim
from geopy.exc import GeocoderTimedOut
import geopandas as gpd
import networkx as nx
from math import sin, cos, sqrt, atan2, radians


def get_nearestedge_node(temp_y, temp_x, G):
    """
    Find the nearest node available in Open street map
    Parameters
    ----------
    osm_id : node ID
    a : plotting graph
    g : bus graph
    Returns
    -------
    temp_nearest_edge[1]/temp_nearest_edge[2] : nearest node to a way ID
    """
    temp_nearest_edge = ox.get_nearest_edge(G, (temp_y, temp_x))
    temp_1 = temp_nearest_edge[0].coords[0]
    temp_2 = temp_nearest_edge[0].coords[1]
    temp1_x = temp_1[0]
    temp1_y = temp_1[1]
    temp_1_distance = calculate_H(temp1_y, temp1_x, temp_y, temp_x)

    temp2_x = temp_2[0]
    temp2_y = temp_2[1]
    temp_2_distance = calculate_H(temp2_y, temp2_x, temp_y, temp_x)
    if temp_1_distance < temp_2_distance:
        return temp_nearest_edge[1]
    else:
        return temp_nearest_edge[2]


def calculate_H(s_lat, s_lon, e_lat, e_lon):
    """
    Calculate a distance with x,y coordinates with
    Parameters
    ----------
    s_lat : float (starting lat)
    s_lon : float (starting lon)
    e_lat : float (ending lat)
    e_lon : float (ending lon)
    Returns
    -------
    distance
    """
    snlat = radians(s_lat)
    snlon = radians(s_lon)
    elat = radians(e_lat)
    elon = radians(e_lon)
    actual_dist = 6371.01 * acos(sin(snlat) * sin(elat) + cos(snlat) * cos(elat) * cos(snlon - elon))
    actual_dist = actual_dist * 1000
    return actual_dist



def dijkstras(graph, start, end, cost_per_trans):
    """
    Dijkstra algorithm to find the shortest path and taking into account least transfer
    """

    seen = set()
    # maintain a queue of paths
    queue = []
    # push the first path into the queue
    heapq.heappush(queue, (0, 0, 0, [(start, None)]))

    while queue:

        # get the first path from the queue
        (curr_cost, curr_dist, curr_trans, path) = heapq.heappop(queue)
        
        # get the last node from the path
        (node, curr_service) = path[-1]

        # path found
        if node == end:
            return (curr_dist, curr_trans, path)

        if (node, curr_service) in seen:
            continue

        seen.add((node, curr_service))

        # enumerate all adjacent nodes, construct a new path and push it into the queue
        for (adjacent, service), distance in graph.get(node, {}).items():
            new_path = list(path)
            new_path.append((adjacent, service))
            new_dist = curr_dist + distance
            new_cost = distance + curr_cost
            new_trans = curr_trans
            if curr_service != service:
                new_cost += cost_per_trans
                new_trans += 1
            heapq.heappush(queue, (new_cost, new_dist, new_trans, new_path))


def convertRoute(coords):
    """
    Flip the coordinates of the linestring
    """
    output = []
    for x in range(len(coords)):  # Parent Array
        for i in range(len(coords[x])):  # Inner Array
            output.append([coords[x][i][1], coords[x][i][0]])
    return output




def bus_route(startOsmid, endOsmid, cost_per_trans):
    """
    Running the bus routing algorithm
    """
    
    dijkstra_result=[]
    bus_route_name_service=[]
    plotting_routes = []
    lineStrings = []
    plotting_nodes = []
    route_coordinates = []
    routes_map = {}
    graph = {}

    """
    Preparing the relevent data sets to be used
    """
    stops = json.loads(open("../data/bus_stops.json").read())
    routes = json.loads(open("../data/bus_routes.json").read())
    df = pd.read_csv("../data/bus_stop.csv")
    stop_code_map = {stop["BusStopCode"]: stop for stop in stops}

    """
    Converting the osmid into bus stops code to run in the dijkstra algorithm
    """
    for idx,x in enumerate(df["osmid"]):    
        if str(startOsmid)==str(x):
            startBusStops=df["asset_ref"][idx]
            route_coordinates.append([df["y"][idx],df["x"][idx]])
        if str(endOsmid)==str(x):
            endBusStops=df["asset_ref"][idx]

    """
    Creates the graph needed to run djikstra on
    """
    for route in routes:
        key = (route["ServiceNo"], route["Direction"])
        if key not in routes_map:
            routes_map[key] = []
        routes_map[key] += [route]

    for service, path in routes_map.items():
        path.sort(key=lambda r: r["StopSequence"])
        for route_idx in range(len(path) - 1):
            key = path[route_idx]["BusStopCode"]
            if key not in graph:
                graph[key] = {}
            curr_route_stop = path[route_idx]
            next_route_stop = path[route_idx + 1]
            curr_dist = curr_route_stop["Distance"] or 0
            next_dist = next_route_stop["Distance"] or curr_dist
            dist = next_dist - curr_dist
            assert dist >= 0, (curr_route_stop, next_route_stop)
            curr_code = curr_route_stop["BusStopCode"]
            next_code = next_route_stop["BusStopCode"]
            graph[curr_code][(next_code, service)] = dist

    """
    Calling the dijkstra function and storing the result
    """
    (distance, transfers, path) = dijkstras(graph, startBusStops, endBusStops, cost_per_trans)
    dijkstra_result.append([len(path), distance, transfers])

    """
    Generating the path output and storing the the x and y coordinates in an array
    """
    for code, service in path:
        if service != None:
            bus_service, b = service
            bus_route_name_service.append([bus_service, stop_code_map[code]["Description"],stop_code_map[code]["BusStopCode"],stop_code_map[code]["Latitude"], stop_code_map[code]["Longitude"]])
    """
    Fixing inaccurate coordinates from datamall data set
    """
    for y in bus_route_name_service:    
        for idx,x in enumerate(df["asset_ref"]):    
            if str(y[2])==str(x):
                y[3]=df["y"][idx]
                y[4]=df["x"][idx]
                route_coordinates.append([df["y"][idx], df["x"][idx]])

    """
    Creating the graph using osmnx
    """
    a = ox.graph_from_point((1.3984, 103.9072), distance=3000, network_type='drive_service')
    
    """
    Generating the nodes nearest to bus stop coordinates
    """
    for i in route_coordinates:
        plotting_nodes.append(get_nearestedge_node(i[0], i[1], a))
    
    """
    Generating the path to plot on the map
    """
    for x in range(0, len(plotting_nodes) - 1):
        plotting_routes.append(nx.shortest_path(a, plotting_nodes[x], plotting_nodes[x + 1]))

    """
    Converting the nodes into coordinates to plot on the GUI map
    """
    for x in plotting_routes:
        
        lineStrings.append(convertRoute(ox.node_list_to_coordinate_lines(a, x)))
        
    return lineStrings, dijkstra_result, bus_route_name_service

  
def parse_osm_nodes_paths(osm_data):
    """
    Construct dicts of nodes and paths with key=osmid and value=dict of
    attributes.
    Parameters
    ----------
    osm_data : dict
    JSON response from from the Overpass API
    Returns
    -------
    nodes, paths : tuple
    """

    nodes = {}
    paths = {}
    for element in osm_data['elements']:
        if element['type'] == 'node':
            key = element['id']
            nodes[key] = get_node(element)
        elif element['type'] == 'way':  # osm calls network paths 'ways'
            key = element['id']
            paths[key] = ox.get_path(element)

    return nodes, paths


def random_coords(center_coords, count):
    """
    generate random lat lon coords within a closed range
    """
    lat = []
    lon = []
    
    for i in range(count):
        #set random generator
        lat.append(random.uniform(center_coords[0]-0.1, center_coords[0]+0.1))
        lon.append(random.uniform(center_coords[1]-0.1, center_coords[1]+0.1))
    
    return lat, lon

