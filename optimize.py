import itertools

import data_api as api

# Initialisation
# def init()

# Anealing schedule
# def schedule()

# Candidate generator
# def neighbour(s):
#    s_new = []
#
#    return s_new
#
# Acceptance probability function
# def P():

# Energy (Goal) function
# def E():


# api.get_bus_ids()

all_bus_routes = api.get_route_cells(api.get_all_bus_ids())
domain_all_bus_routes = list(all_bus_routes.values())
range_all_bus_routes = list(all_bus_routes.keys())
total_grid_amount = 100


def get_max_coverage_single_bus():
    bus_coverage = 0
    bus_id = 0
    bus_route = []

    for route in domain_all_bus_routes:
        if len(route) >= max:
            bus_id = range_all_bus_routes[domain_all_bus_routes.index(route)]
            bus_coverage = len(route) / total_grid_amount
            bus_route = route

    return (bus_id, bus_coverage, bus_route)


def get_bus_coverage(bus_id):
    return len(all_bus_routes[bus_id]) / total_grid_amount


def get_bus_coverage_combined(bus_list):
    cell_set = set()

    for bus_id in bus_list:
        try:
            bus_route = set(all_bus_routes[bus_id])
        except:
            bus_route = set()

        cell_set = cell_set.union(bus_route)

    return len(cell_set) / total_grid_amount


if __name__ == '__main__':
    stuff = api.get_all_bus_ids()
    coverage = 0
    comb = []

    for subset in itertools.combinations(stuff, 2):
        cur_coverage = get_bus_coverage_combined(list(subset))
        if coverage < cur_coverage:
            coverage = cur_coverage
            comb = list(subset)

    print("Best combination: ", comb)
    print("Coverage: ", coverage)
