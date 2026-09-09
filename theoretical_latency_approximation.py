import math

# Moved here from start_cassandra_cluster.py, which has been removed.
#
# Note that estimate_latency below assumes 200 km/ms whereas emulate_latency.py
# assumes 204 km/ms; the two were never reconciled, so the original constant is
# kept to leave the estimates this script prints unchanged.

locations_lat_long = [
    (21.027763, 105.834160), # Hanoi
    (45.764042, 4.835659), # Lyon
    (40.712776, -74.005974), # New York
    (39.904202, 116.407394), # Beijing
    (19.075983, 72.877655), # Mumbai
    (51.924419, 4.477733), # Rotterdam
    (31.968599, -99.901810), # Texas
    (-23.550520, -46.633308), # Sao Paulo
    (51.507351, -0.127758), # London
    (1.352083, 103.819839), # Singapore
    (35.689487, 139.691711), # Tokyo
    (32.776665, -96.796989) # Dallas
]

def haversine(lat1, lon1, lat2, lon2):
    # Calculate the great-circle distance between two points on the Earth
    R = 6371  # Earth radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def estimate_latency(distance_km):
    # Speed of light in fiber optics is approximately 200,000 km/s
    speed_of_light_km_per_ms = 200  # km/ms
    latency_ms = distance_km / speed_of_light_km_per_ms

    return math.floor(latency_ms)

def one_wide_area_round_trip(dc_count: int) -> float:
    # result = 0
    # count = 0
    # quorum_size = math.ceil((dc_count + 1) / 2)
    # for i in range(dc_count):
    #     current_pos = locations_lat_long[i]
    #     latency_list = []
    #     for j in range(dc_count):
    #         rep_pos = locations_lat_long[j]
    #         latency_list.append(estimate_latency(haversine(current_pos[0], current_pos[1], rep_pos[0], rep_pos[1])))
    #     latency_list.sort()
    #     result += latency_list[quorum_size - 1] * 2
    #     count += 1
    # result /= count
    # return result
    quorum_size = math.ceil((dc_count + 1) / 2)
    current_pos = locations_lat_long[dc_count - 1]
    latency_list = []
    for i in range(dc_count - 1):
        rep_pos = locations_lat_long[i]
        latency_list.append(estimate_latency(haversine(current_pos[0], current_pos[1], rep_pos[0], rep_pos[1])))
    latency_list.sort()
    print(latency_list)
    return latency_list[quorum_size - 1] * 2

def quorum_estimation(dc_count: int) -> float:
    return one_wide_area_round_trip(dc_count)

def paxos_operaion_estimation(dc_count: int) -> float:
    return one_wide_area_round_trip(dc_count) * 3.5

def accord_operation_estimation_fast_path(dc_count: int) -> float:
    result = 0
    count = 0
    quorum_size = math.ceil((dc_count * 3) / 4)
    for i in range(dc_count):
        current_pos = locations_lat_long[i]
        latency_list = []
        for j in range(dc_count):
            rep_pos = locations_lat_long[j]
            latency_list.append(estimate_latency(haversine(current_pos[0], current_pos[1], rep_pos[0], rep_pos[1])))
        latency_list.sort()
        result += latency_list[quorum_size - 1] * 2
        count += 1
    result /= count
    return result

def accord_operation_estimation_slow_path(dc_count: int) -> float:
    return one_wide_area_round_trip(dc_count) * 4

def read_paxos_estimation(dc_count: int) -> float:
    return one_wide_area_round_trip(dc_count)

def write_paxos_estimation(dc_count: int) -> float:
    return one_wide_area_round_trip(dc_count) * 2

def accord_estimation(dc_count: int, fast_path_quotient: float = 0.3) -> float:
    assert (fast_path_quotient > 0. and fast_path_quotient <= 1.)
    return accord_operation_estimation_fast_path(dc_count) * fast_path_quotient + (1 - fast_path_quotient) * accord_operation_estimation_slow_path(dc_count)

print(one_wide_area_round_trip(3))
