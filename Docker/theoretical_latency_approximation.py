from start_cassandra_cluster import locations_lat_long, haversine, estimate_latency
import math

def one_wide_area_round_trip(node_count: int) -> float:
    # result = 0
    # count = 0
    # quorum_size = math.ceil((node_count + 1) / 2)
    # for i in range(node_count):
    #     current_pos = locations_lat_long[i]
    #     latency_list = []
    #     for j in range(node_count):
    #         rep_pos = locations_lat_long[j]
    #         latency_list.append(estimate_latency(haversine(current_pos[0], current_pos[1], rep_pos[0], rep_pos[1])))
    #     latency_list.sort()
    #     result += latency_list[quorum_size - 1] * 2
    #     count += 1
    # result /= count
    # return result
    quorum_size = math.ceil((node_count + 1) / 2)
    current_pos = locations_lat_long[node_count - 1]
    latency_list = []
    for i in range(node_count - 1):
        rep_pos = locations_lat_long[i]
        latency_list.append(estimate_latency(haversine(current_pos[0], current_pos[1], rep_pos[0], rep_pos[1])))
    latency_list.sort()
    print(latency_list)
    return latency_list[quorum_size - 1] * 2

def quorum_estimation(node_count: int) -> float:
    return one_wide_area_round_trip(node_count)

def paxos_operaion_estimation(node_count: int) -> float:
    return one_wide_area_round_trip(node_count) * 3.5

def accord_operation_estimation_fast_path(node_count: int) -> float:
    result = 0
    count = 0
    quorum_size = math.ceil((node_count * 3) / 4)
    for i in range(node_count):
        current_pos = locations_lat_long[i]
        latency_list = []
        for j in range(node_count):
            rep_pos = locations_lat_long[j]
            latency_list.append(estimate_latency(haversine(current_pos[0], current_pos[1], rep_pos[0], rep_pos[1])))
        latency_list.sort()
        result += latency_list[quorum_size - 1] * 2
        count += 1
    result /= count
    return result

def accord_operation_estimation_slow_path(node_count: int) -> float:
    return one_wide_area_round_trip(node_count) * 4

def read_paxos_estimation(node_count: int) -> float:
    return one_wide_area_round_trip(node_count)

def write_paxos_estimation(node_count: int) -> float:
    return one_wide_area_round_trip(node_count) * 2

def accord_estimation(node_count: int, fast_path_quotient: float = 0.3) -> float:
    assert (fast_path_quotient > 0. and fast_path_quotient <= 1.)
    return accord_operation_estimation_fast_path(node_count) * fast_path_quotient + (1 - fast_path_quotient) * accord_operation_estimation_slow_path(node_count)

print(one_wide_area_round_trip(3))
