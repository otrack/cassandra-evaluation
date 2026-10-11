import csv
import math

import infra

from emulate_latency import load_measured_rtts, one_way_latency

def read_locations(file_path):
    """
    Read lat, lon, and loc from the CSV file.
    """
    locations = []
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            locations.append((float(row['lat']), float(row['lon']), row['loc']))
    return locations

def calculate_ping_matrix(locations):
    """
    Calculate ping time matrix (in milliseconds) for given locations.
    """
    n = len(locations)
    measured_rtts = load_measured_rtts()
    ping_matrix = [[0.0]*n for _ in range(n)]
    
    for i in range(n):
        for j in range(n):
            if i != j:
                ping_matrix[i][j] = 2 * one_way_latency(locations[i], locations[j], measured_rtts)
    return ping_matrix

def print_matrix(locations, ping_matrix):
    """
    Print the ping time matrix in a readable format.
    """
    loc_names = [loc[2] for loc in locations]
    
    # Print header row
    print(" " * 12, end="")
    for loc in loc_names:
        print(f"{loc:>12}", end="")
    print()
    
    # Print each row
    for i, loc in enumerate(loc_names):
        print(f"{loc:<12}", end="")
        for j in range(len(locations)):
            print(f"{ping_matrix[i][j]:>12.0f}", end="")
        print()

def main():
    file_path = infra.locations_file()
    locations = read_locations(file_path)
    ping_matrix = calculate_ping_matrix(locations)
    print_matrix(locations, ping_matrix)

if __name__ == "__main__":
    main()
    
