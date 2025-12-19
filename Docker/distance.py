import csv
import math

from emulate_latency import haversine
from emulate_latency import estimate_latency

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
    ping_matrix = [[0.0]*n for _ in range(n)]
    
    for i in range(n):
        for j in range(n):
            if i != j:
                lat1, lon1, _ = locations[i]
                lat2, lon2, _ = locations[j]
                distance_km = haversine(lat1, lon1, lat2, lon2)
                
                ping_matrix[i][j] = estimate_latency(distance_km*2)
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
    file_path = 'latencies.csv'  # Change file path if needed
    locations = read_locations(file_path)
    ping_matrix = calculate_ping_matrix(locations)
    print_matrix(locations, ping_matrix)

if __name__ == "__main__":
    main()
    
