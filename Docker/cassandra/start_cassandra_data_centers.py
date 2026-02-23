import csv

# Read memory from Docker/gcp.csv
memory_gb = 0.0
with open('Docker/gcp.csv', mode='r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        memory_gb = float(row['memory'])  # Assuming memory column has the memory info

# Calculate -Xmx as 3/4 of the memory
xmx_memory = int(memory_gb * 0.75)
if xmx_memory >= 1:
    xmx_str = f'{xmx_memory}g'
else:
    xmx_str = f'{int(xmx_memory * 1024)}m'

# Set JVM_OPTS
JVM_OPTS = f'-Xms2g -Xmx{xmx_str}'

# Rest of the existing logic continues...