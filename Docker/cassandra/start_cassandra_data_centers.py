import csv
import os

# Determine paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, '..', 'exp.config')
GCP_CSV = os.path.join(SCRIPT_DIR, '..', 'gcp.csv')

# Read machine type from exp.config
machine = None
with open(CONFIG_FILE, mode='r') as f:
    for line in f:
        line = line.strip()
        if line.startswith('machine='):
            machine = line.split('=', 1)[1]
            break

if not machine:
    raise ValueError("machine not defined in exp.config")

# Read memory for the machine from gcp.csv
memory_gb = None
with open(GCP_CSV, mode='r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row['name'] == machine:
            memory_gb = float(row['memory'])
            break

if memory_gb is None:
    raise ValueError(f"Machine type '{machine}' not found in gcp.csv")

# Calculate -Xmx as 3/4 of the memory
xmx_memory = memory_gb * 0.75
if xmx_memory >= 1 and xmx_memory == int(xmx_memory):
    xmx_str = f'{int(xmx_memory)}g'
else:
    xmx_str = f'{int(xmx_memory * 1024)}m'

# Set JVM_OPTS
JVM_OPTS = f'-Xms2g -Xmx{xmx_str}'

# Rest of the existing logic continues...