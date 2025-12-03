import sys
import math
import pandas as pd

from emulate_latency import haversine

def escape_latex(text):
    """Escape special LaTeX characters in a string."""
    replacements = [
        ('\\', '\\textbackslash{}'),
        ('&', '\\&'),
        ('%', '\\%'),
        ('$', '\\$'),
        ('#', '\\#'),
        ('_', '\\_'),
        ('{', '\\{'),
        ('}', '\\}'),
        ('~', '\\textasciitilde{}'),
        ('^', '\\textasciicircum{}'),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    return text

def get_global_latency_range(df, workloads, all_ops, num_nodes):
    min_latency = float('inf')
    max_latency = float('-inf')
    for workload in workloads:
        for op in all_ops:
            dfw = df[(df['workload'] == workload) & (df['nodes'] == num_nodes)
                     & (df['op'] == op)]
            for _, row in dfw.iterrows():
                latencies = [float(row[f'p{i}']) for i in range(1, 101)
                             if pd.notnull(row.get(f'p{i}')) and row[f'p{i}'] != 'unknown']
                if latencies:
                    min_latency = min(min_latency, min(latencies))
                    max_latency = max(max_latency, max(latencies))
    if min_latency == float('inf'): min_latency = 0
    if max_latency == float('-inf'): max_latency = 1
    return min_latency, max_latency

def main():
    if len(sys.argv) < 7:
        print(
            "Usage: python cdf.py results.csv workload1 [workload2 ...] num_nodes city latencies.csv output.tex"
        )
        sys.exit(1)
    results_csv = sys.argv[1]
    workloads = sys.argv[2:-4]
    num_nodes = int(sys.argv[-4])
    city = sys.argv[-3]
    lat_csv = sys.argv[-2]
    output_tikz = sys.argv[-1]

    df = pd.read_csv(results_csv)
    
    # Filter by city if the column exists
    if 'city' in df.columns:
        df_filtered = df[df['city'] == city]
        if df_filtered.empty:
            available_cities = df['city'].unique().tolist()
            print(f"WARNING: No data found for city '{city}'. Available cities: {available_cities}", file=sys.stderr)
            sys.exit(1)
        df = df_filtered
    latdf = pd.read_csv(lat_csv)
    node_lats, node_lons = [], []
    for idx, row in latdf.iloc[:num_nodes].iterrows():
        try:
            node_lats.append(float(row['lat']))
            node_lons.append(float(row['lon']))
        except Exception as e:
            print(f"WARNING: Skipping row {idx+2} in latencies.csv due to error: {e}", file=sys.stderr)
    latlon = list(zip(node_lats, node_lons))

    # Get all unique operations
    all_ops = set()
    for workload in workloads:
        subdf = df[(df['workload'] == workload) & (df['nodes'] == num_nodes) ]
        all_ops.update(subdf['op'].unique())
    all_ops = sorted(list(all_ops))
    n_ops = len(all_ops)
    n_wl = len(workloads)

    # Protocol order for consistent color assignment
    protocol_order = []
    for workload in workloads:
        for op in all_ops:
            dfw = df[(df['workload'] == workload) & (df['nodes'] == num_nodes)
                     & (df['op'] == op)]
            for proto in dfw['protocol'].unique():
                if proto not in protocol_order:
                    protocol_order.append(proto)

    min_latency, max_latency = get_global_latency_range(df, workloads, all_ops, num_nodes)
    xpad = 0.05 * (max_latency - min_latency)
    min_latency = max(0, min_latency - xpad)
    max_latency = max_latency + xpad

    color_cycle = [
        "red", "blue", "green!50!black", "cyan!80!black",
        "magenta!80!black", "yellow!80!black", "black"
    ]

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("    \\centering\n")
        f.write("    \\begin{tikzpicture}\n")

        f.write("      \\begin{groupplot}[\n")
        f.write(f"        group style={{group size={n_ops} by {n_wl}, horizontal sep=1.2cm, vertical sep=1.2cm}},\n")
        f.write("        width=4cm, height=4cm,\n")
        f.write("        grid=both,\n")        
        f.write("        ymajorgrids=true,\n")
        f.write("        xmajorgrids=true,\n")
        f.write("        ymin=0, ymax=1,\n")
        f.write(f"        xmin={min_latency:.2f},\n")
        f.write(f"        xmax={500:.2f},\n")
        f.write("        ytick={0,0.5,1},\n")
        f.write("        cycle list name=color list,\n")
        f.write("      ]\n")

        for wl_index, workload in enumerate(workloads):
            for op_index, op in enumerate(all_ops):
                dfw = df[(df['workload'] == workload) & (df['nodes'] == num_nodes)
                         & (df['op'] == op)]
                f.write("        \\nextgroupplot[\n")
                if op_index == 0:
                    f.write(f"          ylabel={{{workload}}},\n")
                else:
                    f.write("          yticklabels={{}},\n")
                if wl_index == n_wl-1:
                    f.write("          xlabel={{Latency (ms)}},\n")
                if wl_index == 0:
                    f.write(f"          title={{{op}}},\n")
                f.write("        ]\n")

                if dfw.empty:
                    continue

                for proto_idx, proto in enumerate(protocol_order):                    
                    if proto not in dfw['protocol'].unique():
                        continue
                    row = dfw[dfw['protocol'] == proto].iloc[0] if not dfw[dfw['protocol'] == proto].empty else None
                    if row is None:
                        continue
                    latencies = [row[f'p{i}'] for i in range(1, 101)
                                 if pd.notnull(row.get(f'p{i}')) and row[f'p{i}'] != 'unknown']
                    if not latencies:
                        continue
                    col = color_cycle[proto_idx % len(color_cycle)]
                    f.write("          \\addplot+["+col+", mark=none] table {\n")
                    for i, val in enumerate(latencies):
                        pct = i/99
                        f.write(f"          {val} {pct}\n")
                    f.write("          };\n")

        f.write("      \\end{groupplot}\n")
        f.write("    \\end{tikzpicture}\n")

        # --- Caption with color swatches ---
        city_escaped = escape_latex(city)
        f.write(f"    \\caption{{CDF of operation latencies for different YCSB workloads and Cassandra protocols at {city_escaped}. ")
        for proto_idx, proto in enumerate(protocol_order):
            col = color_cycle[proto_idx % len(color_cycle)]
            f.write(r"\protect\tikz \protect\draw[thick, {color}] (0,0) -- +(0.8,0);~{{{proto}}}".format(color=col, proto=proto))
            if proto_idx < len(protocol_order) - 1:
                f.write(", ")
        f.write("    \\label{fig:workload-cdf}}\n")
        f.write("\\end{figure}\n")

if __name__ == "__main__":
    main()
    
