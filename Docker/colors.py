"""Utility to load the unified protocol-to-color mapping from protocols.csv."""

import csv
import os

DEFAULT_COLOR_CYCLE = [
    "red", "blue", "green!50!black", "cyan!80!black",
    "magenta!80!black", "yellow!80!black", "black",
]


def load_protocol_colors(csv_path=None):
    """Load protocol -> LaTeX color mapping from protocols.csv.

    Returns a dict {protocol_name: latex_color_string}.
    Falls back to an empty dict if the file is not found or cannot be parsed.
    """
    if csv_path is None:
        csv_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "protocols.csv"
        )
    colors = {}
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                proto = row.get("protocol", "").strip()
                color = row.get("color", "").strip()
                if proto and color:
                    colors[proto] = color
    except (FileNotFoundError, IOError):
        pass
    return colors


def get_protocol_color(protocol, protocol_colors, fallback_idx=0):
    """Return the LaTeX color for *protocol*.

    Looks up *protocol_colors* (from :func:`load_protocol_colors`) first;
    falls back to :data:`DEFAULT_COLOR_CYCLE` using *fallback_idx* when the
    protocol is not listed in the CSV.
    """
    if protocol in protocol_colors:
        return protocol_colors[protocol]
    return DEFAULT_COLOR_CYCLE[fallback_idx % len(DEFAULT_COLOR_CYCLE)]
