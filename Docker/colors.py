"""Utility to load the unified protocol-to-color mapping from protocols.csv."""

import csv
import math
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


def load_protocol_aliases(csv_path=None):
    """Load protocol -> alias mapping from protocols.csv.

    Returns a dict {protocol_name: alias_string}.
    Falls back to an empty dict if the file is not found or cannot be parsed.
    """
    if csv_path is None:
        csv_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "protocols.csv"
        )
    aliases = {}
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                proto = row.get("protocol", "").strip()
                alias = row.get("alias", "").strip()
                if proto and alias:
                    aliases[proto] = alias
    except (FileNotFoundError, IOError):
        pass
    return aliases


def get_protocol_color(protocol, protocol_colors, fallback_idx=0):
    """Return the LaTeX color for *protocol*.

    Looks up *protocol_colors* (from :func:`load_protocol_colors`) first;
    falls back to :data:`DEFAULT_COLOR_CYCLE` using *fallback_idx* when the
    protocol is not listed in the CSV.
    """
    if protocol in protocol_colors:
        return protocol_colors[protocol]
    return DEFAULT_COLOR_CYCLE[fallback_idx % len(DEFAULT_COLOR_CYCLE)]


def _escape_latex(text):
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


def make_protocol_legend(protocol_order, protocol_colors, indent="  ",
                         protocol_aliases=None):
    """Generate LaTeX code for a balanced colored-bar protocol legend header.

    Produces one or two centered lines of colored-bar entries placed between
    ``\\centering`` and ``\\begin{tikzpicture}`` in the figure body.

    When there are more than four protocols the entries are split evenly:
    the first line gets ``ceil(n/2)`` entries and the second line gets
    ``floor(n/2)`` entries so neither line is disproportionately short.

    *protocol_aliases* is an optional dict mapping protocol names to their
    display aliases (e.g. from :func:`load_protocol_aliases`).  When provided,
    each entry in the legend uses the alias rather than the raw protocol name.

    Returns a string ready to be written directly to the output file.
    """
    if protocol_aliases is None:
        protocol_aliases = {}
    entries = []
    for idx, proto in enumerate(protocol_order):
        col = get_protocol_color(proto, protocol_colors, idx)
        label = protocol_aliases.get(proto, proto)
        entries.append(
            r"\protect\tikz \protect\draw[thick, {color}] (0,0) -- +(0.5,0);"
            r"~\texttt{{{label}}}".format(color=col, label=_escape_latex(label))
        )

    n = len(entries)
    if n == 0:
        return ""

    if n <= 4:
        return "{indent}{{\\small {content}}}\\\\[4pt]\n".format(
            indent=indent,
            content=r"\quad ".join(entries),
        )

    # Two balanced lines: ceil(n/2) on first, floor(n/2) on second
    split = math.ceil(n / 2)
    line1 = r"\quad ".join(entries[:split])
    line2 = r"\quad ".join(entries[split:])
    return (
        "{indent}{{\\small {l1}}}\\\\[2pt]\n"
        "{indent}{{\\small {l2}}}\\\\[4pt]\n"
    ).format(indent=indent, l1=line1, l2=line2)
