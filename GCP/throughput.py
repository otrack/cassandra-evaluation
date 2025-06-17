#!/usr/bin/env python3

from os import listdir
from os.path import isfile, join

import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import spline

METRICS_DIR = "logs/3"
PLOT_DIR = "plots"
PROTOCOL = "VCD"

XRANGE = 250
YRANGE = 900

PLOT_SMOOTH = False
SMOOTHNESS = 500

def read(f):
    """
    Read from file and separate entries by ,
    and parts of each entry by -
    """
    r = []

    with open(f, "r") as fd:
        for line in fd:
            parts = line.strip().split("-")
            r.append(parts)

    return r

def ms_to_s(ms):
    """
    Convert a string timestamp in milliseconds to seconds.
    """
    return int(int(ms) / 1000)


def main():
    # assumes there's a single execution (e.g. only for 128 clients)
    
    files = [join(METRICS_DIR, f) for f in listdir(METRICS_DIR)]
    files = [f for f in files if isfile(f) and PROTOCOL in f]

    # save cluster -> (ts -> size)
    data = {}

    for f in files:
        # get cluster name
        parts = f.split("-")
        cluster = parts[2] + "-" + parts[3]

        if "chains" in f:
            # init cluster
            data[cluster] = {}

            # get chains
            for ts, size in read(f):

                # convert them to integers
                ts = int(ts)
                size = int(size)

                # store all sizes from the same timestamp
                if not ts in data[cluster]:
                    data[cluster][ts] = []
                data[cluster][ts].append(size)

        if "events" in f:

            # there's a single events file
            metrics = {}

            # read file
            read_f = read(f)
            _, _, seqs, tss = list(zip(*read_f))

            # convert seqs and tss to int
            seqs = list(map(int, seqs))
            tss = list(map(int, tss))

            # get min seq and min ts (in millisecond)
            min_seq = min(seqs)
            min_ts = min(tss)

            # get metrics
            for what, sender, seq, ts in read_f:
                # compute seq and ts
                seq = int(seq) - min_seq
                ts = int(ts) - min_ts

                # create event name
                name = "(" + sender + "," + str(seq) + ") by " + cluster

                # create event if it doesn't exist
                if not name in metrics:
                    metrics[name] = {}

                # append event
                metrics[name][ts] = what

            # save [(event, start, end)]
            events = []

            for name in metrics:
                [start, end] = sorted(metrics[name])
                assert metrics[name][start] == "recover_start"
                assert metrics[name][end] == "recover_end"

                # add to events
                events.append((start, end, name))

            # plot events
            plot_events(events)

    # aggregate per ranges of timestamps
    aggregate = {}

    for cluster, ts_to_sizes in data.items():
        # init cluster
        aggregate[cluster] = {}

        print("Processing " + cluster + "...")

        # get min and max ts
        min_ts = ms_to_s(min(ts_to_sizes.keys()))
        max_ts = ms_to_s(max(ts_to_sizes.keys()))

        for i in range(min_ts, max_ts + 1):

            # tput = sum values of the map
            # s.t. its key mapped to second is the current second 's'
            tput = sum([sum(sizes) for ts, sizes in ts_to_sizes.items() if ms_to_s(ts) == i])

            # subtract initial ts from i
            ts = i - min_ts

            # store data point
            aggregate[cluster][ts] = tput

    # plot aggregate data
    for cluster, ts_to_size in aggregate.items():
        # plot data
        x = np.array(list(ts_to_size.keys()))
        y = np.array(list(ts_to_size.values()))
        plot_tput(x, y, cluster, cluster + ".png")

        # plot smooth data
        if PLOT_SMOOTH:
            x_smooth = np.linspace(x.min(), x.max(), SMOOTHNESS)
            y_smooth = spline(x, y, x_smooth)
            plot_tput(x_smooth, y_smooth, cluster + " (smooth)", cluster + "_smooth.png")

def plot_events(events):
    # sort events
    events = list(reversed(sorted(events)))


    # [width, height]
    params = plt.rcParams["figure.figsize"]
    plt.rcParams["figure.figsize"] = [12, 40]

    # compute y range
    yrange = range(len(events))
    (starts, ends, names) =  list(zip(*events))

    # compute widths
    widths = [e - s for s, e in zip(starts, ends)]

    # init plot
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    # add plot
    ax.barh(yrange, widths, left=starts)
    ax.set(xlabel="time (ms)", ylabel="dots recovered")
    plt.yticks(yrange, names)
    fig.savefig(join(PLOT_DIR, PROTOCOL + "_events.png"))

    # restore params
    plt.rcParams["figure.figsize"] = params


def plot_tput(x, y, title, output):
    # init plot
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    # configure range on axis
    axis = plt.gca()
    axis.set_xlim([0, XRANGE])
    axis.set_ylim([0, YRANGE])

    # add plot
    ax.plot(x, y)
    ax.set(xlabel="time (s)", ylabel="tput (ops/s)", title=title)
    fig.savefig(join(PLOT_DIR, PROTOCOL + "_" + output))

if __name__ == "__main__":
    main()
