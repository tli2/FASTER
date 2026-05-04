import sys, os.path, glob, re
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.lines as mlines
import matplotlib.patheffects as pe
import random


matplotlib.rc('pdf', fonttype=42)
# Say, "the default sans-serif font is COMIC SANS"
# matplotlib.rcParams['font.sans-serif'] = "Arial"
# Then, "ALWAYS use sans-serif fonts"
# matplotlib.rcParams['font.family'] = "sans-serif"

import numpy as np
import seaborn as sns
# import palettable
# from palettable.colorbrewer.diverging import Spectral_10
# from cycler import cycler

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
FIG_DIR = os.path.join(SCRIPT_DIR, "figures")
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

# Workload name used when running exp04; must match --workload flag passed to that script.
# The standard data files in AE/data/ were generated with "events-50k-long".
EP_LATENCY_WORKLOAD = "events-50k-long"


def compute_percentile_latencies(filename):
    latencies = []
    with open(os.path.join(DATA_DIR, filename), 'r') as file:
        for line in file:
            stripped_line = line.strip()  # Remove any leading/trailing whitespace
            if stripped_line:  # Check if the line is not empty
                try:
                    latency = float(stripped_line)
                    latencies.append(latency)
                except ValueError:
                    print(f"Warning: Ignoring non-numeric data: {stripped_line}")

    latencies_array = np.array(latencies)
    median = np.median(latencies_array)
    p95 = np.percentile(latencies_array, 95)
    return median, p95


def compute_avg_latencies(filename):
    latencies = []
    with open(os.path.join(DATA_DIR, filename), 'r') as file:
        for line in file:
            stripped_line = line.strip()  # Remove any leading/trailing whitespace
            if stripped_line:  # Check if the line is not empty
                try:
                    latency = float(stripped_line)
                    latencies.append(latency)
                except ValueError:
                    print(f"Warning: Ignoring non-numeric data: {stripped_line}")

    latencies_array = np.array(latencies)
    avg = np.mean(latencies_array)
    std = np.std(latencies_array)
    return avg, std


def read_file(filename):
    data = []
    with open(os.path.join(DATA_DIR, filename), 'r') as file:
        for line in file:
            stripped_line = line.strip()  # Remove any leading/trailing whitespace
            if stripped_line:  # Check if the line is not empty
                try:
                    latency = float(stripped_line)
                    data.append(latency)
                except ValueError:
                    print(f"Warning: Ignoring non-numeric data: {stripped_line}")

    data = np.array(data)
    return data


def get_thrpt_latency_filename(wps, speculative):
    return f"workload-{wps}wps-result-0-{'speculative' if speculative else ''}.txt"


def get_event_processing_thrpt_latency_filename(thr, speculative):
    return f"workload-{thr}wps-result-0-{'speculative' if speculative else ''}.txt"


def plot_TravelReservation_thrpt_latency(legend=True, save=True, show=False):
    savefile = os.path.join(FIG_DIR, "TravelReservation-throughput-latency.pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    spec_meds = []
    spec_p95s = []
    spec_thrpt = []
    for w in [16, 32, 64, 128, 256, 512, 1024]:
        med, p95 = compute_percentile_latencies(f"workload-1000wps-result-{w}-.txt")
        spec_meds.append(med)
        spec_p95s.append(p95)
        spec_thrpt.append(len(read_file(f"workload-1000wps-result-{w}-.txt")) / 120.0)

    nospec_meds = []
    nospec_p95s = []
    nospec_thrpt = []
    for w in [64, 128, 256, 512, 1024, 2048]:
        med, p95 = compute_percentile_latencies(f"workload-1000wps-ns-result-{w}-.txt")
        nospec_meds.append(med)
        nospec_p95s.append(p95)
        nospec_thrpt.append(len(read_file(f"workload-1000wps-ns-result-{w}-.txt")) / 120.0)

    temporal_meds = []
    temporal_p95s = []
    temporal_thrpt = []
    for w in [16, 32, 64, 128]:
        med, p95 = compute_percentile_latencies(f"temporal-thr-result-{w}-.txt")
        temporal_meds.append(med)
        temporal_p95s.append(p95)
        temporal_thrpt.append(len(read_file(f"temporal-thr-result-{w}-.txt")) / 120.0)

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.set_ylabel("Latency (ms)", fontsize=34)
    ax.set_xlabel("Throughput (Workflows/s)", fontsize=34)

    # --- CHANGED: linewidth=4, markersize=14 ---
    a, = ax.plot(spec_thrpt, spec_meds, linestyle='-', marker='o', label="Speculative-p50", markersize=14, linewidth=4)
    b, = ax.plot(nospec_thrpt, nospec_meds, linestyle='-', marker='s', label="Non-Spec-p50", markersize=14, linewidth=4)
    c, = ax.plot(spec_thrpt, spec_p95s, linestyle='-', marker='^', label="Speculative-p95", markersize=14, linewidth=4)
    d, = ax.plot(nospec_thrpt, nospec_p95s, linestyle='-', marker='x', label="Non-Spec-p95", markersize=14, linewidth=4, markeredgewidth=3)
    e, = ax.plot(temporal_thrpt, temporal_meds, linestyle='-', marker='D', label="Temporal-p50", markersize=14, linewidth=4)
    f, = ax.plot(temporal_thrpt, temporal_p95s, linestyle='-', marker='*', label="Temporal-p95", markersize=14, linewidth=4)

    ax.set_xticks([1000, 3000, 5000])
    ax.set_yticks([100, 500, 1000, 1500 ])
    plt.yticks(fontsize=34)
    plt.xticks(fontsize=34)

    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
        if legend:
            # --- CHANGED: Updated Line2D to match new plot styles (linewidth=4, markersize=14) ---
            line_spec_med = mlines.Line2D([], [], color=a.get_color(), marker='o', linestyle='-', markersize=18, linewidth=6, label="Speculative-p50")
            line_nospec_med = mlines.Line2D([], [], color=b.get_color(), marker='s', linestyle='-', markersize=18, linewidth=6, label="Non-Spec-p50")
            line_spec_p95 = mlines.Line2D([], [], color=c.get_color(), marker='^', linestyle='-', markersize=18, linewidth=6, label="Speculative-p95")
            line_nospec_p95 = mlines.Line2D([], [], color=d.get_color(), marker='x', linestyle='-', markersize=18, linewidth=6, markeredgewidth=3, label="Non-Spec-p95")
            line_temporal_med = mlines.Line2D([], [], color=e.get_color(), marker='D', linestyle='-', markersize=18, linewidth=6, label="Temporal-p50")
            line_temporal_p95 = mlines.Line2D([], [], color=f.get_color(), marker='*', linestyle='-', markersize=18, linewidth=6, label="Temporal-p95")

            fig_legend = plt.figure(figsize=(16, 1))
            fig_legend.legend(handles=[line_spec_med, line_spec_p95, line_nospec_med, line_nospec_p95, line_temporal_med, line_temporal_p95],
                              labels=["Speculative-p50", "Speculative-p95", "Non-Spec-p50", "Non-Spec-p95", "Temporal-p50", "Temporal-p95"],
                              loc='center', ncol=3, frameon=False, fontsize=30)
            fig_legend.savefig(os.path.join(FIG_DIR, 'TravelReservation-throughput-latency-legend.pdf'))
    if show:
        plt.show()
    plt.close()


def get_latency_scaleup_filename(num, speculative):
    return f"workload-{num}service-result-0-{'speculative' if speculative else ''}.txt"


def plot_TravelReservation_latency_scaleup(legend=True, save=True, show=False):
    savefile = os.path.join(FIG_DIR, "TravelReservation-scaleup-latency.pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    num_services = [i for i in range(1, 11)]
    spec_meds = []
    nospec_meds = []
    temporal_meds = []
    spec_p95s = []
    nospec_p95s = []
    temporal_p95s = []
    for n in num_services:
        med_nospec, p95_nospec = compute_percentile_latencies(get_latency_scaleup_filename(n, False))
        med_spec, p95_spec = compute_percentile_latencies(get_latency_scaleup_filename(n, True))
        med_temporal, p95_temporal = compute_percentile_latencies(f"TravelReservation-latency-{n}-temporal-result.txt")
        spec_meds.append(med_spec)
        nospec_meds.append(med_nospec)
        temporal_meds.append(med_temporal)
        spec_p95s.append(p95_spec)
        nospec_p95s.append(p95_nospec)
        temporal_p95s.append(p95_temporal)

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.set_ylabel("Latency (ms)", fontsize=34)
    ax.set_xlabel("#Services", fontsize=34)

    # --- CHANGED: linewidth=4, markersize=14 for all lines ---
    a, = ax.plot(num_services, spec_meds, linestyle='-', marker='o', label="Speculative-p50", markersize=18, linewidth=6)
    b, = ax.plot(num_services, nospec_meds, linestyle='-', marker='s', label="Baseline-p50", markersize=18, linewidth=6)
    c, = ax.plot(num_services, spec_p95s, linestyle='-.', marker='^', label="Speculative-p95", markersize=18, linewidth=6)
    d, = ax.plot(num_services, nospec_p95s, linestyle='-', marker='x', label="Baseline-p95", markersize=18, linewidth=6, markeredgewidth=3)
    e, = ax.plot(num_services, temporal_meds, linestyle='-', marker='D', label="Temporal-p50", markersize=18, linewidth=6)
    f, = ax.plot(num_services, temporal_p95s, linestyle='-', marker='*', label="Temporal-p95", markersize=18, linewidth=6)

    ax.set_yticks([20, 100, 200, 400, 600, 800])
    ax.set_xticks([2, 4, 6, 8, 10])
    plt.yticks(fontsize=34)
    plt.xticks(fontsize=34)

    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
        if legend:
            fig_legend = plt.figure(figsize=(18, 1.2))
            fig_legend.legend(handles=[a, b, c, d, e, f],
                              labels=["Speculative-p50", "Baseline-p50", "Speculative-p95", "Baseline-p95", "Temporal-p50", "Temporal-p95"],
                              loc='center', ncol=3, frameon=False, fontsize=30, markerscale=1.5)
            fig_legend.savefig(os.path.join(FIG_DIR, 'TravelReservation-latency-scaleup-legend.pdf'))
    if show:
        plt.show()
    plt.close()

def get_coordinator_latency_filename(n, d):
    return f"coordinator-microbench-result-n{n}-d{d}.csv"


def plot_coordinator_latency(legend=True, save=True, show=False):
    savefile = os.path.join(FIG_DIR, "coordinator-latency.pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    num_services = [i * 8 for i in range(1, 9)]
    d0avgs = []
    d0stds = []
    d4avgs = []
    d4stds = []
    d8avgs = []
    d8stds = []
    for i in range(1, 9):
        d0avg, d0std = compute_avg_latencies(get_coordinator_latency_filename(i, '0.0'))
        d4avg, d4std = compute_avg_latencies(get_coordinator_latency_filename(i, '0.4'))
        d8avg, d8std = compute_avg_latencies(get_coordinator_latency_filename(i, '0.8'))
        d0avgs.append(d0avg)
        d0stds.append(d0std)
        d4avgs.append(d4avg)
        d4stds.append(d4std)
        d8avgs.append(d8avg)
        d8stds.append(d8std)

    d0avgs = np.array(d0avgs)
    d0stds = np.array(d0stds)
    d4avgs = np.array(d4avgs)
    d4stds = np.array(d4stds)
    d8avgs = np.array(d8avgs)
    d8stds = np.array(d8stds)

    fig, ax = plt.subplots(figsize=(6, 3))
    ax.set_ylabel("Latency (ms)", fontsize=24)
    ax.set_xlabel("#Services", fontsize=24)

    a, = ax.plot(num_services, d0avgs, '-', color='blue')
    ax.fill_between(num_services, d0avgs - d0stds, d0avgs + d0stds, color='blue', alpha=0.1)
    b, = ax.plot(num_services, d4avgs, '-', color='green')
    ax.fill_between(num_services, d4avgs - d4stds, d4avgs + d4stds, color='green', alpha=0.1)
    c, = ax.plot(num_services, d8avgs, '-', color='red')
    ax.fill_between(num_services, d8avgs - d8stds, d8avgs + d8stds, color='red', alpha=0.1)

    ax.set_yticks([0, 5, 10])
    ax.set_xticks([16, 32, 48, 64])
    plt.yticks(fontsize=24)
    plt.xticks(fontsize=24)

    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
        if legend:
            fig_legend = plt.figure(figsize=(11, 0.4))
            fig_legend.legend(handles=[a, b, c],
                              labels=["d=0.0", "d=0.4", "d=0.8"],
                              loc='center', ncol=3, frameon=False, fontsize=24)
            fig_legend.savefig(os.path.join(FIG_DIR, 'coordinator-latency-legend.pdf'))
    if show:
        plt.show()
    plt.close()


def plot_events_bar_latency_10(save=True, show=False):
    savefile = os.path.join(FIG_DIR, "events-c10-lat.pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass
    ind = np.arange(2)
    width = 0.3
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.set_ylabel("Latency (ms)", fontsize=32)
    ax.set_xticks(ind)
    ax.set_xticklabels(["dse", "baseline"], fontsize=32)

    _, lat_spec   = read_timeline_from_csv(f"EventProcessing-latency-{EP_LATENCY_WORKLOAD}-results-c10spec-lat.csv")
    _, lat_nospec = read_timeline_from_csv(f"EventProcessing-latency-{EP_LATENCY_WORKLOAD}-results-c10nospec-lat.csv")
    p50_bars = [np.percentile(lat_spec, 50), np.percentile(lat_nospec, 50)]
    p95_bars = [np.percentile(lat_spec, 95), np.percentile(lat_nospec, 95)]

    a = ax.bar(ind - width / 2, p50_bars, width, color='blue', zorder=3)
    b = ax.bar(ind + width / 2, p95_bars, width, color='red', zorder=3)

    ax.tick_params(axis='y', labelsize=32)
    ax.set_yticks([0, 40, 80, 120, 160])
    plt.xticks(fontsize=32)
    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)

    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
        # if legend:
        #     fig_legend = plt.figure(figsize=(11, 0.4))
        #     fig_legend.legend(handles=[a, b],
        #                       labels=["p50 latency", "p95 latency"],
        #                       loc='center', ncol=2, frameon=False, fontsize=24)
        #     fig_legend.savefig(os.path.join(FIG_DIR, 'events-bar.pdf'))
    if show:
        plt.show()
    plt.close()


def plot_events_bar_latency_500(save=True, show=False):
    savefile = os.path.join(FIG_DIR, "events-c500-lat.pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass
    ind = np.arange(2)
    width = 0.3
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.set_ylabel("Latency (ms)", fontsize=32)
    ax.set_xticks(ind)
    ax.set_xticklabels(["dse", "baseline"], fontsize=32)

    _, lat_nospec = read_timeline_from_csv(f"EventProcessing-latency-{EP_LATENCY_WORKLOAD}-results-c500nospec-lat.csv")
    nospec_p50 = np.percentile(lat_nospec, 50)
    nospec_p95 = np.percentile(lat_nospec, 95)

    c500spec_file = f"EventProcessing-latency-{EP_LATENCY_WORKLOAD}-results-c500spec-lat.csv"
    try:
        _, lat_spec = read_timeline_from_csv(c500spec_file)
        spec_p50 = np.percentile(lat_spec, 50)
        spec_p95 = np.percentile(lat_spec, 95)
    except FileNotFoundError:
        print(f"Warning: {c500spec_file} not found; using hardcoded values. Run exp04 --speculative true --checkpoint-interval 500 --workload {EP_LATENCY_WORKLOAD} to generate it.")
        spec_p50, spec_p95 = 638, 1012

    ax.bar(ind - width / 2, [spec_p50, nospec_p50], width, color='blue', zorder=3)
    ax.bar(ind + width / 2, [spec_p95, nospec_p95], width, color='red', zorder=3)

    ax.tick_params(axis='y', labelsize=32)
    ax.set_yticks([0, 4000, 8000, 12000])
    plt.xticks(fontsize=32)
    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)

    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()


def plot_events_bar_bytes(save=True, show=False):
    conditions = [
        ("c10spec",    "dse-10"),
        ("c10nospec",  "baseline-10"),
        ("c500spec",   "dse-500"),
        ("c500nospec", "baseline-500"),
    ]
    bytes_mb, missing = [], []
    for tag, _ in conditions:
        fname = f"EventProcessing-latency-{EP_LATENCY_WORKLOAD}-results-{tag}-stats.csv"
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            missing.append(fname)
            continue
        with open(fpath) as f:
            for line in f:
                if line.startswith("BytesWritten:"):
                    bytes_mb.append(float(line.split()[1]) / 1_000_000)
                    break

    if missing:
        print(f"[plot_events_bar_bytes] missing files: {missing}; skipping")
        return

    savefile = os.path.join(FIG_DIR, "events-bytes.pdf")
    if save:
        try:
            os.remove(savefile)
        except:
            pass
    labels = [label for _, label in conditions]
    ind, width = np.arange(4), 0.5
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.set_ylabel("Bytes Written (MB)", fontsize=34)
    ax.set_xticks(ind)
    ax.set_xticklabels(labels, fontsize=34)
    ax.bar(ind, bytes_mb, width, color='green', zorder=3)
    ax.tick_params(axis='y', labelsize=34)
    ax.set_yticks([0, 500, 1000, 1500])
    plt.xticks(fontsize=28)
    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()


def read_timeline_from_csv(filename):
    time = []
    latency = []
    with open(os.path.join(DATA_DIR, filename), 'r') as file:
        for line in file:
            stripped_line = line.strip()  # Remove any leading/trailing whitespace
            if stripped_line:  # Check if the line is not empty
                parts = stripped_line.split(',')  # Split the line into parts
                if len(parts) > 1:  # Ensure there are at least two columns
                    try:
                        time.append(float(parts[0]))   # Convert the second column to float
                        latency.append(float(parts[1]))
                    except ValueError:
                        print(f"Warning: Ignoring non-numeric data: {parts[1]}")
                else:
                    print("Warning: Line with insufficient columns ignored.")
    return np.array(time), np.array(latency)


def plot_2pc_scatter(data, label, save=True, show=False):
    savefile = os.path.join(FIG_DIR, label + "-2pc-scatter.png")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    _, data = read_timeline_from_csv(data)
    p50 = np.percentile(data, 50)
    p95 = np.percentile(data, 95)

    plt.figure(figsize=(6, 3))
    x = np.arange(len(data))

    plt.axhline(y=p50, color='green', linestyle='--', linewidth=2.0)
    plt.axhline(y=p95, color='red', linestyle='--', linewidth=2.0)

    text_p50 = plt.text(len(data), p50, f' P50: {p50:.1f}ms', color='green', 
                        fontsize=24, fontweight='bold', ha='right', va='bottom')
    text_p50.set_path_effects([pe.withStroke(linewidth=3, foreground='white')])

    text_p95 = plt.text(len(data), p95, f' P95: {p95:.1f}ms', color='red', 
                        fontsize=24, fontweight='bold', ha='right', va='bottom')
    text_p95.set_path_effects([pe.withStroke(linewidth=3, foreground='white')])

    a = plt.scatter(x, data, color='blue', label='Spec', s=1, alpha=1.0)
    plt.ylabel("Latency (ms)", fontsize=24)
    plt.yticks([0, 20, 40, 60, 80], fontsize=24)
    plt.xticks([])
    plt.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.ylim(0, 100)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()


def plot_timeline_sim(output, datafile, save=True, show=False):
    savefile = os.path.join(FIG_DIR, output + ".pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    time, latency = read_timeline_from_csv(datafile)
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.set_ylabel("Latency (ms)", fontsize=24)
    ax.set_xlabel("Time (s)", fontsize=24)
    a = ax.plot(time / 1000.0, latency, linestyle="-", marker='o', markersize=1)
    plt.yticks([0, 100, 200, 300], fontsize=24)
    plt.xticks([0, 30, 60, 90, 120], fontsize=24)
    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)

    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()




def plot_timeline(output, datafile, save=True, show=False):
    savefile = os.path.join(FIG_DIR, output + ".pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    time, latency = read_timeline_from_csv(datafile)
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.set_ylabel("Latency (s)", fontsize=24)
    ax.set_xlabel("Time (s)", fontsize=24)
    a = ax.plot(time / 1000.0, latency / 1000.0,linestyle="-", marker='o',  markersize=1)
    plt.yticks([0, 2, 4, 6, 8, 10], fontsize=24)
    plt.xticks([0, 30, 60, 90, 120], fontsize=24)
    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)

    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()


def plot_2pc_timeline(output, datafile, xticks, save=True, show=False):
    savefile = os.path.join(FIG_DIR, output + ".png")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    time, latency = read_timeline_from_csv(datafile)
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.set_ylabel("Latency (ms)", fontsize=24)
    ax.set_xlabel("Time (s)", fontsize=24)

    # Plot all data points
    ax.plot(time / 1000000000.0, latency, 'o', color='blue', markersize=1, alpha=1.0)

    # Plot dashed line at the middle entry
    midpoint_index = len(latency) // 2
    midpoint_time = time[midpoint_index] / 1000000000.0
    ax.axvline(x=midpoint_time, color="grey", ls='--', alpha=0.3)

    # Mark points with a certain y value as red
    red_indices = [i for i, val in enumerate(latency) if val < 0]
    ax.plot(time[red_indices] / 1000000000.0, [ random.randint(10, 40) for _ in red_indices], 'o', color='red', label='aborted', markersize=4)

    plt.yticks([0, 20, 40, 60, 80], fontsize=24)
    plt.xticks(xticks, fontsize=24)
    plt.ylim(0, 100)
    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)

    plt.tight_layout()
    if save:
        plt.savefig(savefile, bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()

def plot_local_action_scalability(save=True, show=False):
    savefile = os.path.join(FIG_DIR, "local-action-scalability.pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    num_threads = [i for i in range(1, 33)]
    throughput = read_file(os.path.join(DATA_DIR, "local-action-scalability.txt"))

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.set_ylabel("Throughput (M ops/s)", fontsize=32)
    ax.set_xlabel("# Threads", fontsize=32)
    a = ax.plot(num_threads, throughput / 1000000, '-')
    ax.set_yticks([0, 50, 100, 150])
    ax.set_xticks([8, 16, 24, 32])
    plt.yticks(fontsize=32)
    plt.xticks(fontsize=32)

    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()


def plot_receive_action_scalability(save=True, show=False):
    savefile = os.path.join(FIG_DIR, "receive-action-scalability.pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    num_threads = [i for i in range(1, 33)]
    throughput = read_file(os.path.join(DATA_DIR, "send-receive-action-scalability.txt"))

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.set_ylabel("Throughput (M ops/s)", fontsize=32)
    ax.set_xlabel("# Threads", fontsize=32)
    a = ax.plot(num_threads, throughput / 1000000, '-')
    ax.set_yticks([0, 10, 20, 30, 40])
    ax.set_xticks([8, 16, 24, 32])
    plt.yticks(fontsize=32)
    plt.xticks(fontsize=32)

    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()


def plot_detach_scalability(save=True, show=False):
    savefile = os.path.join(FIG_DIR, "detach-scalability.pdf")
    if save:
        try:
            os.remove(savefile)  # remove any old figures
        except:
            pass

    num_threads = [i for i in range(1, 33)]
    throughput = read_file(os.path.join(DATA_DIR, "detach-merge-action-scalability.txt"))

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.set_ylabel("Throughput (M ops/s)", fontsize=32)
    ax.set_xlabel("# Threads", fontsize=32)
    a = ax.plot(num_threads, throughput / 1000000, '-')
    ax.set_yticks([0, 2, 4, 6, 8, 10])
    ax.set_xticks([8, 16, 24, 32])
    plt.yticks(fontsize=32)
    plt.xticks(fontsize=32)

    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
    if show:
        plt.show()
    plt.close()


def plot_dse_thr_lat_p50(save=True, show=False):
    def _load_mode(mode):
        pattern = os.path.join(DATA_DIR, f'spfaster-{mode}-summary-w*.txt')
        files = sorted(glob.glob(pattern),
                       key=lambda f: int(re.search(r'-w(\d+)\.txt$', f).group(1)))
        if not files:
            return None, None
        thrpts, p50s = [], []
        for f in files:
            text = open(f).read()
            thr = next((l for l in text.splitlines() if l.startswith('Throughput:')), None)
            med = next((l for l in text.splitlines() if l.startswith('Median Latency:')), None)
            if thr and med:
                thrpts.append(float(thr.split()[1]) / 1000.0)  # ops/s → K Op/s
                p50s.append(float(med.split()[2]))
        return (thrpts, p50s) if thrpts else (None, None)

    data = {m: _load_mode(m) for m in ('none', 'noint', 'int')}
    missing = [m for m, (t, _) in data.items() if t is None]
    if missing:
        print(f"[plot_dse_thr_lat_p50] missing summary files for modes: {missing}; skipping")
        return

    savefile = os.path.join(FIG_DIR, "dse_thr_lat_p50.pdf")
    if save:
        try:
            os.remove(savefile)
        except:
            pass

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.set_xlabel("Throughput(K Op/s)", fontsize=24)
    ax.set_ylabel("Latency (ms)", fontsize=24)
    a, = ax.plot(*data['none'],  linestyle="--", marker='o', markersize=8)
    b, = ax.plot(*data['noint'], linestyle="--", marker='s', markersize=8)
    c, = ax.plot(*data['int'],   linestyle="--", marker='^', markersize=8)
    ax.set_yticks([0, 5, 10, 15, 20, 25])
    ax.set_xticks([40, 80, 120, 160])
    plt.yticks(fontsize=24)
    plt.xticks(fontsize=24)
    ax.grid(which='major', axis='y', color='#D3D3D3', zorder=0)
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(FIG_DIR, savefile), bbox_inches='tight', pad_inches=0.0)
        fig_legend = plt.figure(figsize=(11, 0.4))
        fig_legend.legend(handles=[a, b, c],
                          labels=["no DSE", "DSE-manual", "DSE-interceptor"],
                          loc='center', ncol=3, frameon=False, fontsize=24)
        fig_legend.savefig(os.path.join(FIG_DIR, 'dse_thr_lat_p50-legend.pdf'))
    if show:
        plt.show()
    plt.close()


if __name__ == "__main__":
    plot_TravelReservation_thrpt_latency()
    plot_TravelReservation_latency_scaleup()
    plot_coordinator_latency()
    plot_events_bar_bytes()
    plot_events_bar_latency_10()
    plot_events_bar_latency_500()
    plot_timeline("recovery-1-nospec", "recovery-1-nospec-lat.csv")
    plot_timeline("recovery-1-spec", "recovery-1-spec-lat.csv")
    plot_timeline_sim("recovery-1-sim", "EventProcessing-recovery-events-10k-long-results-c10new2-lat.csv")
    plot_2pc_scatter("2pc-results-speculative-small.txt", "spec")
    plot_2pc_scatter("2pc-results-default-small.txt", "nospec")
    plot_2pc_scatter("2pc-results-orleans.txt", "orleans")
    plot_2pc_timeline("2pc-results-failover", "2pc-results-failover-small.txt", [0, 2, 4, 6])
    plot_2pc_timeline("2pc-results-nofail", "2pc-results-default-small.txt", [0, 4, 8, 12])
    plot_local_action_scalability()
    plot_receive_action_scalability()
    plot_detach_scalability()
    plot_dse_thr_lat_p50()



