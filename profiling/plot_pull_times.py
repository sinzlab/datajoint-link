import os
import pstats
import sys

import matplotlib.pyplot as plt

PRIMARY_KEY_COUNT = int(sys.argv[2])
PULL_COUNT = int(sys.argv[3])

path = os.path.join("profiling", "stats", sys.argv[1])

entry_counts, times = [], []
for filename in os.listdir(path):
    if not filename.endswith("pstats"):
        continue
    entry_count, primary_key_count, pull_count = (int(c) for c in os.path.splitext(filename)[0].split("_")[1:])
    if primary_key_count != PRIMARY_KEY_COUNT or pull_count != PULL_COUNT:
        continue
    entry_counts.append(entry_count)
    profile = pstats.Stats(os.path.join(path, filename))
    times.append(profile.total_tt)

entry_counts, times = zip(*sorted(zip(entry_counts, times)))

plt.plot(entry_counts, times, "o-")
plt.xscale("log")
plt.yscale("log")
plt.xlabel("# entries in source table")
plt.ylabel("pull time [s]")
plt.title(f"Pull Times (primary key count: {PRIMARY_KEY_COUNT}, pull count: {PULL_COUNT})")
plt.tight_layout()
plt.savefig(os.path.join("profiling", "figs", f"pull_times_{sys.argv[1]}_{PRIMARY_KEY_COUNT}_{PULL_COUNT}.png"))
