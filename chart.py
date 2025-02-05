from dataclasses import dataclass, field
import argparse
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# FIXME: should output list of top keys, not a single key
def get_border_key(df: pd.DataFrame, top_keys_share: float):
    sum = 0
    border_key = None
    for key, val in df["key"].value_counts().items():
        if sum + val > len(df) * top_keys_share:
            break
        sum += val
        border_key = key
    return border_key


def main():
    parser = argparse.ArgumentParser(description="Draw charts of cache simulation")
    parser.add_argument("--journal", type=str, default="journal.csv", help="Simulation journal filename")
    parser.add_argument("--top-keys-share", type=float, default=0.8, help="Top keys share to analyze")
    parser.add_argument("--top-keys-threshold", type=float, default=0, help="")
    parser.add_argument("--quantile", type=float, default=0.95, help="")
    parser.add_argument("--loglevel", default=logging.INFO, choices=list(logging.getLevelNamesMapping().keys()), help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    logging.getLogger().info("Reading journal file %s...", args.journal)
    df = pd.read_csv(args.journal, parse_dates=["timestamp"], index_col="timestamp", comment="#")
    logging.getLogger().info("Read %d records", len(df))

    # create a figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 8))

    if args.top_keys_threshold > 0:
        counts = df["key"].value_counts()
        border_key = counts[counts > args.top_keys_threshold].index[-1]
    elif args.top_keys_share > 0:
        border_key = get_border_key(df, args.top_keys_share)
    else:
        border_key = df["key"].max()

    logging.getLogger().info("Border key is %d", border_key)
    df_topkeys = df[df["key"] <= border_key].copy()
    logging.getLogger().info("Requests for popular keys count %d (%0.4f)", len(df_topkeys), len(df_topkeys) / len(df))

    # plot response time over time
    df_topkeys.drop(columns="result", inplace=True)
    df_resampled_p = df_topkeys.resample("60s").quantile(args.quantile)
    df_resampled_mean = df_topkeys.resample("60s").mean()
    ax1.plot(df_resampled_p.index, df_resampled_p["response_time"], linestyle="-", marker="", color="r", label="p{:0.2f}".format(args.quantile * 100.0))
    ax1.plot(df_resampled_mean.index, df_resampled_mean["response_time"], linestyle="-", marker="", color="g", label="mean")
    ax1.legend()
    ax1.set_xlabel("Simulation time")
    ax1.set_ylabel("Response time (ms)")
    ax1.set_ylim(0)
    ax1.grid(True)

    # plot number of requests over time
    rps = df["response_time"].resample("1s").count().resample("5s").mean()
    rps_topkeys = df_topkeys["response_time"].resample("1s").count().resample("5s").mean()

    ax2.plot(rps.index, rps.values, linestyle="-", linewidth=1.0, marker="", color="b", label="All keys")
    ax2.plot(rps_topkeys.index, rps_topkeys.values, linestyle="-", linewidth=1.0, marker="", color="r", label="Top keys")
    ax2.legend()
    ax2.set_xlabel("Simulation time")
    ax2.set_ylabel("Requests per second")
    ax2.set_ylim(0)
    ax2.grid(True)

    # plot key popularity histogram
    key_popularity = df["key"].value_counts().nlargest(200)
    ax3.bar(key_popularity.index, key_popularity.values, width=1.0, color="b")
    ax3.set_xlabel("Key")
    ax3.set_ylabel("Requests count")
    ax3.grid(True)

    # plot cache hit ration
    df_hit_ratio = df.copy()
    df_hit_ratio["hit"] = df["result"].transform(lambda x: 1 if x.find("cache_hit") != -1 else 0)
    df_hit_ratio = df_hit_ratio.groupby("key")["hit"].mean()[0:border_key]
    df_miss_ratio = 1.0 - df_hit_ratio
    ax4.bar(df_hit_ratio.index, df_hit_ratio.values, width=1.0, color="g")
    ax4.bar(df_miss_ratio.index, df_miss_ratio.values, bottom=df_hit_ratio.values, width=1.0, color="r")
    ax4.set_xlabel("Key")
    ax4.set_ylabel("Cache hit ratio")
    ax4.grid(True)

    # adjust layout and show the plot
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
