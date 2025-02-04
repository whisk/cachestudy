from dataclasses import dataclass, field
import argparse
import logging
import matplotlib.pyplot as plt
import pandas as pd

# FIXME: should output list of top keys, not a single key
def get_border_key(df: pd.DataFrame, top_keys_share: float):
    sum = 0
    border_key = -1
    for key, val in df["key"].value_counts().items():
        if sum + val > len(df) * top_keys_share:
            break
        sum += val
        border_key = key
    return border_key


def main():
    parser = argparse.ArgumentParser(description='Draw charts of cache simulation')
    parser.add_argument('--journal', type=str, default='journal.csv', help='Simulation journal filename')
    parser.add_argument('--top-keys-share', type=float, default=0.8, help='Top keys share to analyze')
    parser.add_argument('--top-keys-threshold', type=float, default=0, help='')
    parser.add_argument('--loglevel', default=logging.INFO, choices=list(logging.getLevelNamesMapping().keys()), help='Logging level')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    logging.getLogger().info("Reading journal file %s...", args.journal)
    df = pd.read_csv(args.journal, parse_dates=['timestamp'], index_col='timestamp')
    logging.getLogger().info("Read %d records", len(df))

    if args.top_keys_threshold > 0:
        counts = df["key"].value_counts()
        border_key = counts[counts > args.top_keys_threshold].index[-1]
    else:
        border_key = get_border_key(df, args.top_keys_share)

    logging.getLogger().info("Border key is %d", border_key)
    df_topkeys = df[df["key"] <= border_key]
    logging.getLogger().info("Requests for popular keys count %d (%0.4f)", len(df_topkeys), len(df_topkeys) / len(df))

    # resample data
    df_resampled_p99 = df_topkeys.resample("60s").quantile(0.99)
    df_resampled_mean = df_topkeys.resample("60s").mean()

    # create a figure with two subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))

    # Plot response time over time on the first subplot
    ax1.plot(df_resampled_p99.index, df_resampled_p99['response_time'], linestyle='-', marker='', color='r', label='p99')
    ax1.plot(df_resampled_mean.index, df_resampled_mean['response_time'], linestyle='-', marker='', color='g', label='mean')
    ax1.legend()
    ax1.set_xlabel('Simulation time')
    ax1.set_ylabel('Response time (ms)')
    ax1.set_ylim(0)
    ax1.grid(True)

    # plot number of requests over time on the second subplot
    rps = df['response_time'].resample("1s").count().resample("5s").mean()
    rps_topkeys = df_topkeys['response_time'].resample("1s").count().resample("5s").mean()

    ax2.plot(rps.index, rps.values, linestyle='-', linewidth=1.0, marker='', color='b', label='All keys')
    ax2.plot(rps_topkeys.index, rps_topkeys.values, linestyle='-', linewidth=1.0, marker='', color='r', label='Top keys')
    ax2.legend()
    ax2.set_xlabel('Simulation time')
    ax2.set_ylabel('Requests per second')
    ax2.set_ylim(0)
    ax2.grid(True)

    # plot key popularity histogram on the second subplot
    key_popularity = df['key'].value_counts().nlargest(100)
    ax3.bar(key_popularity.index, key_popularity.values, color='b')
    ax3.set_xlabel('Key')
    ax3.set_ylabel('Popularity')
    ax3.grid(True)

    # adjust layout and show the plot
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
