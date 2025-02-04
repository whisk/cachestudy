import pandas as pd
import matplotlib.pyplot as plt
import argparse

from dataclasses import dataclass, field

parser = argparse.ArgumentParser(description='Cache simulation with dynamic expiration')
parser.add_argument('--input', type=str, default='output.csv', help='Output filename for the CSV file')
args = parser.parse_args()

df = pd.read_csv(args.input, parse_dates=['timestamp'], index_col='timestamp')
df_popular = df[df['key'] < 1000]

# resample data
df_resampled_p99 = df_popular.resample("60s").quantile(0.99)
df_resampled_mean = df_popular.resample("60s").mean()

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
requests_rate = df['response_time'].resample("1s").count()
ax2.plot(requests_rate.index, requests_rate.values, linestyle='-', linewidth=1.0, marker='', color='b')
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
