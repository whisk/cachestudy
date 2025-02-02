import simpy
import simpy.events
import simpy.util
import random
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math
import logging

from dataclasses import dataclass, field

df = pd.read_csv('cache_dynamic_expiration.csv', parse_dates=['timestamp'], index_col='timestamp')

# resample data
df_resampled_p99 = df.resample("60s").quantile(0.99)
df_resampled_p95 = df.resample("60s").quantile(0.95)
df_resampled_p50 = df.resample("60s").quantile(0.50)
df_resampled_mean = df.resample("60s").mean()

# create a figure with two subplots
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))

# Plot response time over time on the first subplot
ax1.plot(df_resampled_p99.index, df_resampled_p99['response_time'], linestyle='--', linewidth=1.0, marker='', color='r', label='p99')
ax1.plot(df_resampled_p95.index, df_resampled_p95['response_time'], linestyle='--', linewidth=1.0, marker='', color='y', label='p95')
ax1.plot(df_resampled_p50.index, df_resampled_p50['response_time'], linestyle='-', linewidth=1.0, marker='', color='b', label='p50')
ax1.plot(df_resampled_mean.index, df_resampled_mean['response_time'], linestyle='-', linewidth=1.0, marker='', color='g', label='mean')
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
