from dataclasses import dataclass, field
import argparse
import logging
import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import random
import simpy
import simpy.events
import simpy.util

SIMULATION_TIME = 60 * 60 * 1000

KEY_CACHE_PREFILL_MAX = 10000
KEY_CACHE_TTL = 20 * 60 * 1000

KEY_GEN_ALPHA = 0.25
KEY_GEN_K = 12.5
KEY_GEN_MOD = 1000003

REQUESTS_PER_U = 0.1
REQUESTS_LAMBDA = 1.0 * REQUESTS_PER_U

LOGNORM_MU = 0.05
LOGNORM_SIGMA = 0.25

CACHE_CAPACITY = 100000
CACHE_RESP_MIN = 0.005 * 1000
CACHE_RESP_MEAN = 0.010 * 1000
CACHE_TIMEOUT = 0.200 * 1000

DATABASE_CAPACITY = 1000
DATABASE_RESP_MIN = 0.25 * 1000
DATABASE_RESP_MEAN = 0.50 * 1000
DATABASE_TIMEOUT = 5 * 1000

@dataclass
class Stats:
    requests: int = 0
    ok: int = 0
    fails: int = 0
    data: list[float] = field(default_factory=list)

    def add_data(self, timestamp, result: int, response_time: float, key: int):
        row = [timestamp, result, response_time, key]
        self.data.append(row)

class Response():
    def __init__(self, val):
        self.is_ok = True
        self.msg = ""
        self.val = val

    @classmethod
    def Success(cls, val):
        err = cls(val)
        return err

    @classmethod
    def Error(cls, msg):
        err = cls(None)
        err.msg = msg
        err.is_ok = False
        return err

class CacheEnvironment(simpy.Environment):
    # process_lambda() works like process() but for code that does not generate events
    def process_lambda(self, f, delay: float = 0):
        def helper():
            yield self.timeout(delay)
            f()

        self.process(helper())

class KeyValueStorage(simpy.Resource):
    def __init__(self, env: simpy.Environment, capacity: int = 1, timeout: int = 1, response_time_min: int = 1, response_time_mean: int = 1000):
        self._values     = dict()
        self._expires_at = dict()
        self._timeout = timeout
        self._response_time_min = response_time_min
        self._response_time_k = response_time_mean - response_time_min
        super().__init__(env, capacity)

    def get(self, key):
        if key not in self._values:
            return None
        if self._expires_at[key] < self._env.now:
            # key expired
            return None
        return self._values[key]

    def set(self, key, value, ttl = None):
        if ttl is None:
            ttl = SIMULATION_TIME + 1
        self._values[key] = value
        self._expires_at[key] = self._env.now + ttl

    def ttl(self, key) -> int:
        if key not in self._expires_at:
            return -2
        if self._expires_at[key] < self._env.now:
            return -1
        return self._expires_at[key] - self._env.now

    def delete(self, key):
        del self._values[key]

    def wipe(self, part: float = 1.0):
        keys = list(self._values.keys())
        for key in keys:
            if random.random() < part:
                cache.delete(key)

    def process_get(self, env, key):
        req = self.request()
        yield req | env.timeout(self._timeout)
        if not req.triggered:
            # resource read timed out, fail
            self.release(req)
            return Response.Error("Timeout")
        # wait for resource to respond
        yield env.timeout(self._response_time_min + random.lognormvariate(LOGNORM_MU, LOGNORM_SIGMA) * self._response_time_k)
        val = self.get(key)
        self.release(req)
        return Response.Success(val)

    def process_set(self, env, key, value, ttl = None):
        req = self.request()
        yield req | env.timeout(self._timeout)
        if not req.triggered:
            # resource write timed out, fail
            self.release(req)
            return Response.Error("Timeout")
        # wait for resource to respond
        yield env.timeout(self._response_time_min + random.lognormvariate(LOGNORM_MU, LOGNORM_SIGMA) * self._response_time_k)
        val = self.set(key, value, ttl)
        self.release(req)
        return Response.Success(val)


def backend(env: simpy.Environment, cache: KeyValueStorage, database: KeyValueStorage, key, stats: Stats, ttl_ext_prob: float = 0.0):
    t0 = env.now
    resp = yield from cache.process_get(env, key)
    if resp.is_ok and resp.val is not None:
        logging.getLogger().debug("Cache hit")
        # data was in the cache
        stats.ok += 1

        # extend ttl
        if random.random() < ttl_ext_prob:
            stats.add_data(t0, 2, env.now - t0, key)
            ttl = cache.ttl(key)
            logging.getLogger().debug("Remaining TTL: %d", ttl)
            if ttl > 0:
                logging.getLogger().debug("Extending TTL")
                database_resp = yield from database.process_get(env, key)
                if database_resp.is_ok:
                    _ = yield from cache.process_set(env, key, database_resp.val, KEY_CACHE_TTL)
            return resp.val
        else:
            stats.add_data(t0, 1, env.now - t0, key)
            return resp.val

    if not resp.is_ok:
        logging.getLogger().debug("Cache fail")
        stats.add_data(t0, 0, env.now - t0, key)
        return resp.val

    logging.getLogger().debug("Cache miss")
    # cache is empty, read from database
    database_resp = yield from database.process_get(env, key)
    if not database_resp.is_ok:
        # database timed out, fail
        stats.fails += 1
        stats.add_data(t0, 0, env.now - t0, key)
        return None

    # save result to cache
    _ = yield from cache.process_set(env, key, database_resp.val, KEY_CACHE_TTL)
    stats.ok += 1
    stats.add_data(t0, 0, env.now - t0, key)
    return database_resp.val

def simulation(env: CacheEnvironment, cache: KeyValueStorage, database: KeyValueStorage, stats: Stats, ttl_ext_prob: float = 0.0):
    # prepopulate dabase and cache
    for key in range(KEY_CACHE_PREFILL_MAX):
        val = "Value {}".format(key)
        database.set(key, val)
        cache.set(key, val, random.uniform(0, KEY_CACHE_TTL))
    logging.getLogger().debug("Database and cache prepopulated")

    while True:
        yield env.timeout(random.expovariate(REQUESTS_LAMBDA))
        stats.requests += 1
        key = int(np.random.pareto(KEY_GEN_ALPHA) * KEY_GEN_K) % KEY_GEN_MOD
        logging.getLogger().debug("Request key %d", key)
        env.process(backend(env, cache, database, key, stats, ttl_ext_prob))

def main():
    parser = argparse.ArgumentParser(description='Cache simulation with dynamic TTL extension')
    parser.add_argument('--journal', type=str, default='journal.csv', help='Simulation journal output filename')
    parser.add_argument('--ttl-ext-prob', type=float, default=0.0, help='TTL extension probability on cache reads')
    parser.add_argument("--loglevel", default=logging.DEBUG, choices=[*logging.getLevelNamesMapping().keys()], help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    env = CacheEnvironment()
    cache = KeyValueStorage(env, CACHE_CAPACITY, CACHE_TIMEOUT, CACHE_RESP_MIN, CACHE_RESP_MEAN)
    database = KeyValueStorage(env, DATABASE_CAPACITY, DATABASE_TIMEOUT, DATABASE_RESP_MIN, DATABASE_RESP_MEAN)
    stats = Stats()

    logging.getLogger().info("Starting simulation")
    env.process(simulation(env, cache, database, stats, args.ttl_ext_prob))
    env.run(until=SIMULATION_TIME)
    logging.getLogger().info("Simulation done")

    df = pd.DataFrame(stats.data, columns=['timestamp', 'result', 'response_time', 'key'])
    df.set_index('timestamp', inplace=True)
    df.index = pd.to_datetime(df.index, unit='ms')
    df.sort_index(inplace=True)

    fp = open(args.journal, 'w')
    fp.write("# Simulation journal\n")
    fp.write("# " + str(args) + "\n")
    df.to_csv(fp)
    logging.getLogger().info("Journal saved")

if __name__ == "__main__":
    main()
