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

SIMULATION_TIME = 3600000

KEY_CACHE_PREFILL_MAX = 100000
KEY_CACHE_TTL = 360000

KEY_GEN_A = 0.5
KEY_GEN_K = 5.0
KEY_GEN_MOD = 6969691

REQUESTS_PER_U = 0.1
REQUESTS_LAMBDA = 1.0 * REQUESTS_PER_U

CACHE_CAPACITY = 10000
CACHE_RESP_MEAN = 10
CACHE_RESP_DEV = 1
CACHE_TIMEOUT = 100

DATABASE_CAPACITY = 100
DATABASE_RESP_MEAN = 250
DATABASE_RESP_DEV = 25
DATABASE_TIMEOUT = 2000

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
    def __init__(self, env: simpy.Environment, capacity: int = 1, timeout: int = 1, response_time: int = 1):
        self._values     = dict()
        self._expires_at = dict()
        self._timeout = timeout
        self._response_time = response_time
        self._response_time_sigma = response_time / 10
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
        yield env.timeout(random.normalvariate(self._response_time, self._response_time_sigma))
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
        yield env.timeout(random.normalvariate(self._response_time, self._response_time_sigma))
        val = self.set(key, value, ttl)
        self.release(req)
        return Response.Success(val)


def backend(env: simpy.Environment, cache: KeyValueStorage, database: KeyValueStorage, key, stats: Stats):
    t0 = env.now
    resp = yield from cache.process_get(env, key)
    if resp.is_ok and resp.val is not None:
        logging.getLogger().debug("Cache hit")
        # data was in the cache
        stats.ok += 1
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
    stats.add_data(t0, 1, env.now - t0, key)
    return database_resp.val

def simulation(env: CacheEnvironment, cache: KeyValueStorage, database: KeyValueStorage, stats: Stats):
    # prepopulate dabase and cache
    for key in range(KEY_CACHE_PREFILL_MAX):
        val = "Value {}".format(key)
        database.set(key, val)
        cache.set(key, val, random.uniform(0, KEY_CACHE_TTL))
    logging.getLogger().debug("Database and cache prepopulated")

    while True:
        yield env.timeout(random.expovariate(REQUESTS_LAMBDA))
        stats.requests += 1
        key = int(np.random.pareto(KEY_GEN_A) * KEY_GEN_K) % KEY_GEN_MOD
        logging.getLogger().debug("Request key %d", key)
        env.process(backend(env, cache, database, key, stats))


logging.basicConfig(level=logging.DEBUG)
env = CacheEnvironment()
cache = KeyValueStorage(env, CACHE_CAPACITY, CACHE_TIMEOUT, CACHE_RESP_MEAN)
database = KeyValueStorage(env, DATABASE_CAPACITY, DATABASE_TIMEOUT, DATABASE_RESP_MEAN)
stats = Stats()

logging.getLogger().info("Starting simulation")
env.process(simulation(env, cache, database, stats))
env.run(until=SIMULATION_TIME)
logging.getLogger().info("Simulation done")

df = pd.DataFrame(stats.data, columns=['timestamp', 'result', 'response_time', 'key'])
df.set_index('timestamp', inplace=True)
df.index = pd.to_datetime(df.index, unit='ms')
df.sort_index(inplace=True)

df.to_csv('cache_dynamic_expiration.csv')
logging.getLogger().info("Data saved")
