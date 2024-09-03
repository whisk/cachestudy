import simpy
import simpy.events
import random
from dataclasses import dataclass

SIMULATION_TIME = 600000
KEY_MAX = 5000
KEY_CACHE_TTL_MEAN = 60000
REQUESTS_MEAN = 1
REQUESTS_DEV  = 0.1

CACHE_CAPACITY = 100
CACHE_RESP_MEAN = 10
CACHE_RESP_DEV = 1
CACHE_TIMEOUT = 100

DATABASE_CAPACITY = 10
DATABASE_RESP_MEAN = 100
DATABASE_RESP_DEV = 10
DATABASE_TIMEOUT = 1000

@dataclass
class Stats:
    requests: int = 0
    ok: int = 0
    fails: int = 0

class KeyValueStorage(simpy.Resource):
    def __init__(self, env: simpy.Environment, capacity: int = 1):
        self._values     = dict()
        self._expires_at = dict()
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


def backend(env: simpy.Environment, cache: KeyValueStorage, database: KeyValueStorage, key, stats: Stats):
    cache_req = cache.request()
    val = yield cache_req | env.timeout(CACHE_TIMEOUT)
    if not cache_req.triggered:
        # cache read timed out, fail
        return None
    # wait for cache response
    yield env.timeout(CACHE_RESP_MEAN)
    val = cache.get(key)
    cache.release(cache_req)
    if val is not None:
        stats.ok += 1
        return val

    # cache is empty, read from database
    database_req = database.request()
    yield database_req | env.timeout(DATABASE_TIMEOUT)
    if not database_req.triggered:
        # database timed out, fail
        stats.fails += 1
        return None
    yield env.timeout(DATABASE_RESP_MEAN)
    val = database.get(key)
    database.release(database_req)
    # save result to cache
    cache_req = cache.request()
    yield cache_req | env.timeout(CACHE_TIMEOUT)
    if not cache_req.triggered:
        # cache write timed out, semi fail
        return val
    cache.set(key, val, KEY_CACHE_TTL_MEAN)
    cache.release(cache_req)
    stats.ok += 1
    return val

def run(env: simpy.Environment, cache: KeyValueStorage, database: KeyValueStorage, stats: Stats) -> simpy.events.ProcessGenerator:
    # prepopulate dabase and cache
    for key in range(KEY_MAX):
        val = "Value {}".format(key)
        database.set(key, val)
        #cache.set(key, val, random.uniform(0, KEY_CACHE_TTL_MEAN))
        cache.set(key, val, KEY_CACHE_TTL_MEAN)
    print("database and cache populated")

    while True:
        yield env.timeout(random.normalvariate(REQUESTS_MEAN, REQUESTS_DEV))
        stats.requests += 1
        key = random.randint(0, KEY_MAX)
        env.process(backend(env, cache, database, key, stats))

env = simpy.Environment()
cache = KeyValueStorage(env, CACHE_CAPACITY)
database = KeyValueStorage(env, DATABASE_CAPACITY)
stats = Stats()
env.process(run(env, cache, database, stats))
env.run(until=SIMULATION_TIME)
print(stats)
