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


class KeyValueStorage(simpy.Resource):
    def __init__(self, env: simpy.Environment, capacity: int = 1, timeout: int = 1, response_time: int = 1):
        self._values     = dict()
        self._expires_at = dict()
        self._timeout = timeout
        self._response_time = response_time
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

    def process_get(self, env, key):
        req = self.request()
        yield req | env.timeout(self._timeout)
        if not req.triggered:
            # resource read timed out, fail
            return Response.Error("Timeout")
        # wait for resource to respond
        yield env.timeout(self._response_time)
        val = self.get(key)
        self.release(req)
        return Response.Success(val)

    def process_set(self, env, key, value, ttl = None):
        req = self.request()
        yield req | env.timeout(self._timeout)
        if not req.triggered:
            # resource write timed out, fail
            return Response.Error("Timeout")
        # wait for resource to respond
        yield env.timeout(self._response_time)
        val = self.set(key, value, ttl)
        self.release(req)
        return Response.Success(val)


def backend(env: simpy.Environment, cache: KeyValueStorage, database: KeyValueStorage, key, stats: Stats):
    resp = yield from cache.process_get(env, key)
    if resp.is_ok and resp.val is not None:
        # data was in the cache
        stats.ok += 1
        return resp.val

    # cache is empty, read from database
    database_resp = yield from database.process_get(env, key)
    if not database_resp.is_ok:
        # database timed out, fail
        stats.fails += 1
        return None

    # save result to cache
    _ = yield from cache.process_set(env, key, database_resp.val, KEY_CACHE_TTL_MEAN)
    stats.ok += 1
    return database_resp.val


def run(env: simpy.Environment, cache: KeyValueStorage, database: KeyValueStorage, stats: Stats) -> simpy.events.ProcessGenerator:
    # prepopulate dabase and cache
    for key in range(KEY_MAX):
        val = "Value {}".format(key)
        database.set(key, val)
        cache.set(key, val, random.uniform(0, KEY_CACHE_TTL_MEAN))
        #cache.set(key, val, KEY_CACHE_TTL_MEAN)
    print("database and cache populated")

    while True:
        yield env.timeout(random.normalvariate(REQUESTS_MEAN, REQUESTS_DEV))
        stats.requests += 1
        key = random.randint(0, KEY_MAX)
        env.process(backend(env, cache, database, key, stats))


env = simpy.Environment()
cache = KeyValueStorage(env, CACHE_CAPACITY, CACHE_TIMEOUT, CACHE_RESP_MEAN)
database = KeyValueStorage(env, DATABASE_CAPACITY, DATABASE_TIMEOUT, DATABASE_RESP_MEAN)
stats = Stats()
env.process(run(env, cache, database, stats))
env.run(until=SIMULATION_TIME)
print(stats)
