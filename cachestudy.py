import salabim as sim
import random

class Request(sim.Component):
    def __init__(self):
        super().__init__()
        self.key = random.randint(0, 1024)

    def process(self):
        # read from cache
        self.request(cache, fail_delay=30)
        self.hold(sim.Normal(5, 1).sample())

        self.release()
        if random.randint(0, 100) < 5:
            # read from database
            self.request(database, fail_delay=300)
            self.hold(sim.Normal(100, 25).sample())
            self.release()
            # update cache
            self.request(cache, fail_delay=30)
            self.hold(sim.Normal(5, 1).sample())
            self.release()

        # guard
        self.release()

class RequestGenerator(sim.Component):
    def process(self):
        while True:
            Request()
            self.hold(sim.Uniform(0, 10).sample())

class Cache(sim.Resource):
    pass

env = sim.Environment()
RequestGenerator()
database = sim.Resource("database", capacity=1)
cache = Cache("cache", capacity=1)
env.run(till=100000)

cache.print_statistics()
cache.print_info()
database.print_statistics()
database.print_info()
