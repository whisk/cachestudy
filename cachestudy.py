import salabim as sim

class Request(sim.Component):
    def process(self):
        self.request(database)
        self.hold(sim.Uniform(0, 100).sample())
        self.release()

class RequestGenerator(sim.Component):
    def process(self):
        while True:
            Request()
            self.hold(sim.Uniform(0, 100).sample())

env = sim.Environment()
RequestGenerator()
database = sim.Resource("database", capacity=1)
env.run(till=50000)

database.print_statistics()
database.print_info()
