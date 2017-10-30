from mrjob.job import MRJob
from mrjob.step import MRStep
from numpy import prod

# Find occurances of odd degrees in a graph
class EulerGraph(MRJob):
    def get_degrees(self, _, line):
        for node in line.split():
            yield node, 1

    def sum_degrees(self, Node, degree):
        # Ideally I would use the product of 1-(degree % 2),
        # But modulo and product are not JSON serializable...
        yield Node, sum(degree)

    def odd_degrees(self, _, degree):
        # Send every node to the same reducer
        # yield 'Euler', (sum(degree))
        yield 'Euler', (degree % 2)

    def euler_result(self, Graph, degree):
        # Ideally I would use the product of 1-(degree % 2),
        # But modulo and product are not JSON serializable...
        yield Graph, max(degree)

    def steps(self):
        return [MRStep(mapper=self.get_degrees,
                       mapper_final=self.sum_degrees,
                       combiner=self.odd_degrees,
                       reducer=self.sum_degrees),
                MRStep(reducer=self.euler_result)]

if __name__ == '__main__':
    EulerGraph.run()
