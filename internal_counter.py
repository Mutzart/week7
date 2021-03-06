from mrjob.job import MRJob
from mrjob.step import MRStep
from numpy import prod

# Own exercise
class DegreeNode(MRJob):
    def init_get_degrees(self):
        self.degrees = {}

    # internal tracker of the degrees in each node
    def get_degrees(self, _, line):
        for node in line.split():
            self.degrees.setdefault(node, 0)
            # Increment for each occurance in the doc
            self.degrees[node] = self.degrees[node] + 1

    def final_get_degrees(self):
        for node, degree in self.degrees.iteritems():
            # Send each node through the combiner checking for even degree
            yield node, (degree)

    def sum_degrees(self, node, degree):
        # Send every node to the same reducer
        # yield 'Euler', (sum(degree))
        yield node, sum(degree)

    def euler_result(self, Graph, degree):
        # Ideally I would use the product of 1-(degree % 2),
        # But modulo and product are not JSON serializable...
        yield Graph, max(degree)

    def steps(self):
        return [MRStep(mapper_init=self.init_get_degrees,
                       mapper=self.get_degrees,
                       mapper_final=self.final_get_degrees,
                       combiner=self.sum_degrees,
                       reducer=self.sum_degrees),
                MRStep(reducer=self.euler_result)]


if __name__ == '__main__':
    DegreeNode.run()
