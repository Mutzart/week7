from mrjob.job import MRJob
from mrjob.step import MRStep
from numpy import prod

# Count number of times a word appears in a text
class WordOccurances(MRJob):
    def mapper(self, _, line):
      # Loop over every word and define a mapper for each
      for word in line.split():
          # Send a unit pulse for each occurance of a word (converted to lower case)
          yield word.lower(), 1

    def reducer(self, key, values):
        # Sum the unit pulses yielded for each word
        yield key, sum(values)



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

    def euler_result(self,Graph,degree):
        # Ideally I would use the product of 1-(degree % 2),
        # But modulo and product are not JSON serializable...
        yield Graph, max(degree)

    def steps(self):
        return [MRStep(mapper=self.get_degrees,
                       mapper_final=self.sum_degrees,
                       combiner=self.odd_degrees,
                       reducer=self.sum_degrees),
                MRStep(reducer=self.euler_result)]


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

    def euler_result(self,Graph,degree):
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
    WordOccurances.run()
    EulerGraph.run()
    DegreeNode.run()