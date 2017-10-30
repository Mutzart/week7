from mrjob.job import MRJob



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


if __name__ == '__main__':
    WordOccurances.run()
