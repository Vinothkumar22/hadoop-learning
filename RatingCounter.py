from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield userID, 1

    def reducer(self, rating, occurence):
        yield rating, sum(occurence)

if __name__ == '__main__':
    MRRatingCounter.run()
