from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict

class RelatedMoviesIndexProcess(MRJob):

    def configure_options(self):
        super(RelatedMoviesIndexProcess, self).configure_options()
        self.add_file_option("--item", help = "File location to prep userId and movie rating index")

    def steps(self):
        return [
            MRStep(mapper = self.mapper, reducer_init = self.reducer_init, reducer = self.reducer)
        ]

    def reducer_init(self):
        self.userIdMovieIdsIndex = defaultdict(list)
        with open("u.data") as f:
            for line in f:
                (userId, movieId, rating, _) = line.split()
                self.userIdMovieIdsIndex[userId].append((movieId, rating))
            f.close()

    def mapper(self, _, line):
        (userId, movieId, rating, _) = line.split()
        yield movieId, (userId, rating)

    def reducer(self, movieId, userIdRatingPairs):
        for userIdRatingPair in userIdRatingPairs:
            userId = userIdRatingPair[0]
            rating = userIdRatingPair[1]
            for relatedMovieIdRatingPair in self.userIdMovieIdsIndex[userId]:
                possibleRelatedMovieId = relatedMovieIdRatingPair[0]
                possibleRelatedRating = relatedMovieIdRatingPair[1]
                yield movieId, possibleRelatedMovieId

if __name__ == '__main__':
    RelatedMoviesIndexProcess.run()
