from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict

import codecs

FILE_NAME_RATING="ratings.csv"
FILE_NAME_MOVIES="movies.csv"
DELIMITTER_RATING=","
DELIMITTER_MOVIES=","

class RelatedMoviesIndexProcess(MRJob):

    def configure_options(self):
        super(RelatedMoviesIndexProcess, self).configure_options()
        self.add_file_option("--item", help = "File location to prep userId and movie rating index")
        self.add_file_option("--movieNameFile", help = "File location to prep movieId and Movie name index")

    def steps(self):
        return [
            MRStep(mapper = self.mapper, reducer_init = self.reducer_init, reducer = self.reducer)
        ]

    def reducer_init(self):
        self.userIdMovieIdsIndex = defaultdict(list)
        with open(FILE_NAME_RATING) as f:
            for line in f:
                (userId, movieId, rating, _) = line.split(DELIMITTER_RATING)
                if (userId == "userId"):
                    continue
                if (float(rating) > 2):
                    self.userIdMovieIdsIndex[userId].append((movieId, rating))
            f.close()

        self.movieIdNameIndex = {}
        with codecs.open(FILE_NAME_MOVIES, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                fields = line.split(DELIMITTER_MOVIES)
                if (fields[0] == "movieId"):
                    continue
                self.movieIdNameIndex[fields[0]] = fields[1]
            f.close()

    def mapper(self, _, line):
        (userId, movieId, rating, _) = line.split(DELIMITTER_RATING)
        if (userId != "userId"):
            yield movieId, (userId, rating)

    def reducer(self, movieId, userIdRatingPairs):
        for userIdRatingPair in userIdRatingPairs:
            userId = userIdRatingPair[0]
            rating = float(userIdRatingPair[1])
            for relatedMovieIdRatingPair in self.userIdMovieIdsIndex[userId]:
                possibleRelatedMovieId = relatedMovieIdRatingPair[0]
                if (possibleRelatedMovieId == movieId):
                    continue
                possibleRelatedRating = float(relatedMovieIdRatingPair[1])
                if (((rating >= possibleRelatedRating) and (rating - possibleRelatedRating < 2))
                or ((rating <= possibleRelatedRating) and possibleRelatedRating - rating < 2)):
                    yield self.movieIdNameIndex[movieId], self.movieIdNameIndex[possibleRelatedMovieId]

if __name__ == '__main__':
    RelatedMoviesIndexProcess.run()
