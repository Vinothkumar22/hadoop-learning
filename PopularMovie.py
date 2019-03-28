from mrjob.job import MRJob
from mrjob.step import MRStep

class PopularMovie(MRJob):
	
	def steps(self):
		return [
			MRStep(mapper = self.mapper_movie_count, reducer = self.reducer_none_count_movie),
			MRStep(reducer = self.reducer_popular_movie)
		]
	
	def mapper_movie_count(self, _, line):
		(user, movie, rating, time) = line.split('\t')
		yield movie, 1
	
	def reducer_none_count_movie(self, movie, occurences):
		yield None, (sum(occurences), movie)
	
	def reducer_popular_movie(self, _, count_movie):
		yield max(count_movie)
	
if __name__ == '__main__':
	PopularMovie.run()