from mrjob.job import MRJob
from mrjob.step import MRStep

class PopularMovieName(MRJob):

	def configure_args(self):
		super(PopularMovieName, self).configure_args()
		self.add_file_arg('--adFile', help = 'hello')

	def steps(self):
		return [
			MRStep(mapper = self.mapper_movieId_occurence, reducer_init = self.reducer_init, reducer = self.reducer_None_countMovieName),
			MRStep(reducer = self.reducer_populer_movie_name)
		]

	def mapper_movieId_occurence(self, _, line):
		(_, movieId, _, _) = line.split('\t')
		yield movieId, 1
	
	def reducer_init(self):
		self.movieIdNameIndex = {}
		
		with open("u.item") as f:
			for line in f:
				fields = line.split("|")
				self.movieIdNameIndex[fields[0]] = fields[1]
		
	def reducer_None_countMovieName(self, movieId, occurences):
		yield None, (sum(occurences), self.movieIdNameIndex[movieId])
	
	def reducer_populer_movie_name(self, _, countMovieName):
		yield max(countMovieName)
	
if __name__ == '__main__':
	PopularMovieName.run()