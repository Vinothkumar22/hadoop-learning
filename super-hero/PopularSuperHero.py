from mrjob.job import MRJob
from mrjob.step import MRStep

class PopularSuperHero(MRJob):
	def configure_args(self):
		super(PopularSuperHero, self).configure_args()
		self.add_file_arg("--addFile", help = "path to supporting files")

	def steps(self):
		return [
			MRStep(mapper = self.mapper_heroId_friendsCount, reducer_init = self.reducer_init_hero_name, reducer = self.reducer_none_totalFriendsCountHeroId),
			MRStep(reducer = self.reducer_popular_hero)
		]
		
	def mapper_heroId_friendsCount(self, _, line):
		entries = line.split()
		yield int(entries[0]), int(len(entries) - 1)
	
	def reducer_init_hero_name(self):
		self.heroIdNameIndex = {}
		with open("Marvel-Names.txt") as f:
			for line in f:
				fields = line.split('"')
				self.heroIdNameIndex[int(fields[0])] = fields[1]
	
	def reducer_none_totalFriendsCountHeroId(self, heroId, friendsCount):
		yield None, (sum(friendsCount), self.heroIdNameIndex[heroId])
	
	def reducer_popular_hero(self, _, totalFriendsCountHero):
		yield max(totalFriendsCountHero)
	
if __name__ == '__main__':
	PopularSuperHero.run()