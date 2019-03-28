from mrjob.job import MRJob

class AvgFrndsForAge(MRJob):
	def mapper(self, key, line):
		(id, name, age, frndsCount) = line.split(',')
		yield float(age), float(frndsCount)
	
	def reducer(self, age, frndsCount):
		total = 0
		occurence = 0;
		for x in frndsCount:
			total += x
			occurence += 1
		yield age, (total / occurence)

if __name__ == '__main__':
    AvgFrndsForAge.run()