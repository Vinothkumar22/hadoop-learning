from mrjob.job import MRJob
from mrjob.step import MRStep

import re


REGEX_WORD = re.compile(r"[\w']+")

class WordCountMapperChain(MRJob):
	
	def steps(self):
		return [
			MRStep(mapper = self.mapper_word_count		, reducer = self.reducer_word_count),
			MRStep(mapper = self.mapper_sort_occurence	, reducer = self.reducer_sort_occurence)
		]

	def mapper_word_count(self, _, line):
		words = REGEX_WORD.findall(line)
		for word in words:
			yield word.lower(), 1
	
	def reducer_word_count(self, word, occurence):
		yield word, sum(occurence)
	
	def mapper_sort_occurence(self, word, occurence):
		yield '%04d'%int(occurence), word
	
	def reducer_sort_occurence(self, occurence, words):
		for word in words:
			yield occurence, word
			
			
			
if __name__ == '__main__':
	WordCountMapperChain.run()