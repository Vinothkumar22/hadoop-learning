from mrjob.job import MRJob
import re


REGEX_WORD = re.compile(r"[\w']+")

class WordCount(MRJob):
	def mapper(self, _, line):
		words = REGEX_WORD.findall(line)
		for word in words:
			yield word.lower(), 1
	
	def reducer(self, word, occurence):
		yield word, sum(occurence)
		
if __name__ == '__main__':
	WordCount.run()