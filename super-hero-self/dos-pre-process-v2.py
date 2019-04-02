from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class DOSPreProcess(MRJob):

	INPUT_PROTOCOL = RawValueProtocol
	OUTPUT_PROTOCOL = RawValueProtocol

	def configure_options(self):
		super(DOSPreProcess, self).configure_options()
		self.add_passthrough_option("--root", help = "Root hero ID")

	def mapper(self, _, line):
		fields = line.split()
		adjacents = fields[-len(fields)-1:]
		adjacentsStr = " ".join(adjacents)
			
		yield fields[0], adjacentsStr
	
	def reducer(self, heroId, lines):
		adjacents = []
		level = 9999
		state = "UNTOUCHED"
		for adjStr in lines:
			adjacents.extend(adjStr.split())
			
			if (self.options.root == heroId):
				level = 0
				state = "TO_BE_EXPLORED"
		
		adjacentsStr = ",".join(adjacents)
		yield heroId, "|".join((heroId, adjacentsStr, str(level), state))
	
if __name__ == '__main__':
	DOSPreProcess.run()