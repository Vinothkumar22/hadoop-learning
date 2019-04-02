from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class DosProcess(MRJob):
	
	INPUT_PROTOCOL = RawValueProtocol
	OUTPUT_PROTOCOL = RawValueProtocol
	
	def configure_options(self):
		super(DosProcess, self).configure_options()
		self.add_passthrough_option("--target", help = "Input for target hero ID to get DOS")

	def mapper(self, _, line):
		fields = line.split("|")
		
		heroId = fields[0]
		connections = fields[1].split(",")
		level = int(fields[2])
		state = fields[3]
		
		if (state == 'TO_BE_EXPLORED'):
			for connection in connections:
				if (self.options.target == connection):
					counterName = ("Target ID " + connection + " was hit with distance " + str(level + 1))
					self.increment_counter('Degrees of Separation',	counterName, 1)
				yield connection, "|".join((connection, "", str(level + 1), "TO_BE_EXPLORED"))
			state = "EXPLORED"
				
		yield heroId, "|".join((heroId, ",".join(connections), str(level), state))
		
	def reducer(self, heroId, lines):
		connections = []
		level = 9999
		state = "UNTOUCHED"
		for line in lines:
			fields = line.split("|")
			connections.extend(fields[1].split(","))
			if (int(level) > int(fields[2])):
				level = fields[2]
			if (fields[3] == "EXPLORED"):
				state = "EXPLORED"
			if (state == "UNTOUCHED" and fields[3] == "TO_BE_EXPLORED"):
				state = fields[3]
		yield heroId, "|".join((heroId, ",".join(connections), str(level), state))

if __name__ == '__main__':
	DosProcess.run()