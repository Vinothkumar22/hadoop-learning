from mrjob.job import MRJob

class MinTemp(MRJob):
	def toCelcium(self, tenthOfFaren):
		return float(tenthOfFaren) / 10.0
	
	def mapper(self, key, line):
		(station, dateTime, type, temp, _, _, _, _) = line.split(',')
		if type == 'TMIN':
			celcius = self.toCelcium(temp)
			yield station, celcius
	
	def reducer(self, station, temps):
		yield station, min(temps)
	
if __name__ == '__main__':
	MinTemp.run()