import sys

with open('dos-itr-1.txt', 'w') as out:
	with open('Marvel-graph.txt') as f:
		for line in f:
			fields = line.split()
			adjacents = fields[-len(fields) - 1:]
			
			adjacentsStr = ",".join(adjacents)
			
			level = 9999#infinity
			state = "UNTOUCHED"
			if (fields[0] == sys.argv[1]):
				level = 0
				state = "TO_BE_VISITED"
				
			outStr = "|".join((fields[0], adjacentsStr, str(level), state))
			out.write(outStr)
			out.write("\n")
	f.close()
out.close()