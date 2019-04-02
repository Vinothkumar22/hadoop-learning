import sys

with open('BFS-iteration-0.txt', 'w') as out:
	with open('Marvel-graph.txt') as f:
		for line in f:
			fields = line.split()
			heroId = fields[0]
			connectionCount = len(fields) - 1
			connections = fields[-connectionCount:]
			
			color = 'WHITE'
			distance = 9999
			
			if (heroId == sys.argv[1]):
				color = 'GRAY'
				distance = 0
				
			if (heroId != ''):
				connectionsPart = ",".join(connections)
				outStr = "|".join((heroId, connectionsPart, str(distance), color))
				out.write(outStr)
				out.write("\n")
	f.close()
out.close()