FileName = ("frontier_summary.txt")
data=file(FileName).readlines()
data.sort()
f = open("frontier_summary.txt", "w")
for line in data:
	f.write(line)