from collections import defaultdict
d = defaultdict(list)

print(d['q']) #### [] - empty list will be printed
d.setdefault('a', []).append(1,2,3,4)
