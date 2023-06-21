'''
5 5
999999991 999999992 999999993 999999994 999999999
999999991 999999993 999999995 999999999 999999997
999999990 999999992 999999996 999999998 999999994

Find happiness
'''

# numbers = input()
# n,m = numbers.split(' ')
# array = input().split(' ').__iter__()
# A = input().split(' ').__iter__()
# B = input().split(' ').__iter__()
# happiness = 0
# p = [j for j in array if j in list(A)]
# happiness += len(p)
# q = [j for j in array if j in list(B)]
# happiness -= len(q)
# print(int(happiness))


from collections import defaultdict
def special_data_table( number_of_slots, values, find_item ) :
	####### DO NOT MODIFY BELOW #######
	myTable = MySpecialTable(number_of_slots)
	for val in values:
		myTable.add_item(val)

	return myTable.find_item(find_item)
	####### DO NOT MODIFY ABOVE #######

class MySpecialTable():
    def __init__(self, slots):
        self.slots = slots
        self.table = defaultdict(list)
        self.create_table()

    def hash_key(self, value):
        return value%self.slots

    def create_table(self):
        self.table = defaultdict(list)

    def add_item(self, value):
        key = self.hash_key(value)
        self.table[key].append(value)

    def find_item(self, item):
        print('Table:   ', self.table)
        exis = [i for i in self.table if item in self.table[i]]
        print(exis)
        return exis[0] if exis else -1


special_data_table(int(3), [1,3,5,7,9,], int(3))