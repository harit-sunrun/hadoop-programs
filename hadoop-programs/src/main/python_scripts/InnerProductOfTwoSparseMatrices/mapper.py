#! /usr/bin/env python

"""
Assumption : all inputs in matrix are integer
If they contain decimals, change the multiplication to
__get_float_multiplication
"""

import sys
key = '1'

def __get_integer_multiplication(x1, x2):
	return int(x1) * int(x2)

def __get_float_multiplication(x1, x2):
	return float(x1) * float(x2)

for line in sys.stdin:
	fields = line.split(',')
	if fields[0] == "0" and fields[1] == "0":
		continue
	print 'LongValueSum:' + key + '\t' + str(__get_integer_multiplication(fields[0], fields[1]))

