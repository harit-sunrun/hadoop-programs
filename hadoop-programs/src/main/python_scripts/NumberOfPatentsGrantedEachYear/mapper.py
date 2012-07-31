#! /usr/bin/env python

"""
This class makes use of Aggregate package in Hadoop. uses -reduce aggregate
"""
import sys

index = int(sys.argv[1])
for line in sys.stdin:
    fields = line.split(',')
    print 'LongValueSum:' + str(fields[index]) + '\t' + '1'