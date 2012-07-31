#! /usr/bin/env python
"""
This class uses -reducer aggregate to call ValueHistogram of Aggregate package of Hadoop. 
ValueHistogram for each key, tells number of unique values(not this program though), minumum count, maximum count, median count, average count, std
"""
import sys

index1 = int(sys.argv[1])
index2 = int(sys.argv[2])

for line in sys.stdin:
    fields = line.split(',')
    print 'ValueHistogram:' + str(fields[index1]) + '\t' + str(fields[index2])