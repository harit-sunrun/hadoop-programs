#! /usr/bin/env python

import sys

for line in sys.stdin:
    fields = line.split('\t')
    time_split = fields[2].split(':')
    key = str(time_split[0]) + ':00'
    print 'LongValueSum:' + key + '\t' + '1'
    