#!/usr/bin/python

import sys
count=0

# input comes from STDIN (stream data that goes to the program)
for row in sys.stdin:

    key, join = row.strip().split('\t',1)
    join = join.strip().split(',')
    if len(join) < 12:
        continue
    print(join[3]+"\t"+"1")
