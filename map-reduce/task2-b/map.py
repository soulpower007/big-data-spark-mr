#!/usr/bin/python

import sys
count=0

# input comes from STDIN (stream data that goes to the program)
for row in sys.stdin:

    key, join = row.strip().split('\t',1)
    join = join.strip().split(',')
    try:

        if float(join[16]) <=15:
            print("#"+"\t"+"1")
    except:
        continue

