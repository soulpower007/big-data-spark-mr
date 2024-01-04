#!/usr/bin/python

import sys
count=0

# input comes from STDIN (stream data that goes to the program)
for row in sys.stdin:
    
    key, join = row.strip().split('\t',1)
    key = key.strip().split(',')
    join = join.strip().split(',')
    try:
        print( key[3][0:10]+"\t"+str(float( join[11])+float(join[12] )+float(join[14]) )+","+str( join[14]) )
    except:
        continue

