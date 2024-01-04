#!/usr/bin/python

import sys
count=0

# input comes from STDIN (stream data that goes to the program)
for row in sys.stdin:

    key, join = row.strip().split('\t',1)
    join = join.strip().split(',')
    if len(join) < 12:
        continue

    try:
        
        if float(join[11]) <=20:
            print("0,20"+"\t"+"1")
        elif float(join[11]) <=40 and float(join[11]) >20 :
            print("20.01,40"+"\t"+"1")
        elif float(join[11]) <=60 and float(join[11]) > 40 :
            print("40.01,60"+"\t"+"1")
        elif float(join[11]) <=80 and float(join[11]) > 60 :
            print("60.01,80"+"\t"+"1")
        elif float(join[11]) >80:
            print("80.01,infinite"+"\t"+"1")
    except:
        continue
