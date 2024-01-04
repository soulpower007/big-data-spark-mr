#!/usr/bin/python

import sys
count=0

# input comes from STDIN (stream data that goes to the program)
for row in sys.stdin:

    ele = row.strip().split(',')
    # exclude header
    if ele[0]=='medallion':
        continue
    key = ""
    val = ""

    # fares
    if len(ele)==11:
        key += ele[0] + "," + ele[1] + "," + ele[2] + "," + ele[3]
        val += ele[4] + "," + ele[5] + "," + ele[6] + "," + ele[7]+ "," + ele[8] + "," + ele[9] + "," + ele[10]
        print "%s\t%s"%(key,val)
    #trips
    elif len(ele)==14:
        key += ele[0] + "," + ele[1] + "," + ele[2] + "," + ele[5]
        val += ele[3] + "," + ele[4] + "," + ele[6] + "," + ele[7]+ "," + ele[8] + "," + ele[9] + "," + ele[10] + ","+ ele[11] + "," + ele[12] + "," + ele[13] + ","
        print(key+"\t"+val)    

