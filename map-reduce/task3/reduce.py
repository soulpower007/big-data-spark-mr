#!/usr/bin/python

import sys

current_key = None
current_join = ""
join_count=0
# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
    
    key, join = line.strip().split("\t",1)
    
    try:
        key = str(key)
        join = str(join)
    except ValueError:
        continue
    
    if key == current_key:
        # check if fares...we appended a comma when emitting out fares data
        if current_join[-1]==',':
            current_join += join
            join_count+=1
        else:
            current_join = join+current_join
            join_count+=1
    else:
        if current_key and join_count==2:
            # output goes to STDOUT (stream data that the program writes)
            print "%s\t%s" %(current_key,current_join)
        join_count=1
        current_key = key
        current_join = join

if join_count==2:
    print "%s\t%s"%(current_key,current_join)

