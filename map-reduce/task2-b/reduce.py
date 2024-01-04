#!/usr/bin/python

import sys
current_range = None
current_count = 0

for line in sys.stdin:
    
    _range, count = line.strip().split("\t",1)
    try:
        _range = str(_range)
        count = int(count)
    except ValueError:
        continue
    
    if _range == current_range:
        current_count = current_count + 1
    else:
        if current_range:
            # output goes to STDOUT (stream data that the program writes)
            print(current_range+"\t"+str(current_count))
        current_range = _range
        current_count = 1
print(str(current_count))
