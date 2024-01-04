#!/usr/bin/python

import sys
current_range = None
current_tots = 0
current_tips = 0

for line in sys.stdin:
    
    _range, count = line.strip().split("\t",1)
    
    tots,tips = count.strip().split(",",1)

    try:
        _range = str(_range)
        tots = float(tots)
        tips = float(tips)
    
    except ValueError:
        continue
    
    if _range == current_range:
        current_tots += tots
        current_tips +=tips
    else:
        if current_range:
            # output goes to STDOUT (stream data that the program writes)
            print(current_range+"\t"+str(round(current_tots,2))+","+str(round(current_tips,2)))
        current_range = _range
        current_tots = tots
        current_tips = tips
print(current_range+"\t"+str(round(current_tots,2))+","+str(round(current_tips,2)))
