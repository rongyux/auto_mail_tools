#!/usr/bin/env python
"""
mapper
"""

import sys

sid_list = list()
for line in file(sys.argv[1]):
    data = line.strip().split('\t')
    if data[0] not in sid_list :
        sid_list.append(data[0])
 
for line in sys.stdin:
    data = line.strip("\n").split("\t")
    searchid = data[0]
    if searchid not in sid_list:
        continue
    print '\t'.join(data)
