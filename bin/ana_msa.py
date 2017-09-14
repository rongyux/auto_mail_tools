#!/usr/bin/env python
"""
mapper
"""

import sys

planid_list = list()
for line in file(sys.argv[1]):
    data = line.strip().split('\t')
    if data[0] not in planid_list :
        planid_list.append(data[0])

for line in sys.stdin:
    data = line.strip("\n").split("\t")
    searchid = data[0]
    #query = data[2]
    #userid = data[3]
    ##if userid != '30002':
    ##    continue
    planid = data[4]
    ppim_quality_flag = data[14]
    if planid in planid_list and ppim_quality_flag =='1':
        print '\t'.join([searchid,planid,ppim_quality_flag])
