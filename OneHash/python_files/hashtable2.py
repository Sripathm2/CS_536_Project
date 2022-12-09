# Import statements for the notebook
import sys
from turtle import clear
from numpy import NaN
import numpy as np
import pandas as pd
import itertools
import time
from functools import partial
from bitstring import BitArray
from scapy.all import *
from tqdm import tqdm
from scapy.utils import rdpcap
from multiprocessing import Pool
from scipy.stats import pearsonr
sys.path.insert(0, '../Python_files/')
from collections import defaultdict
import hashlib
#import matpotlib
import getopt
import argparse
import json
import zlib

parser = argparse.ArgumentParser(
                    prog = 'ProgramName',
                    description = 'What the program does',
                    epilog = 'Text at the bottom of help')

#parser.add_argument('filename')           # positional argument
parser.add_argument('-hset', '--hashset')      # option that takes a value
parser.add_argument('-p', '--percentile')      # option that takes a value
parser.add_argument('-n', '--numflow')      # option that takes a value
parser.add_argument('-func', '--function')  # on/off flag

args = parser.parse_args()
print(args.hashset, args.percentile, args.function, args.numflow)
# #print(args.percentile, args.function, args.numflow)
# print(type(args.hashset), type(args.percentile), type(args.function), type(args.numflow))

default_file_to_work_with = './ucsb.pcap'

trafficCountGroundTrue = defaultdict(int)
#trafficCountGroundTrue = {}
trafficCountEstimated = defaultdict(int)
hashKeyToFlow = defaultdict(set)
totalflow = set()
totalhashkey = set()

targetFlowCount = int(args.numflow) ## targetFlowCount can also be changed; To be changed
modSize = int(args.hashset) ### the mod size is 10K now; it can be changed later
percentile = float(args.percentile) ## percentile can change; To be Changed
percentileCount = targetFlowCount * percentile

# print(targetFlowCount, modSize, percentile)
#exit(1)


def udfhash(inputhash, flowString):
    if inputhash == "sha512":
        newhash = hashlib.sha512(flowString.encode())
    elif inputhash == "md5":
        newhash = hashlib.md5(flowString.encode())
    elif inputhash == "crc32":
        newhash = zlib.crc32(flowString.encode())


    return newhash

def flowToHashIndex(flowString, modSize):
    newhash = udfhash(args.function, flowString)
    #newhash = hashlib.sha512(flowString.encode())
    hexhash = newhash.hexdigest()
    decimalHash = int(hexhash, 16)
    hashKey = decimalHash % modSize

    return hashKey


def process_file_5min(file_to_work_with, output_file, max_limit_of_files):
    dataset_list = []
    count =0
    ipv6_handling = {}
    ip_counter = 0
    file = open('5tuple.csv', 'r')
    lines = file.readlines()
    #print(file_to_work_with)
    for flowstring in tqdm(lines):
        
         
        #trafficCount = {}
        temp_dict = {'proto':'-1','src':'-1','dst':'-1','sport':'-1','dport':'-1',
            'ip_len':'-1','ip_id':'-1','tcp_ack':'-1',
            'tcp_data_offset':'-1','ip_flags':'-1','tcp_flags':'-1',
            'ip_frag':'-1','ip_tos':'-1','ip_ihl':'-1',
            'ip_ttl':'-1','tcp_window':'-1','tcp_urgptr':'-1'}
        combined = ''
        if count%10000 == 0:
            print('read ' + str(count))

        totalflow.add(flowstring)
        #print(flowstring)
        trafficCountGroundTrue[flowstring] = trafficCountGroundTrue[flowstring] + 1 ### record the ground true count
        newhash = udfhash(args.function, flowstring)
        #newhash = hashlib.sha512(flowstring.encode())
        if args.function == "md5" or args.function == "sha512":
            hexhash = newhash.hexdigest()
            decimalHash = int(hexhash, 16)
            #print("decimalHash is ", decimalHash)

        else:
            decimalHash = newhash
            #print("decimalHash is ", decimalHash)
        totalhashkey.add(decimalHash)
        #print(type(hexhash))
        #print(type(decimalHash))
        hashKey = decimalHash % modSize
        trafficCountEstimated[hashKey] += 1
        hashKeyToFlow[hashKey].add(flowstring)

        if count%max_limit_of_files == 0 and count != 0:
           break
        count += 1

        add_to_clean = True
        for key in temp_dict.keys():
            if temp_dict[key] == '-1':
                add_to_clean = False
            elif '_bin' not in key and key != 'src' and key != 'dst':
                temp_dict[key] = float(temp_dict[key])
        if add_to_clean:
            dataset_list.append(temp_dict)
    len(dataset_list)
    df = pd.DataFrame(dataset_list)
    df.to_pickle(output_file)
    #print(df)

    #print(trafficCountGroundTrue)
    GroundTrueCount = []
    for flow in trafficCountGroundTrue:
        GroundTrueCount.append([trafficCountGroundTrue[flow], flow])

    EstimatedCount = []

    #for hashKey in hashKeyToFlow:
    #    for flow in hashKeyToFlow[hashKey]:
    #        EstimatedCount.append([trafficCountEstimated[hashKey], flow])
    # for hashKey in trafficCountEstimated:
    #     EstimatedCount.append([trafficCountEstimated[hashKey], hashKey])


    for hashkey in trafficCountEstimated:
        for flowstring in hashKeyToFlow[hashkey]:
            EstimatedCount.append([trafficCountEstimated[hashkey], flowstring])

    GroundTrueCount = sorted(GroundTrueCount, key = lambda x: x[0], reverse = True)
    EstimatedCount = sorted(EstimatedCount, key = lambda x: x[0], reverse = True)

    GroundTrueSet = set()
    EstimatedSet = set()
    currcount1 = 0
    for count, flow in GroundTrueCount: 
        currcount1 += count
        if currcount1 > percentileCount: break
        else: GroundTrueSet.add(flow)

    # GroundTrueHashIndex = set()
    # for flow in GroundTrueSet:
    #     key = flowToHashIndex(flow, modSize)
    #     GroundTrueHashIndex.add(key)

    # currcount2 = 0
    # for count, key in EstimatedCount: 
    #     currcount2 += count
    #     if currcount2 > percentileCount: break
    #     else: EstimatedSet.add(key)

    totalEstimatedCount = 0
    for count,flow in EstimatedCount:
        totalEstimatedCount += count
    
    EstimatedPercentileCount = totalEstimatedCount * percentile
    currcount2 = 0
    for count, flow in EstimatedCount: 
        currcount2 += count
        if currcount2 > EstimatedPercentileCount: break
        else: EstimatedSet.add(flow)

    print("size of the flow is", len(totalflow))
    print("size of the hashkey is", len(totalhashkey))
    print("size of groundTrue set", len(GroundTrueSet))
    print("size of Esitmated set", len(EstimatedSet))

    intersectionSet = GroundTrueSet - GroundTrueSet.difference(EstimatedSet)
    intersectionSetSize = len(intersectionSet)
    combinedSetSize = len(GroundTrueSet) + len(EstimatedSet) - intersectionSetSize
    accuracy = intersectionSetSize / combinedSetSize
    print("the Accuracy is", accuracy)

    outfile = str(modSize) + "_" + str(percentile) + "_" + args.function + ".json" ## ./modsize_percentile_function.json

    filehandle = open(outfile, "w")

    histogram_map = {}

    for i in range(modSize):
        histogram_map[i] = len(hashKeyToFlow[i])
        #(hashKeyToFlow[i])
    
    dictionary = {
    "accuracy": accuracy,
    "histogram_map": histogram_map
    }


    json.dump(dictionary, filehandle)

if __name__ == '__main__':
    process_file_5min(default_file_to_work_with, 'Dataset.pkl', 60000000)