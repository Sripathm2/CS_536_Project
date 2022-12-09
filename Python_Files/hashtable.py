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
    #print(file_to_work_with)
    for pkt in PcapReader(file_to_work_with):
        if count == targetFlowCount: break ### only process certain number of packets
        
         
        #trafficCount = {}
        temp_dict = {'proto':'-1','src':'-1','dst':'-1','sport':'-1','dport':'-1',
            'ip_len':'-1','ip_id':'-1','tcp_ack':'-1',
            'tcp_data_offset':'-1','ip_flags':'-1','tcp_flags':'-1',
            'ip_frag':'-1','ip_tos':'-1','ip_ihl':'-1',
            'ip_ttl':'-1','tcp_window':'-1','tcp_urgptr':'-1'}
        combined = ''
        if count%10000 == 0:
            print('read ' + str(count))
        #trafficCount[str(pkt.payload.src) + str(pkt.payload.sport) + str(pkt.payload.dst) + str(pkt.payload.dport) + str(pkt.payload.proto)] += 1
        if not (hasattr(pkt, 'payload') and hasattr(pkt.payload, 'sport')): continue        
        if not hasattr(pkt.payload, 'proto'): continue
        if not (hasattr(pkt, 'payload') and hasattr(pkt.payload, 'dport')): continue

        mysport, mydport, myproto = pkt.payload.sport, str(pkt.payload.dport), str(pkt.payload.proto)
        flowstring = pkt.payload.src + " - " + str(mysport) + " - " + pkt.payload.dst + " - " + str(mydport) + " - " + str(myproto) 
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
        if hasattr(pkt.payload, 'proto'):
            temp_dict['proto'] = str(pkt.payload.proto)
            temp_dict['proto_bin'] = BitArray(uint=int(pkt.payload.proto), length=8).bin
        if hasattr(pkt.payload, 'src'):
            temp_dict['src'] = str(pkt.payload.src)
            bitarr = ''
            try:
                for x in pkt.payload.src.split('.'):
                    bitarr += BitArray(uint=int(x), length=8).bin
            except:
                if str(pkt.payload.src) not in ipv6_handling:
                    ipv6_handling[str(pkt.payload.src)] = BitArray(uint=ip_counter, length=32).bin
                    ip_counter += 1
                bitarr = ipv6_handling[str(pkt.payload.src)]
            temp_dict['src_bin'] = bitarr
        if hasattr(pkt.payload, 'dst'):
            temp_dict['dst'] = str(pkt.payload.dst)
            bitarr = ''
            try:
                for x in pkt.payload.dst.split('.'):
                    bitarr += BitArray(uint=int(x), length=8).bin
            except:
                if str(pkt.payload.dst) not in ipv6_handling:
                    ipv6_handling[str(pkt.payload.dst)] = BitArray(uint=ip_counter, length=32).bin
                    ip_counter += 1
                bitarr = ipv6_handling[str(pkt.payload.dst)]
            temp_dict['dst_bin'] = bitarr    
        if hasattr(pkt, 'payload') and hasattr(pkt.payload, 'sport'):
            temp_dict['sport'] = str(pkt.payload.sport)
            #print("temp_dict['sport']", temp_dict['sport'])
            temp_dict['sport_bin'] = BitArray(uint=int(str(int(pkt.payload.sport))[0:5]), length=16).bin
            #print("temp_dict['sport_bin']", temp_dict['sport_bin'])

        if hasattr(pkt, 'payload') and hasattr(pkt.payload, 'dport'):
            temp_dict['dport'] = str(pkt.payload.dport)
            temp_dict['dport_bin'] = BitArray(uint=int(pkt.payload.dport), length=16).bin
        if hasattr(pkt.payload, 'ihl'):
            temp_dict['ip_ihl'] = str(pkt.payload.ihl)
            temp_dict['ip_ihl_bin'] = BitArray(uint=int(pkt.payload.ihl), length=4).bin
        if hasattr(pkt.payload, 'id'):
            temp_dict['ip_id'] = str(pkt.payload.id)
            temp_dict['ip_id_bin'] = BitArray(uint=int(str(int(pkt.payload.id))[0:5]), length=16).bin
        if hasattr(pkt.payload, 'len'):
            temp_dict['ip_len'] = str(pkt.payload.len)
            temp_dict['ip_len_bin'] = BitArray(uint=int(pkt.payload.len), length=16).bin
        if hasattr(pkt.payload, 'frag'):
            temp_dict['ip_frag'] = str(pkt.payload.frag)
            temp_dict['ip_frag_bin'] = BitArray(uint=int(pkt.payload.frag), length=13).bin
        if hasattr(pkt.payload, 'tos'):
            temp_dict['ip_tos'] = str(pkt.payload.tos)
            temp_dict['ip_tos_bin'] = BitArray(uint=int(pkt.payload.tos), length=8).bin
        if hasattr(pkt.payload, 'ttl'):
            temp_dict['ip_ttl'] = str(pkt.payload.ttl)
            temp_dict['ip_ttl_bin'] = BitArray(uint=int(pkt.payload.ttl), length=8).bin
        if hasattr(pkt.payload, 'flags') and hasattr(pkt.payload.flags, 'flagrepr') and int(pkt.payload.flags.value) < 9:
            temp_dict['ip_flags'] = str(pkt.payload.flags.value)
            temp_dict['ip_flags_bin'] = BitArray(uint=int(pkt.payload.flags.value), length=3).bin
        if hasattr(pkt.payload, 'payload') and hasattr(pkt.payload.payload, 'flags') and hasattr(pkt.payload.payload.flags, 'flagrepr'):
            temp_dict['tcp_flags'] = str(pkt.payload.payload.flags.value)
            temp_dict['tcp_flags_bin'] = BitArray(uint=int(pkt.payload.flags.value), length=9).bin
        if hasattr(pkt.payload, 'payload') and hasattr(pkt.payload.payload, 'ack'):
            temp_dict['tcp_ack'] = str(pkt.payload.payload.ack)
            temp_dict['tcp_ack_bin'] = BitArray(uint=int(pkt.payload.payload.ack), length=32).bin
        if hasattr(pkt.payload, 'payload') and hasattr(pkt.payload.payload, 'dataofs') and pkt.payload.payload.dataofs is not None:
            temp_dict['tcp_data_offset'] = str(pkt.payload.payload.dataofs)
            temp_dict['tcp_data_offset_bin'] = BitArray(uint=int(pkt.payload.payload.dataofs), length=4).bin
        if hasattr(pkt.payload, 'payload') and hasattr(pkt.payload.payload, 'window'):
            temp_dict['tcp_window'] = str(pkt.payload.payload.window) 
            temp_dict['tcp_window_bin'] = BitArray(uint=int(pkt.payload.payload.window), length=16).bin
        if hasattr(pkt.payload, 'payload') and hasattr(pkt.payload.payload, 'urgptr'):
            temp_dict['tcp_urgptr'] = str(pkt.payload.payload.urgptr)
            temp_dict['tcp_urgptr_bin'] = BitArray(uint=int(pkt.payload.payload.urgptr), length=16).bin
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

    # print("size of the flow is", len(totalflow))
    # print("size of the hashkey is", len(totalhashkey))
    # print("size of groundTrue set", len(GroundTrueSet))
    # print("size of Esitmated set", len(EstimatedSet))

    intersectionSet = GroundTrueSet - GroundTrueSet.difference(EstimatedSet)
    intersectionSetSize = len(intersectionSet)
    combinedSetSize = len(GroundTrueSet) + len(EstimatedSet) - intersectionSetSize
    accuracy = intersectionSetSize / combinedSetSize
    # print("the Accuracy is", accuracy)

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