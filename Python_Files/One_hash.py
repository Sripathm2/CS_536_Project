# Import statements for the notebook
import sys
from turtle import clear
from numpy import NaN
from functools import partial
from scapy.all import *
from tqdm import tqdm
from multiprocessing import Pool
sys.path.insert(0, '../Python_files/')
from collections import defaultdict
import hashlib



trafficCountGroundTrue = defaultdict(int)
trafficCountEstimated = defaultdict(int)
hashKeyToFlow = defaultdict(set)
totalflow = set()
totalhashkey = set()

targetFlowCount = 10000000 ## targetFlowCount can also be changed; To be changed
modSize = 10000 ### the mod size is 10K now; it can be changed later
percentile = 0.95 ## percentile can change; To be Changed
percentileCount = targetFlowCount * 0.95

def flowToHashIndex(flowString, modSize):
    newhash = hashlib.sha512(flowString.encode())
    hexhash = newhash.hexdigest()
    decimalHash = int(hexhash, 16)
    hashKey = decimalHash % modSize

    return hashKey


def process_file_5min():
    file = open('/mnt/ngas/Datasets/ucsb/5tuple.csv', 'r')
    print('reading lines')
    lines = file.readlines()
    count =0

    print('generating count')
    for line in tqdm(lines):
        #if count == targetFlowCount: break ### only process certain number of packets
        count += 1 
        newstring = line
        totalflow.add(newstring)
        trafficCountGroundTrue[newstring] = trafficCountGroundTrue[newstring] + 1 ### record the ground true count
        newhash = hashlib.sha512(newstring.encode())
        hexhash = newhash.hexdigest()
        decimalHash = int(hexhash, 16)
        totalhashkey.add(decimalHash)
        hashKey = decimalHash % modSize
        trafficCountEstimated[hashKey] += 1
        hashKeyToFlow[hashKey].add(newstring)

    GroundTrueCount = []
    print('trafficCountGroundTrue loop')
    for flow in tqdm(trafficCountGroundTrue):
        GroundTrueCount.append([trafficCountGroundTrue[flow], flow])

    EstimatedCount = []
    print('trafficCountEstimated loop')
    for hashKey in tqdm(trafficCountEstimated):
        EstimatedCount.append([trafficCountEstimated[hashKey], hashKey])

    GroundTrueCount = sorted(GroundTrueCount, key = lambda x: x[0], reverse = True)
    EstimatedCount = sorted(EstimatedCount, key = lambda x: x[0], reverse = True)

    GroundTrueSet = set()
    EstimatedSet = set()
    currcount1 = 0
    print('GroundTrueCount loop')
    for count, flow in tqdm(GroundTrueCount): 
        currcount1 += count
        if currcount1 > percentileCount: break
        else: GroundTrueSet.add(flow)

    GroundTrueHashIndex = set()
    print('GroundTrueSet loop')
    for flow in tqdm(GroundTrueSet):
        key = flowToHashIndex(flow, modSize)
        GroundTrueHashIndex.add(key)

    currcount2 = 0
    print('EstimatedCount loop')
    for count, key in tqdm(EstimatedCount): 
        currcount2 += count
        if currcount2 > percentileCount: break
        else: EstimatedSet.add(key)

    print("size of the flow is", len(totalflow))
    print("size of the hashkey is", len(totalhashkey))
    print("size of groundTrue set", len(GroundTrueSet))
    print("size of Esitmated set", len(EstimatedSet))
    print("the Accuracy is", 1 - len(EstimatedSet.difference(GroundTrueSet)) / len(GroundTrueSet))

if __name__ == '__main__':
    process_file_5min()