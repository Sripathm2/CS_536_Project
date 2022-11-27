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
parser.add_argument('-n', '--numflow')      # option that takes a value

args = parser.parse_args()
print(args.numflow)
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
csvFile = "5tuple.csv"

def process_file_5min(file_to_work_with, output_file, max_limit_of_files):
    dataset_list = []
    count =0
    ipv6_handling = {}
    ip_counter = 0
    csvFileHandle = open(csvFile, "w")
    #print(file_to_work_with)
    for pkt in PcapReader(file_to_work_with):
        if count == targetFlowCount: break ### only process certain number of packets
        
        if count%10000 == 0:
            print('read ' + str(count))
            #print(flowstring)
        #trafficCount[str(pkt.payload.src) + str(pkt.payload.sport) + str(pkt.payload.dst) + str(pkt.payload.dport) + str(pkt.payload.proto)] += 1
        if not (hasattr(pkt, 'payload') and hasattr(pkt.payload, 'sport')): continue        
        if not hasattr(pkt.payload, 'proto'): continue
        if not (hasattr(pkt, 'payload') and hasattr(pkt.payload, 'dport')): continue

        mysport, mydport, myproto = pkt.payload.sport, str(pkt.payload.dport), str(pkt.payload.proto)
        flowstring = pkt.payload.src + " - " + str(mysport) + " - " + pkt.payload.dst + " - " + str(mydport) + " - " + str(myproto) 
        # if count%10000 == 0:
        #     print(flowstring)
        csvFileHandle.write(flowstring + "\n")
        count += 1


        #print(flowstring)


if __name__ == '__main__':
    process_file_5min(default_file_to_work_with, 'Dataset.pkl', 60000000)