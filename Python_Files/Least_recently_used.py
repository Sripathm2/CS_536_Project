import mmh3
import numpy as np
from tqdm import tqdm
from random import random
from collections import defaultdict

# config
# depth = 4 # number of hash functions
# width = 66000
# percentile = 95
packet_threshold = 50000000


def least_recently_used(depth, width, percentile):
    trafficCountGroundTrue = defaultdict(int)
    cms_heavy_hitters = defaultdict(int)
    
    print('reading lines')
    file = open('/home/mishra60/CS536/CS_536_Project/datasets/merged.out', 'r')
    lines = file.readlines()
    # removing header
    lines = lines[1:]
    #lines = lines[:10000000]
    temp_lines = []
    for line in tqdm(lines):
        temp_lines.append(line[line.index(','):])
    lines = temp_lines
    
    cm = ModifiedCountMinSketch(width, depth, packet_threshold)

    for line in tqdm(lines):
        trafficCountGroundTrue[line] += 1
        cm.increment(line)

    # Ground_truth_percentileCount calc
    flow_counts = np.array(list(trafficCountGroundTrue.values()))
    Ground_truth_percentileCount = np.percentile(flow_counts, percentile)

    GroundTrueSet = set()
    for flow in tqdm(trafficCountGroundTrue.keys()):
        count = trafficCountGroundTrue[flow]
        cms_heavy_hitters[flow] = cm.estimate(flow)
        if count >= Ground_truth_percentileCount:
            GroundTrueSet.add(flow)

    ## Estimated_percentileCount calc
    flow_counts = np.array(list(cms_heavy_hitters.values()))
    Estimated_percentileCount = np.percentile(flow_counts, percentile)
    print(flow_counts)
    print(Estimated_percentileCount)
    
    EstimatedSet = set()
    for flow in tqdm(trafficCountGroundTrue.keys()):
        count = cms_heavy_hitters[flow]
        if count >= Estimated_percentileCount:
            EstimatedSet.add(flow)
    

    print("size of groundTrue set", len(GroundTrueSet))
    print("size of Esitmated set", len(EstimatedSet))
    print("stats ", depth, ' . ', width, ' . ', percentile)
    print("the Accuracy is", len(GroundTrueSet.intersection(EstimatedSet)) / (len(GroundTrueSet)+len(EstimatedSet) -len(EstimatedSet.intersection(GroundTrueSet)) ))


class ModifiedCountMinSketch(object):
    def __init__(self, width, depth, packet_threshold):
        self.width = width
        self.depth = depth
        self.table = np.zeros([depth, width]) 
        self.counter = 0
        self.packet_threshold = packet_threshold
        self.counters = np.zeros([depth, width])

        self.seeds = [int(random()*10000) for x in range(self.depth)]

    def increment(self, key):
        self.counter += 1
        for i in range(0, self.depth):
            index = mmh3.hash(key, self.seeds[i]) % self.width
            if self.counter - self.counters[i, index] < packet_threshold:
                self.table[i, index] += 1
                self.counters[i, index] = self.counter
            else:
                self.table[i, index] = 1
                self.counters[i, index] = self.counter

    def estimate(self, key):
        min_est = self.width
        for i in range(0, self.depth):
            index = mmh3.hash(key, self.seeds[i]) % self.width
            min_est = min(min_est, self.table[i, index])
        return min_est

    def merge(self, new_cms):
        return self.table + new_cms


if __name__ == '__main__':
    hash_number = [2,4,8,16]
    hashset_size = [6000, 10000, 20000, 30000, 65536]
    percentiles = [95, 99]
    for dep in hash_number:
        for wid in hashset_size:
            for per in percentiles:
                least_recently_used(dep,wid,per)