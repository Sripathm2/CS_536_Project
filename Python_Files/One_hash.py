import mmh3
import numpy as np
from tqdm import tqdm
from collections import defaultdict


# config
hash_size = 66000 ### the mod size is 10K now; it can be changed later
percentile = 95 ## percentile can change;
random_seed = 12345
file = open('/mnt/ngas/Datasets/ucsb/5tuple.csv', 'r')

trafficCountGroundTrue = defaultdict(int)
trafficCountEstimated = defaultdict(int)


def flowToHashIndex(flowString, hash_size):
    hashKey = mmh3.hash(flowString, random_seed) % hash_size
    return hashKey


def one_hash():
    print('reading lines')
    lines = file.readlines()
    #lines = lines[:10000]

    for line in tqdm(lines):
        trafficCountGroundTrue[line] = trafficCountGroundTrue[line] + 1
        trafficCountEstimated[flowToHashIndex(line,hash_size)] += 1

    ## Ground_truth_percentileCount calc
    flow_counts = np.array(list(trafficCountGroundTrue.values()))
    Ground_truth_percentileCount = np.percentile(flow_counts, percentile)

    GroundTrueSet = set()
    for flow in tqdm(trafficCountGroundTrue.keys()):
        count = trafficCountGroundTrue[flow]
        if count >= Ground_truth_percentileCount:
            GroundTrueSet.add(flow)

    ## Estimated_percentileCount calc
    flow_counts = np.array(list(trafficCountEstimated.values()))
    Estimated_percentileCount = np.percentile(flow_counts, percentile)
    
    EstimatedSet = set()
    for flow in tqdm(trafficCountGroundTrue.keys()):
        key = flowToHashIndex(flow, hash_size)
        count = trafficCountEstimated[key]
        if count >= Estimated_percentileCount:
            EstimatedSet.add(flow)

    
    print("size of groundTrue set", len(GroundTrueSet))
    print("size of Esitmated set", len(EstimatedSet))
    print("the Accuracy is", len(GroundTrueSet.intersection(EstimatedSet)) / (len(GroundTrueSet)+len(EstimatedSet) -len(EstimatedSet.intersection(GroundTrueSet)) ))

if __name__ == '__main__':
    one_hash()