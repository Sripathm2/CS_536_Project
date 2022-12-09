import mmh3
import numpy as np
from tqdm import tqdm
from collections import defaultdict
import zlib
import hashlib
import argparse
import json

parser = argparse.ArgumentParser(
                    prog = 'ProgramName',
                    description = 'What the program does',
                    epilog = 'Text at the bottom of help')

#parser.add_argument('filename')           # positional argument
parser.add_argument('-hset', '--hashset')      # option that takes a value
parser.add_argument('-p', '--percentile')      # option that takes a value
parser.add_argument('-func', '--function')  # on/off flag

args = parser.parse_args()
print(args.hashset, args.percentile, args.function)


# config
hash_size = int(args.hashset) ### the mod size is 10K now; it can be changed later
percentile = int(args.percentile) ## percentile can change;
inputhashfunction = args.function
file = open('5tuple.csv', 'r')

trafficCountGroundTrue = defaultdict(int)
trafficCountEstimated = defaultdict(int)
hashKeyToFlow = defaultdict(set)
uniqueTotalSet = set()

def flowToHashIndex(flowString, hash_size):
    if inputhashfunction == "sha512":
        newhash = hashlib.sha512(flowString.encode())
        hexhash = newhash.hexdigest()
        hashKey = int(hexhash, 16) % hash_size
    elif inputhashfunction == "md5":
        newhash = hashlib.md5(flowString.encode())
        hexhash = newhash.hexdigest()
        hashKey = int(hexhash, 16) % hash_size
    elif inputhashfunction == "crc32":
        hashKey = zlib.crc32(flowString.encode()) % hash_size

    return hashKey


def one_hash():
    print('reading lines')
    lines = file.readlines()
    #lines = lines[:10000]

    for line in tqdm(lines):
        #print("line is ", line)
        trafficCountGroundTrue[line] = trafficCountGroundTrue[line] + 1
        #print("hash key number is ", flowToHashIndex(line,hash_size))
        trafficCountEstimated[flowToHashIndex(line,hash_size)] += 1

    ## Ground_truth_percentileCount calc
    #print("list(trafficCountGroundTrue.values() is ", list(trafficCountGroundTrue.values()))
    flow_counts = np.array(list(trafficCountGroundTrue.values()))
    Ground_truth_percentileCount = np.percentile(flow_counts, percentile)

    GroundTrueSet = set()
    for flow in tqdm(trafficCountGroundTrue.keys()):
        count = trafficCountGroundTrue[flow]
        uniqueTotalSet.add(flow)
        if count >= Ground_truth_percentileCount:
            GroundTrueSet.add(flow)

    ## Estimated_percentileCount calc
    flow_counts = np.array(list(trafficCountEstimated.values()))
    Estimated_percentileCount = np.percentile(flow_counts, percentile)
    

    EstimatedSet = set()
    for flow in tqdm(trafficCountGroundTrue.keys()):
        key = flowToHashIndex(flow, hash_size)
        count = trafficCountEstimated[key]
        hashKeyToFlow[key].add(flow)
        if count >= Estimated_percentileCount:
            EstimatedSet.add(flow)

    
    accuracy = len(GroundTrueSet.intersection(EstimatedSet)) / (len(GroundTrueSet)+len(EstimatedSet) -len(EstimatedSet.intersection(GroundTrueSet)) )
    falsePositive = (len(EstimatedSet) - len(GroundTrueSet.intersection(EstimatedSet))) / (len(GroundTrueSet)+len(EstimatedSet) -len(EstimatedSet.intersection(GroundTrueSet)) )
    falseNegative = (len(GroundTrueSet) - len(GroundTrueSet.intersection(EstimatedSet))) / (len(GroundTrueSet)+len(EstimatedSet) -len(EstimatedSet.intersection(GroundTrueSet)) )

    print("total number of unique set is", len(uniqueTotalSet))
    print("size of groundTrue set", len(GroundTrueSet))
    print("size of Esitmated set", len(EstimatedSet))
    print("the Accuracy is", accuracy)


    outfile = str(hash_size) + "_" + str(percentile) + "_" + args.function + ".json" ## ./modsize_percentile_function.json

    filehandle = open(outfile, "w")

    histogram_map = {}

    for i in range(hash_size):
        histogram_map[i] = len(hashKeyToFlow[i])
        #(hashKeyToFlow[i])
    
    dictionary = {
    "accuracy": accuracy,
    "thresholdGroundTrue": Ground_truth_percentileCount,
    "thresholdEstimated": Estimated_percentileCount, 
    "GroundTrueSetCount": len(GroundTrueSet),
    "EstimatedSetCount": len(EstimatedSet),
    "falsePositive": falsePositive,
    "falseNegative": falseNegative,
    "histogram_map": histogram_map
    }


    json.dump(dictionary, filehandle)




if __name__ == '__main__':
    one_hash()