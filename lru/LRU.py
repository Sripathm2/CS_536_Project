import sys
sys.path.insert(0, '../Python_files/')
from scapy.all import *
from collections import defaultdict



default_file_to_work_with = 'ucsb.pcap'


traffic_count_groundtrue = defaultdict(int)
traffic_count_estimated = defaultdict(int)
lru_cache=defaultdict(list)
lru_cache_expandable=defaultdict(list)
totalflow = set()


target_flow_count = 100000## target_flow_count to be changed
percentile = 0.95 ## percentile can change
packet_threshold=1000 ## packet threshold to clear the previous count
cache_size = 10000 ### the lru cache size 



def insertInCacheTable(flowString,pkt_id):
    if(flowString in lru_cache):#flow present so need to replace the packet_id
        values=lru_cache[flowString]
        count,prev_pkt_id=values[0],values[1]
        if((pkt_id-prev_pkt_id) > packet_threshold):
            lru_cache[flowString]=[1,pkt_id]    
        else:
            count=count+1
            lru_cache[flowString]=[count,pkt_id] 
    else:
        if(len(lru_cache)>=cache_size):
            list_=sorted(lru_cache.items(), key=lambda e: e[1][1])
            key=list_[0][0]
            del lru_cache[key]
            lru_cache[flowString]=[1,pkt_id]
           
        else:
            lru_cache[flowString]=[1,pkt_id]


def insertInCacheTableExpandable(flowString,pkt_id): # Expandable cache
    if(flowString in lru_cache_expandable): #flow present so need to replace the packet_id
        values=lru_cache_expandable[flowString]
        count,prev_pkt_id=values[0],values[1]
        if((pkt_id-prev_pkt_id) > packet_threshold):
            lru_cache_expandable[flowString]=[1,pkt_id]
            
        else:
            count=count+1
            lru_cache_expandable[flowString]=[count,pkt_id]
    #flow not present so add the flow    
    else:
        lru_cache_expandable[flowString]=[1,pkt_id]

def calculate_accuracy(cache):
    GroundTrueCount = []
    for flow in traffic_count_groundtrue:
        GroundTrueCount.append([traffic_count_groundtrue[flow], flow])


    EstimatedCount = []
    for key, value in cache.items():
        EstimatedCount.append([value[0], key])

    GroundTrueCount = sorted(GroundTrueCount, key = lambda x: x[0], reverse = True)
    EstimatedCount = sorted(EstimatedCount, key = lambda x: x[0], reverse = True)

    GroundTrueSet = set()
    EstimatedSet = set()


    currcount1 = 0
    percentileCount= math.floor(sum(i[0] for i in GroundTrueCount)* percentile)
    for count, flow in GroundTrueCount: 
        currcount1 += count
        if currcount1 > percentileCount: break
        else: GroundTrueSet.add(flow)
    
    percentileCount2 = math.floor(sum(i[0] for i in EstimatedCount)* percentile)
    currcount2 = 0
    for count, key in EstimatedCount:
        currcount2 += count
        if currcount2 > percentileCount2: break
        else: EstimatedSet.add(key)
    num= len(list(set(GroundTrueSet) & set(EstimatedSet)))
    dem = len(list(GroundTrueSet.union(EstimatedSet)))
    return num/dem




def main(file_to_work_with,max_limit_of_files):
  
    count =0
    packet_id=0
    print('generating counts')
    for pkt in PcapReader(file_to_work_with):
        packet_id=packet_id+1
        
        # if count == target_flow_count: break ### only process certain number of packets

        if count%10000 == 0:
            print('read ' + str(count))

        if not (hasattr(pkt, 'payload') and hasattr(pkt.payload, 'sport')): continue
        if not hasattr(pkt.payload, 'proto'): continue
        if not (hasattr(pkt, 'payload') and hasattr(pkt.payload, 'dport')): continue
        mysport, mydport, myproto = pkt.payload.sport, str(pkt.payload.dport), str(pkt.payload.proto)

        ##flow_id is 5 tuple(sport,dport,proto,src,des)
        flow_id = pkt.payload.src + " - " + str(mysport) + pkt.payload.dst + " - " + str(mydport) + " - " + str(myproto)
        totalflow.add(flow_id)#adding the flow in totalflow
        traffic_count_groundtrue[flow_id] = traffic_count_groundtrue[flow_id] + 1 ### record the ground true count
        
        insertInCacheTable(flow_id,packet_id)### insert flow in lruCache
        insertInCacheTableExpandable(flow_id,packet_id)
        

        # if count%max_limit_of_files == 0 and count != 0:
        #    break
        count += 1

    print("generating accuracy for lru with size contraint")
    accuracy1= calculate_accuracy(lru_cache)
    print("generating accuracy for lru without size contraint")
    accuracy2 = calculate_accuracy(lru_cache_expandable)
    print('accuracy lru size fixed'+str(accuracy1)+'accuracy lru size expandable'+str(accuracy2))



if __name__ == '__main__':
    main(default_file_to_work_with,  60000000)