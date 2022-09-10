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

#default_file_to_work_with = '../Dataset_files/10thousand_packets.pcap'
#default_file_to_work_with = '../Dataset_files/old_10thousand_packets.pcap'
#default_file_to_work_with = '../Dataset_files/million_packets.pcap'
#default_file_to_work_with = '../Dataset_files/5million_packets.pcap'
#default_file_to_work_with = '../Dataset_files/10million_packets.pcap'
#default_file_to_work_with = '../Dataset_files/15million_packets.pcap'
#default_file_to_work_with = '../Dataset_files/equinix-nyc.dirA.20190117-125910.UTC.anon.pcap.gz'
default_file_to_work_with = '../Dataset_files/5m_01_28_22.pcap'


def process_file_5min(file_to_work_with, output_file, max_limit_of_files):
    dataset_list = []
    count =0
    ipv6_handling = {}
    ip_counter = 0
    print(file_to_work_with)
    for pkt in PcapReader(file_to_work_with):
        temp_dict = {'proto':'-1','src':'-1','dst':'-1','sport':'-1','dport':'-1',
            'ip_len':'-1','ip_id':'-1','tcp_ack':'-1',
            'tcp_data_offset':'-1','ip_flags':'-1','tcp_flags':'-1',
            'ip_frag':'-1','ip_tos':'-1','ip_ihl':'-1',
            'ip_ttl':'-1','tcp_window':'-1','tcp_urgptr':'-1'}
        combined = ''
        if count%10000 == 0:
            print('read ' + str(count))
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
            temp_dict['sport_bin'] = BitArray(uint=int(str(int(pkt.payload.sport))[0:5]), length=16).bin
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
    print(df)
    
if __name__ == '__main__':
    process_file_5min(default_file_to_work_with, 'Dataset.pkl', 60000000)