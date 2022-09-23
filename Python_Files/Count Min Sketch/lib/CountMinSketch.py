import numpy as np
from random import random
import mmh3


class CountMinSketch(object):
    ''' Class for a CountMinSketch data structure
    '''

    def __init__(self, width, depth):
        ''' Method to initialize the data structure
        @param width int: Width of the table
        @param depth int: Depth of the table (num of hash func)
        @param seeds list: Random seed list
        '''
        self.width = width
        self.depth = depth
        self.table = np.zeros([depth, width])  # Create empty table
        
        # Random seeds are used to initialize the "width" hash functions so we effectively get different hashes for the same flow
        self.seeds = [int(random()*10000) for x in range(self.depth)]

    def increment(self, key):
        ''' Method to add a key to the CMS
        @param key str: A string to add to the CMS
        '''
        for i in range(0, self.depth):
            index = mmh3.hash(key, self.seeds[i]) % self.width
            self.table[i, index] += 1

    def estimate(self, key):
        ''' Method to estimate if a key is in a CMS
        @param key str: A string to check
        '''
        min_est = self.width
        for i in range(0, self.depth):
            index = mmh3.hash(key, self.seeds[i]) % self.width
            min_est = min(min_est, self.table[i, index])
        return min_est

    def merge(self, new_cms):
        ''' Method to combine two count min sketches
        @param new_cms CountMinSketch: Another CMS object
        '''
        return self.table + new_cms
