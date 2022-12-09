

import json
import os
import matplotlib.pyplot as plt
import numpy as np

startPercentile = 0.75
endPercentile = 0.95
step = 0.05
percentiles = [75, 80, 85, 90, 95]
hashsetSizes = [6000, 8000, 10000, 12000, 14000, 16000, 18000, 20000]
base_dir = "./crc32/"
functionName = "crc32"

csvFile = ""

for percentile in percentiles:

    csvFile = str(percentile) + "_" + functionName + ".csv"
    csvFileHandle = open(csvFile, "w")
    for hashsetSize in hashsetSizes:

        file = str(hashsetSize) + "_" + str(percentile) + "_" + functionName + ".json"
        f = os.path.join(base_dir, file)
        fhandle = open(f)
        data = json.load(fhandle)

        accuracy = data["accuracy"]

        x_y = data["histogram_map"]
        x, y = [], []
        print("line x_y is", len(x_y))
        for i in x_y.keys():
            x.append(int(i))
            y.append(x_y[str(i)])

        xpoints = np.array(x)
        ypoints = np.array(y)
        mean = np.average(ypoints)
        standard_deviation = np.std(ypoints)
        line = str(hashsetSize) + "," + str(mean) + "," + str(standard_deviation) + "," + str(accuracy)
        csvFileHandle.write(line + "\n")

    
    csvFileHandle.close()



    

