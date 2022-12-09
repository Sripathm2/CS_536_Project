
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
figureName = functionName + ".png"
falsePositiveFigure = functionName + "_FalsePositive.png"
falseNegativeFigure = functionName + "_FalseNegative.png"


for percentile in percentiles:
    accuracies = []
    hs = []
    for hashsetSize in hashsetSizes:
            
        file = str(hashsetSize) + "_" + str(percentile) + "_" + functionName + ".json"
        f = os.path.join(base_dir, file)
        fhandle = open(f)
        data = json.load(fhandle)

        accuracies.append(data["accuracy"])

        filenamelist = file.split("_")
        print(filenamelist)
        hs.append(float(filenamelist[0]))

    xpoints = np.array(hs)
    ypoints = np.array(accuracies)
    plt.plot(xpoints, ypoints, label=percentile)
    plt.legend(loc="upper left")

    plt.show()
    plt.savefig(figureName)

fhandle.close()
plt.close()
for percentile in percentiles:
    falsePositives = []
    hs = []
    for hashsetSize in hashsetSizes:
            
        file = str(hashsetSize) + "_" + str(percentile) + "_" + functionName + ".json"
        f = os.path.join(base_dir, file)
        fhandle = open(f)
        data = json.load(fhandle)

        falsePositives.append(data["falsePositive"])

        filenamelist = file.split("_")
        print(filenamelist)
        hs.append(float(filenamelist[0]))

    xpoints = np.array(hs)
    ypoints = np.array(falsePositives)
    plt.plot(xpoints, ypoints, label=percentile)
    plt.legend(loc="upper left")

    plt.show()
    plt.savefig(falsePositiveFigure)

fhandle.close()
plt.close()

for percentile in percentiles:
    falseNegatives = []
    hs = []
    for hashsetSize in hashsetSizes:
            
        file = str(hashsetSize) + "_" + str(percentile) + "_" + functionName + ".json"
        f = os.path.join(base_dir, file)
        fhandle = open(f)
        data = json.load(fhandle)

        falseNegatives.append(data["falseNegative"])

        filenamelist = file.split("_")
        print(filenamelist)
        hs.append(float(filenamelist[0]))

    xpoints = np.array(hs)
    ypoints = np.array(falseNegatives)
    plt.plot(xpoints, ypoints, label=percentile)
    plt.legend(loc="upper left")

    plt.show()
    plt.savefig(falseNegativeFigure)

fhandle.close()
plt.close()

            
