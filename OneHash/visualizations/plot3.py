import json
import os
import matplotlib.pyplot as plt
import numpy as np

figureName = "crc_95_99_"
x = [6000, 10000, 20000, 30000, 65536]
ys= [[0.0080207,0.0113366,0.0170809,0.0233742,0.0441515], [0.0101236, 0.0111754, 0.0169436, 0.0230751, 0.0432372],[0.0090566, 0.0119301, 0.0182756, 0.0230494, 0.0439721]] ### 99 percentile
#ys = [[0.0290655, 0.0322217, 0.0384371, 0.0439218, 0.0665036], [0.0303323, 0.0320873, 0.0384638, 0.0430272, 0.0667456], [0.0293286, 0.0325112, 0.0386539, 0.0447189, 0.0660873]] ### 95 percentile

crc = [[0.0101236, 0.0111754, 0.0169436, 0.0230751, 0.0432372], [0.0303323, 0.0320873, 0.0384638, 0.0430272, 0.0667456]]
labels = ["sha512", "crc32", "md5"]
#y = 
percentile = ["95 Percentile", "99 Percentile"]

for idx, y in enumerate(crc):
    xpoints = np.array(x)
    ypoints = np.array(y)
    plt.plot(xpoints, ypoints, label=percentile[idx])
    plt.legend(loc="upper left")
    plt.xlabel("Hash Table Size", fontsize=20)
    plt.ylabel("Accuracy",fontsize=18)
    #plt.xticks(fontsize=48)
    #plt.yticks(fontsize=32)
    ax = plt.gca()
    ax.tick_params(axis='both', which='major', labelsize=10)
    plt.show()
    plt.savefig(figureName)
