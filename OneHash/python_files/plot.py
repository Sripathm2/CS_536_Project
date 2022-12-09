import json
import os
import matplotlib.pyplot as plt
import numpy as np

base_dir = './'
# Opening JSON file
data_list = []
for file in os.listdir(base_dir):
    x = []
    y = []
    if '.json' in file:
        f = os.path.join(base_dir, file)
        fhandle = open(f)
        #json_data = pd.read_json(json_path, lines=True)
        #data_list.append(json_data)

        data = json.load(fhandle)
        x_y = data["histogram_map"]
        print("line x_y is", len(x_y))
        for i in x_y.keys():
            x.append(int(i))
            y.append(x_y[str(i)])
        
        #print(x)
        #print(y)
        #print(data)
        xpoints = np.array(x)
        ypoints = np.array(y)
        mean = np.average(ypoints)
        print("mean is", mean)
        standard_deviation = np.std(ypoints)
        print("standard_deviation is", standard_deviation)

        print(xpoints)
        print(ypoints)

        min_mapped = 0
        max_mapped = np.max(ypoints)
        range_mapped = max_mapped - min_mapped
        colors = []

        for val in ypoints:
            if val >= min_mapped and val < 0.25 * max_mapped:
                colors.append("yellow")
            elif val >= 0.25 and val < 0.5 * max_mapped:
                colors.append("orange")
            elif val >= 0.5 * max_mapped and val < 0.75 * max_mapped:
                colors.append("pink")
            else:
                colors.append("red")
    
        plt.bar(xpoints, ypoints, color = colors)
        plt.title("Accuracy=" + str(round(data["accuracy"], 2)) + " mean= " +  str(round(round(mean, 2)))  + " std= " +  str(round(standard_deviation, 2)))
        plt.xlabel("hash table key")
        plt.ylabel("flow count")
        plt.legend(["std is", "mean"])
        #plt.legend(["mean is {}".format(mean)])

        plt.show()
        rootname = f.replace(".json", "")
        plt.savefig(rootname + ".png")

