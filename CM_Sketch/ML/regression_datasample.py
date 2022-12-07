from random import random
import pandas as pd
from lib.CountMinSketch import CountMinSketch
import numpy as np
from tqdm import tqdm

#for a given memory find the best depth
#for a width that is very small for CM, lets see if ML performs any good
width = 65536
depth = 4

k_percentile = 0.5

total = 100000

top_k = total*k_percentile

data = pd.read_csv('/home/mishra60/CS536/CS_536_Project/datasets/merged.out')

print("pickle read done")

print(data)

list_vals = []
val_hash = {}

list_vals_t = []
val_hash_t = {}

test_frac = 0.8

data['src'] = data['src'].apply(str)
data['dst'] = data['dst'].apply(str)
data['sport'] = data['sport'].apply(str)
data['dport'] = data['dport'].apply(str)
data['proto'] = data['proto'].apply(str)



data['5tup'] = data['src']+"|"+data['dst']+"|"+data['sport']+"|"+data['dport']+"|"+data['proto']+"|"
rows = data['5tup'].tolist()
rows = rows[:10000000]
for row in tqdm(rows):
    val = row

    if random() > test_frac:
        list_vals.append(val)
        if val in val_hash:
            val_hash[val]=val_hash[val]+1
        else:
            val_hash[val]=1
    else:
        list_vals_t.append(val)
        if val in val_hash_t:
            val_hash_t[val]=val_hash_t[val]+1
        else:
            val_hash_t[val]=1
    

cm = CountMinSketch(width, depth)
cur_set = set()
for val in tqdm(list_vals):
    cm.increment(val)
    cur_set.add(val)

X = []
y = []
for key in tqdm(cur_set):
    X.append(cm.estimate_all(key))
    y.append(val_hash[key])

X_arr = np.array(X)
y_arr = np.array(y)

from sklearn.linear_model import LinearRegression
reg = LinearRegression().fit(X_arr, y_arr)


cm_t = CountMinSketch(width, depth)
cur_set_t = set()
for val in tqdm(list_vals_t):
    cm_t.increment(val)
    cur_set_t.add(val)

X_t = []
y_t = []
for key in tqdm(cur_set_t):
    X_t.append(cm_t.estimate_all(key))
    y_t.append(val_hash_t[key])

X_arr_t = np.array(X_t)
y_arr_t = np.array(y_t)


ml_predicted_t = []
count_min_predicted_t = []
actual_t = []
for i in tqdm(range(len(X_arr_t))):
    y_hat = reg.predict(X_arr_t[i,:].reshape(-1,depth))
    ml_predicted_t.append((y_hat,i))
    count_min_predicted_t.append((np.amin(X_arr_t[i,:]),i))
    actual_t.append((y_arr_t[i],i))

ml_predicted_t = sorted(ml_predicted_t, key=lambda tup: tup[0], reverse=True)
count_min_predicted_t = sorted(count_min_predicted_t, key=lambda tup: tup[0], reverse=True)
actual_t = sorted(actual_t, key=lambda tup: tup[0])


st_actual_t = set()
st_ml_t = set()
st_cm_t = set()

X = []
Yml = []
Ycm = []

for i in tqdm(range(len(X_arr_t))):
    X.append(i+1)
    st_actual_t.add(actual_t[i][1])
    st_ml_t.add(ml_predicted_t[i][1])
    st_cm_t.add(count_min_predicted_t[i][1])
    Yml.append(len(st_actual_t.intersection(st_ml_t))/(i+1))
    Ycm.append(len(st_actual_t.intersection(st_cm_t))/(i+1))
    #print("ML", len(st_actual_t.intersection(st_ml_t))/(i+1), "CM", len(st_actual_t.intersection(st_cm_t))/(i+1) )


import matplotlib.pyplot as plt

plt.plot(X, Yml, "-b", label="ML")
plt.plot(X, Ycm, "-r", label="CM")
plt.legend(loc="upper left")
plt.show()
plt.savefig('results.png')