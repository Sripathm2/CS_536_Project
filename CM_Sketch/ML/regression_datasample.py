from random import random
import pandas as pd
from lib.CountMinSketch import CountMinSketch
import numpy as np

#for a given memory find the best depth
#for a width that is very small for CM, lets see if ML performs any good
width = 5000
depth = 10

k_percentile = 0.1

total = 100000

top_k = total*k_percentile

data = pd.read_pickle('dataset100K.pkl')


print("pickle read done")

list_vals = []
val_hash = {}

list_vals_t = []
val_hash_t = {}

test_frac = 0.4

for index, row in data.iterrows():
    val = row['src']+"|"+row['dst']+"|"+str(int(row['sport']))+"|"+str(int(row['dport']))+"|"+str(int(row['proto']))

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
for val in list_vals:
    cm.increment(val)
    cur_set.add(val)

X = []
y = []
for key in cur_set:
    X.append(cm.estimate_all(key))
    y.append(val_hash[key])

X_arr = np.array(X)
y_arr = np.array(y)

from sklearn.linear_model import LinearRegression
reg = LinearRegression().fit(X_arr, y_arr)


cm_t = CountMinSketch(width, depth)
cur_set_t = set()
for val in list_vals_t:
    cm_t.increment(val)
    cur_set_t.add(val)

X_t = []
y_t = []
for key in cur_set_t:
    X_t.append(cm_t.estimate_all(key))
    y_t.append(val_hash_t[key])

X_arr_t = np.array(X_t)
y_arr_t = np.array(y_t)


ml_predicted_t = []
count_min_predicted_t = []
actual_t = []
for i in range(len(X_arr_t)):
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

for i in range(len(X_arr_t)):
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