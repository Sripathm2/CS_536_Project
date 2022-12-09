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
for index, row in data.iterrows():
    val = row['src']+"|"+row['dst']+"|"+str(int(row['sport']))+"|"+str(int(row['dport']))+"|"+str(int(row['proto']))
    list_vals.append(val)
    if val in val_hash:
        val_hash[val]=val_hash[val]+1
    else:
        val_hash[val]=1

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

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

#train on less and test on more!
#after parameters are learnt, do the CM hash computation again independently for the test data! (do not use the CM computation from overall)
X_train, X_test, y_train, y_test = train_test_split(X_arr, y_arr, test_size=0.50, random_state=42)
reg = LinearRegression().fit(X_train, y_train)



ml_predicted = []
count_min_predicted = []
actual = []
for i in range(len(X_test)):
    y_hat = reg.predict(X_test[i,:].reshape(-1,depth))
    ml_predicted.append((y_hat,i))
    count_min_predicted.append((np.amin(X_test[i,:]),i))
    actual.append((y_test[i],i))

test_sz = len(X_test)
top_k = k_percentile*test_sz

ml_predicted = sorted(ml_predicted, key=lambda tup: tup[0], reverse=True)
count_min_predicted = sorted(count_min_predicted, key=lambda tup: tup[0], reverse=True)
actual = sorted(actual, key=lambda tup: tup[0])

print(count_min_predicted)
print(actual)

st_actual = set()
st_ml = set()
st_cm = set()
for i in range(test_sz):
    st_actual.add(actual[i][1])
    st_ml.add(ml_predicted[i][1])
    st_cm.add(count_min_predicted[i][1])

    print("ML", len(st_actual.intersection(st_ml))/(i+1), "CM", len(st_actual.intersection(st_cm))/(i+1) )



