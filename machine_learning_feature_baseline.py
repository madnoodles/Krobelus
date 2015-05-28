import csv
from collections import defaultdict
from sklearn.ensemble import ExtraTreesClassifier
from pandas import DataFrame as df
import numpy as np
from sklearn import cross_validation
from sklearn.ensemble import RandomForestClassifier


def csv_to_dic(file):
    list = []
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            list.append(row)
    return list

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

data = csv_to_dic('dataset.csv')

data_fixed = []

for i in data:
    for k in i.keys():
        if is_number(i[k]):
            i[k]=float(i[k])
    i['developer'] = eval(i['developer'])[0]
    i['publisher'] = eval(i['publisher'])[0]
    data_fixed.append(i)

# convert publisher to number
publishers = [i['publisher'] for i in data_fixed]
k=0
publishers_dict = {}
num_pub = {}
for i in publishers:
    k+=1
    publishers_dict[i] = k
    num_pub[k] = i

for i in data_fixed:
    i['publisher'] = publishers_dict[i['publisher']]


d = df(data_fixed)

y1=d['dsct_1st_month_20_pct_plus']
y2=d['dsct_2nd_month_20_pct_plus']
y3=d['dsct_3rd_month_20_pct_plus']
y4=d['dsct_4th_month_20_pct_plus']

X = d.drop(['dsct_propensity','developer','game','pub_date','dsct_1st_month_20_pct_plus', 'dsct_2nd_month_20_pct_plus', 'dsct_3rd_month_20_pct_plus','dsct_4th_month_20_pct_plus'], axis=1)


clf = ExtraTreesClassifier()

X_new = clf.fit(X, y1).transform(X)

clf.feature_importances_ 
importances = clf.feature_importances_
std = np.std([tree.feature_importances_ for tree in clf.estimators_],
             axis=0)

indices = np.argsort(importances)[::-1]

# Print the feature ranking
print("Feature ranking:")

for f in range(10):
    print("%d. %s (%f)" % (f + 1, X.columns.values[indices[f]], importances[indices[f]]))


# compute baseline

clf = ExtraTreesClassifier()
scores = cross_validation.cross_val_score(clf, X, y1, cv=5)
print np.average(scores)


forest = RandomForestClassifier()
scores = cross_validation.cross_val_score(forest, X, y1, cv=5)
print np.average(scores)


from sklearn.naive_bayes import GaussianNB
gnb = GaussianNB()
scores = cross_validation.cross_val_score(gnb, X, y1, cv=5)
print np.average(scores)


