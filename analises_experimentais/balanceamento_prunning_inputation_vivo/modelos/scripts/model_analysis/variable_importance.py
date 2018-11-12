import numpy as np
#import csv

#from cudatree import load_data, RandomForestClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import load_svmlight_file
from sklearn.externals import joblib
from sklearn.metrics import accuracy_score
from sklearn.metrics import f1_score
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
import pandas as pd




#x_train, y_train = load_data("digits")

import sys, getopt

def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:",["ifile="])
   except getopt.GetoptError:
      print 'test.py -i <treino.csv>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'test.py -i <treino.csv>'
         sys.exit()
      elif opt in ("-i", "--ifile"):
         train = arg


   header=pd.read_csv(train, nrows=1).columns
   header2=header[1:]

   treina=pd.read_csv(train, sep=',',header=0)
   y_traina=treina['rotulo']
   del treina['rotulo']
   x_traina = treina
   
   x_train=x_traina
   y_train=y_traina
   del (x_traina)


   x_train2=x_train.fillna(1e6).astype(np.float32)
   x_testa2=x_testa.fillna(1e6).astype(np.float32)
   seed = 1
   forest = RandomForestClassifier(class_weight="balanced", n_estimators = 100, n_jobs=-1, random_state=seed)
   forest.fit(x_train2, y_train)

   importances = forest.feature_importances_
   std = np.std([tree.feature_importances_ for tree in forest.estimators_],
                axis=0)
   indices = np.argsort(importances)[::-1]

# Print the feature ranking
   print("Feature ranking:")

   for f in range(x_train.shape[1]):
       print("%d. feature %s (%f)" % (f + 1, header2[indices[f]], importances[indices[f]]))


if __name__ == "__main__":
   main(sys.argv[1:])

