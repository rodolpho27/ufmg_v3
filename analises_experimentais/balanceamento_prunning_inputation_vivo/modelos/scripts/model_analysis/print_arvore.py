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


#from sklearn.datasets import load_iris
from sklearn import tree


import sys, getopt

def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:",["ifile="])
   except getopt.GetoptError:
      print 'test.py -i <treino.csv> '
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'test.py -i <inputfile> '
         sys.exit()
      elif opt in ("-i", "--ifile"):
         train = arg

   header=pd.read_csv(train, nrows=1).columns
   header2=header[1:]

#   for a in header:
      
   treina=pd.read_csv(train, sep=',',header=0)
#   testa=pd.read_csv(test, sep=',',header=0)
   y_traina=treina['rotulo']
   del treina['rotulo']
   x_traina = treina

   x_train=x_traina
   y_train=y_traina
   del (x_traina)

   x_train2=x_train
   seed = 1

#>>>
   clf = tree.DecisionTreeClassifier(class_weight="balanced",criterion='entropy',max_leaf_nodes=10)
#>>>
   clf = clf.fit(x_train2,y_train)
   tree.export_graphviz(clf, out_file=test,feature_names=header2, proportion=True, )
   print "Figura da arvore gerada! em tree.dot"
   print "comandos para converter para outros formatos de imagem:"
   print "dot -Tpng tree.dot -o tree.png"
   print "dot -Tps tree.dot -o tree.ps"
   
if __name__ == "__main__":
   main(sys.argv[1:])

