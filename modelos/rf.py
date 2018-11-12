import numpy as np
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
   prunning=0
   trees=100
   balanced=1
   scores="scores_preditos.txt"
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:b:t:p:s:",["ifile=","ofile=","balanced=","trees=","prunning=","scores="])
   except getopt.GetoptError:
      print 'test.py -i <trainfile.csv> -o <testfile.csv> -b <0|1> -t <int> -p <0|1> -s <outputscores.txt>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'test.py -i <inputfile> -o <testfile.csv> -b <0|1> -t <int> -p <0|1> -s <outputscores.txt>'
         sys.exit()
      elif opt in ("-i", "--ifile"):
         train = arg
      elif opt in ("-o", "--ofile"):
         test = arg
      elif opt in ("-b", "--balanced"):
         balanced = arg
      elif opt in ("-t", "--trees"):
         trees = int(arg)
      elif opt in ("-p", "--prunning"):
         prunning = int(arg)
      elif opt in ("-s", "--scores"):
         scores= arg


   if prunning == 1 :
      num_nodes=0.01
   else:
      num_nodes=1
   
#   x_traina, y_traina = load_svmlight_file(train)
#   x_testaa, y_testaa = load_svmlight_file(test)
   treina=pd.read_csv(train, sep=',',header=0)
   testa=pd.read_csv(test, sep=',',header=0)
   y_traina=treina['rotulo']
   del treina['rotulo']
   x_traina = treina
   y_testaa = testa['rotulo']
   del testa['rotulo']
   x_testaa = testa
   
   
   x_train=x_traina
   y_train=y_traina
   del (x_traina)

   x_testa=x_testaa
   y_testa=y_testaa
   del (x_testaa)


   x_train2=x_train.fillna(1e6).astype(np.float32)
   x_testa2=x_testa.fillna(1e6).astype(np.float32)
   seed = 1
#   forest = RandomForestClassifier(class_weight={1:10}, n_estimators = 50, n_jobs=25, random_state=seed)
   if balanced==1:
      forest = RandomForestClassifier(class_weight="balanced", n_estimators=trees, n_jobs=-1, random_state=seed, min_samples_leaf=num_nodes)
   else:
      forest = RandomForestClassifier(n_estimators=trees, n_jobs=-1, random_state=seed, min_samples_leaf=num_nodes)

   forest.fit(x_train2, y_train)

   f = open(scores,'w')

   pred=forest.predict(x_testa2)
   print ("Macro-F1: %f" % f1_score(y_testa, pred, average='macro'))
   f.write("Macro-F1: "+str(f1_score(y_testa, pred, average='macro'))+"\n")
   print ("Micro-F1: %f" % f1_score(y_testa, pred, average='micro'))
   f.write("Micro-F1: "+str(f1_score(y_testa, pred, average='micro'))+"\n")

   print ("Precision: %f" % precision_score(y_testa, pred, average='binary'))
   f.write("Precision: "+  str(precision_score(y_testa, pred, average='binary'))+"\n")

   print ("Recall: %f" % recall_score(y_testa, pred, average='binary'))
   f.write("Recall: "+  str(recall_score(y_testa, pred, average='binary'))+"\n")

   print ("f1_binary: %f" % f1_score(y_testa, pred, average='binary'))
   f.write("f1_binary: "+  str(f1_score(y_testa, pred, average='binary'))+"\n")

   pred2=forest.predict_proba(x_testa2)
   c=0

   for p in pred2:
#      print y_testa[c]
      straux="predicted_real"+str(test)+": "+str(p[1])+" "+str(y_testa[c])+" \n"
      f.write(straux )
#      print ("predicted_real%s: %f %f "% (test, p[1], y_testa[c]) )
      c=c+1



if __name__ == "__main__":
   main(sys.argv[1:])


