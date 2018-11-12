# -*- coding: utf-8 -*-

import numpy as np

#from sklearn import ensemble

#from sklearn.ensemble import KNeighborsClassifier, KNeighborsRegressor
from sklearn import neighbors
from sklearn.datasets import load_svmlight_file
from sklearn.externals import joblib
from sklearn.metrics import accuracy_score
from sklearn.metrics import f1_score
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
import pandas as pd
import sys, getopt

from sklearn.externals import joblib


class KNeighborsClassifier:

    def __init__(self, train, test, k, scores, Y_train=None):
        self.train = train
        self.test = test
#        self.n_estimators = n_estimators
#        self.balanced = balanced
        self.scores = scores
        self.seed = 1
        self.k=k
#        self.prunning = prunning
        self.Y_train = Y_train
#        if prunning==1:
#            self.num_nodes = 0.01
#        elif prunning==2:
#            self.num_nodes = 0.001
#        elif prunning==3:
#            self.num_nodes = 5
#        elif prunning==4:
#            self.num_nodes = 10
#        elif prunning==5:
#            self.num_nodes = 100
#        elif prunning==6:
#            self.num_nodes = 1
        

    def preprocess_input_vectors(self):
        trainpd=pd.DataFrame(self.train)
        testpd=pd.DataFrame(self.test)
        self.train = trainpd.fillna(1e6).astype(np.float32)
        self.test = testpd.fillna(1e6).astype(np.float32)
        

    def read_input_files(self):
        treina = pd.read_csv(self.train, sep =',', header = 0)
        testa = pd.read_csv(self.test, sep =',', header = 0)
        
        y_traina = treina['rotulo']
        del treina['rotulo']
        x_traina = treina
        y_testaa = testa['rotulo']
        del testa['rotulo']
        x_testaa = testa
        
        x_train = x_traina
        y_train = y_traina
        del (x_traina)
        
        x_testa = x_testaa
        y_testa = y_testaa
        del (x_testaa)
        
        x_train2 = x_train.fillna(1e6).astype(np.float32)
        x_testa2 = x_testa.fillna(1e6).astype(np.float32)
        
        return x_train2, x_testa2, y_train, y_testa
    
    def model_evaluation(self, pred, y_test):
        """ Salva em arquivo a avaliação da predição.
        """
        print self.scores
#        f = open(self, 'output.txt','w')
        f = open(self.scores,'w')
#        print ("Macro-F1: %f" % f1_score(y_test, pred, average='macro'))
#        f.write("Macro-F1: "+str(f1_score(y_test, pred, average='macro'))+"\n")
        print ("Accuracy: %f" % f1_score(y_test, pred, average='micro'))
        f.write("Accuracy: "+str(f1_score(y_test, pred, average='micro'))+"\n")
        print ("Precision: %f" % precision_score(y_test, pred, average='binary'))
        f.write("Precision: "+  str(precision_score(y_test, pred, average='binary'))+"\n")
         
        print ("Recall: %f" % recall_score(y_test, pred, average='binary'))
        f.write("Recall: "+  str(recall_score(y_test, pred, average='binary'))+"\n")
         
        print ("f1_binary: %f" % f1_score(y_test, pred, average='binary'))
        f.write("f1_binary: "+  str(f1_score(y_test, pred, average='binary'))+"\n")
         
        c = 0 
        for p in pred:
            straux = "predicted_real_"+str(test)+": "+str(p)+" "+str(y_test[c])+" \n"
            f.write(straux )
            c = c+1

    def model_evaluation2(self, pred, predprob, y_test):
        """ Salva em arquivo a avaliação da predição.
        """
#        print self.scores    
        f = open(self.scores,'w')
        acc=f1_score(y_test, pred, average='micro')
        prec=precision_score(y_test, pred, average='binary')
        rec=recall_score(y_test, pred, average='binary')
        f1=f1_score(y_test, pred, average='binary')
        
        c = 0 
        f.write("score,rotulo\n")
        for p in predprob:
            straux = str(p)+","+str(y_test[c])+"\n"
            f.write(straux )
            c = c+1
        return f1,prec,rec,acc

    def build_knn_model(self):
        """ Recebe x e y do treino, e o x do teste.
            Cria o modelo de RF e realiza predição. 
            Retorna a predição.
        """
#        if self.balanced==1:
        forest = neighbors.KNeighborsClassifier(n_neighbors=self.k, n_jobs=-1)
            #forest = ensemble.KNeighborsRegressor(n_estimators = self.n_estimators,random_state = self.seed )
        forest.fit(self.train, self.Y_train)
        pred = forest.predict(self.test)
        predprob = forest.predict_proba(self.test)            
        return pred,predprob[:,1]

                    
