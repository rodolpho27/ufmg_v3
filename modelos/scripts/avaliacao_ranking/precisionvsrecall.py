from sklearn.metrics import precision_recall_curve
from sklearn.metrics import average_precision_score
from sklearn import metrics

import matplotlib.pyplot as plt
import pandas as pd
import sys, getopt



def main(argv):
   inputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:",["ifile="])
   except getopt.GetoptError:
      print 'test.py -i <treino.svmlight>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'test.py -i <inputfile> '
         sys.exit()
      elif opt in ("-i", "--ifile"):
         train = arg




   treina=pd.read_csv(train, sep=',',header=0)
   y_traina=treina['rotulo']
   y_score=treina['score']


   precision, recall, _ = precision_recall_curve(y_traina, y_score)

   plt.step(recall, precision, color='b', alpha=0.2, where='post')
   plt.fill_between(recall, precision, step='post', alpha=0.2,                 color='b')

   plt.xlabel('Recall')
   plt.ylabel('Precision')
   plt.ylim([0.0, 1.05])	
   plt.xlim([0.0, 1.0])
#   plt.title('2-class Precision-Recall curve: AP={0:0.2f}'.format(   average_precision))
   strtitulo="Precision vs Recall for "+train
   plt.title(strtitulo)
#   plt.show()
   #plt.savefig('foo.png')
   plt.savefig('foo.pdf')
if __name__ == "__main__":
   main(sys.argv[1:])          