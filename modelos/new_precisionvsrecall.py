import matplotlib
matplotlib.use('Agg')
   
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import average_precision_score
from sklearn import metrics
   
import matplotlib.pyplot as plt
import pandas as pd
import sys, getopt
   
   
class PrecisionGraphics:

   def __init__(self, inputfile, outputfile):   
   
   
   
      treina=pd.read_csv(inputfile, sep=',',header=0)
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
      strtitulo="Precision vs Recall for "+inputfile
      plt.title(strtitulo)
   #   plt.show()
      #plt.savefig('foo.png')
      plt.savefig(outputfile)
      plt.clf()
#   if __name__ == "__main__":
#      main(sys.argv[1:])          
