import numpy as np
import sys
sys.path.append('.')

from modelos.newrf import RandomForestClassifier
from modelos.new_ndcg_prec_rec import NDCGGraphics
from features.all_datasets.get_ids import GetIds
from modelos.new_precisionvsrecall import PrecisionGraphics

from features.claro_mig.features_iuri import AnalysisFeaturesIuri
from features.claro_mig.features_recarga_claro_iuri_1 import AnalysisFeaturesIuriLag
from features.claro_mig.features_recarga import AnalysisRecharge
from features.claro_mig.features_recarga_lag_vivo import AnalysisRechargeLagVivo
from features.claro_mig.features_recarga_intervalos_temp import AnalysisRechargeTimeIntervals
from features.claro_mig.features_planos_coef_var import AnalysisCoefVar
from features.all_datasets.client_recargaserv import FeaturesServicosdeRecarga
from features.all_datasets.mailing import FeaturesMailing
from features.all_datasets.get_timestamp import AnalysisTimestamp
from features.all_datasets.client_services import FeaturesServicosdoCliente
from features.all_datasets.dia_mes import FeaturesDiaMes
from features.all_datasets.dia_semana import FeaturesDiaSemana
from features.all_datasets.features_clientes import FeaturesClientes
from features.all_datasets.features_ligacoes_clientes import FeaturesLicacoesClientes
from features.all_datasets.fidelidade_prezao import FeatureFidelidadePrezao
from features.all_datasets.hora import FeaturesHora
from features.all_datasets.qtd_prezao import FeaturesQtdPrezao
from features.all_datasets.vivo_idade import FeaturesIdade
from features.all_datasets.vivo_mailing import FeaturesVivoMailing
from features.all_datasets.vivo_recarga import FeaturesVivoRecarga
from features.all_datasets.vivo_status_cliente import FeaturesVivoStatusCliente
from features.all_datasets.whitelist import FeaturesWhitelist
from modelos.newrf import RandomForestClassifier

class ExecuteVivoMig:
	def __init__(self):
		print('Executing Features...')


	def run_features(self, database, npz_type):
		individualfeatures={} #dictionary with individual features.
		
		print('Features Iuri Vivo (recarga_lag_plano)...') #ok features_iuri.py
		try:
			loaded=np.load('{}_features_iuri_vm.npz'.format(npz_type))
			try:
				x = loaded['x_comp']
			except:
				x = loaded['x']
			y = loaded['y']
		except:         
			print("Generating features...")               
			analysis = AnalysisFeaturesIuri(database)
			y, x = analysis.gen_features()
			np.savez_compressed('{}_features_iuri_vm.npz'.format(npz_type), x=x, y=y)
		
		individualfeatures['recargalagplano']=x
		
		print('Features Iuri lag Vivo ...') # ok features_recarga_claro_iuri_1.py
		try:
			loaded = np.load('{}_features_iuri_lag_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:   
			print("Generating features...")                   
			analysis = AnalysisFeaturesIuriLag(database)
			x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_iuri_lag_vm.npz'.format(npz_type), x_comp=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
		individualfeatures['recharge_claro']=x_comp

#		print('Features Recarga...') #NOVAS FEATURES!!!
#		try:
#			loaded = np.load('{}_features_recarga_vm.npz'.format(npz_type))
#			try:
#				x_comp = loaded['x_comp']
#			except:
#				x_comp = loaded['x']			
#			x = np.concatenate([x, x_comp], axis=1)
#		except:
#			print("Generating features...")
#			analysis = AnalysisRecharge(database)
#			x_comp = analysis.gen_features()
#			np.savez_compressed('{}_features_recarga_vm.npz'.format(npz_type), x_comp=x_comp)
#			x = np.concatenate([x, x_comp], axis=1)


		print('Features Recarga com lag, media, desvio (recharge_lagmeddesv) ...')  #ok mundiale_features/features/claro_mig/features_recarga_lag_vivo.py
		try:
			loaded = np.load('{}_features_recarga_lag_vivo_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = AnalysisRechargeLagVivo(database)
			x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_recarga_lag_vivo_vm.npz'.format(npz_type), x_comp=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
		individualfeatures['recharge_lagmeddesv']=x_comp

		print('Features Recarga Intervalos (recharge_interval)...')  #ok features_recarga_intervalos_temp.py
		try:
			loaded = np.load('{}_features_recarga_intervalos_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = AnalysisRechargeTimeIntervals(database)
			x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_recarga_intervalos_vm.npz'.format(npz_type), x_comp=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
		individualfeatures['recharge_interval']=x_comp
		
		print('Features Servicos Recarga (recarga)...') #ok features/all_datasets/client_recargaserv.py
		try:
			loaded = np.load('{}_features_servico_recarga_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except: 
			print("Generating features...")
			analysis = FeaturesServicosdeRecarga(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_servico_recarga_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
                individualfeatures['recarga']=x_comp
                
		print('Features Mailing...')
		try:
			loaded = np.load('{}_features_mailing_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except :
			print("Generating features...")
			analysis = FeaturesMailing(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_mailing_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
                individualfeatures['mailing']=x_comp

		print('Features Servicos clientes...') #ok client_services.py
		try:
			loaded = np.load('{}_features_servico_clientes_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:	
			print("Generating features...")
			analysis = FeaturesServicosdoCliente(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_servico_clientes_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
		x_servico_clientes=x_comp
                individualfeatures['servico_clientes']=x_comp		

		print('Features Dia / Mes...') #ok dia_mes.py
		try:
			loaded = np.load('{}_features_dia_mes_vm.npz'.format(npz_type)) 
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesDiaMes(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_dia_mes_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
                individualfeatures['dia_mes']=x_comp
	
		print('Features Dia / Semana...') #ok dia_semana.py
		try:
			loaded = np.load('{}_features_dia_semana_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:	
			print("Generating features...")
			analysis = FeaturesDiaSemana(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_dia_semana_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
                individualfeatures['dia_semana']=x_comp


		print('Features Clientes (localizacao) ...') #ok ~/mundiale_features/features/all_datasets/features_clientes.py
		try:
			loaded = np.load('{}_features_clientes_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesClientes(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_clientes_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
                individualfeatures['localizacao']=x_comp
		
		print('Features Ligacoes Clientes (qtdcalls)...') #ok features_ligacoes_clientes.py
		try:
			loaded = np.load('{}_features_ligacoes_clientes_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesLicacoesClientes(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_ligacoes_clientes_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
                individualfeatures['qtdcalls']=x_comp

		
		
		print('Features Hora...') #ok hora.py
		try:
			loaded=np.load('{}_features_hora_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesHora(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_hora_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
                individualfeatures['hora']=x_comp
		

                individualfeatures['tudo']=x


		return y, individualfeatures


	def run_timestamp(self, database, npz_type):
		print('getting timestamps...')
		try:
			loaded=np.load('{}_get_timestamp_vm.npz'.format(npz_type))
			try:
				x = loaded['x_comp']
			except:
				x = loaded['x']
		except:         
			print("Generating timestamps...")               
			analysis = AnalysisTimestamp(database)
			x = analysis.gen_features()
			np.savez_compressed('{}_get_timestamp_vm.npz'.format(npz_type), x=x)
		return x
			


	def run_rf(self, outputfile):
		print('Running model...')
		forest = RandomForestClassifier(train=self.x_train, test=self.x_test , prunning=6, balanced=None, n_estimators=50,  scores=outputfile, Y_train=self.y_train)
		forest.preprocess_input_vectors()
		pred,predprob = forest.build_rf_model()
		f1,prec,rec,acc = forest.model_evaluation2(pred, predprob, self.y_test)
#	        print ("f1_binary: %f" % f1)
#	        print ("Accuracy: %f" % acc)
#	        print ("Precision: %f" % prec)
#	        print ("Recall: %f" % rec)

                PrecisionGraphics(outputfile, outputfile+"_precisionvsrecall.pdf")
                NDCGGraphics(outputfile, outputfile+"_NDCG.pdf")


	        return f1,prec,rec,acc 


	def janela_temporal(self,timestamp,y_trainaux,x_trainaux,namefeature):

		new_y=np.concatenate([timestamp[:,np.newaxis],y_trainaux[:,np.newaxis]], axis=1)
		
		yordered=new_y[np.lexsort((new_y[:, 0], ))]
		new_x=np.concatenate((timestamp[:,np.newaxis],x_trainaux), axis=1)
		xordered=new_x[np.lexsort((new_x[:, 0], ))]

		yordered2=np.squeeze(np.delete(yordered, 0, 1))
		xordered2=np.delete(xordered, 0, 1)


		partindices=[]
		auxt=0
		for i in range(6):
			auxt=auxt+xordered2.shape[0]/6

			print auxt
			partindices.append(auxt)
			
		
		partsx=np.split(xordered2,partindices)
		partsy=np.split(yordered2,partindices)


		f1avg=[]
		precavg=[]
		recavg=[]
		accavg=[]
		for i in range(5):
			print ("processando parte "+str(i))			
			self.x_train=partsx[i]
			self.y_train=partsy[i]
			
			self.x_test=partsx[i+1]
			self.y_test=partsy[i+1]
			
			
			f1,prec,rec,acc = self.run_rf("rfscores_part"+str(i)+"_"+namefeature+"_vm.txt")
			f1avg.append(f1)
			precavg.append(prec)
			recavg.append(rec)
			accavg.append(acc)
#		print ("Average of 5 parts:")
#	        print ("f1_binary: %f(%f)" % (np.mean(f1avg),np.std(f1avg)))
#	        print ("Accuracy: %f(%f)" % (np.mean(accavg), np.std(accavg)))
#	        print ("Precision: %f(%f)" % (np.mean(precavg),np.std(precavg)))
#	        print ("Recall: %f(%f)" % (np.mean(recavg),np.std(recavg)))
	        results=[f1avg, precavg, recavg, accavg]
	        return results
		

		
	def run(self):
		timestamp = self.run_timestamp(npz_type='treino', database='files3_vivo_mig')

		y_trainaux,  individual_features= self.run_features(npz_type='treino', database='files3_vivo_mig')

		resultados={}
		for namefeature,x_value in individual_features.items():
			print ("janela temporal para features "+namefeature)			
			resultados[namefeature]=self.janela_temporal(timestamp,y_trainaux,x_value,namefeature)

	


		for namefeature,results in resultados.items():
			print ("----------------------------")					
			print ("Average of 5 parts for "+namefeature+":")
		        print ("f1_binary: %f(%f)" % (np.mean(results[0]),np.std(results[0])))
		        print ("Accuracy: %f(%f)" % (np.mean(results[3]), np.std(results[3])))
		        print ("Precision: %f(%f)" % (np.mean(results[1]),np.std(results[1])))
		        print ("Recall: %f(%f)" % (np.mean(results[2]),np.std(results[2])))
				

		
if __name__ == '__main__':
    claro_mig = ExecuteVivoMig()
    claro_mig.run()
