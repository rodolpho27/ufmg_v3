import numpy as np
import sys
sys.path.append('.')
import matplotlib.pyplot as plt
from sklearn.neural_network import MLPClassifier
from modelos.newrf import RandomForestClassifier
from modelos.new_ndcg_prec_rec import NDCGGraphics
from features.all_datasets.get_ids import GetIds
from modelos.new_precisionvsrecall import PrecisionGraphics
from sklearn.externals import joblib
from features.claro_mig.features_historic import AnalysisHistoric
from features.claro_mig.features_iuri import AnalysisFeaturesIuri
from features.claro_mig.features_recarga_claro_iuri_1 import AnalysisFeaturesIuriLag
from features.claro_mig.features_recarga import AnalysisRecharge
from features.claro_mig.features_recarga_lag_vivo import AnalysisRechargeLagVivo
from features.claro_mig.features_recarga_intervalos_temp import AnalysisRechargeTimeIntervals
from features.claro_mig.features_planos_coef_var import AnalysisCoefVar
from sklearn.metrics import accuracy_score, precision_score, classification_report, recall_score
from features.all_datasets.label import FeaturesLabel

from features.all_datasets.client_recargaserv import FeaturesServicosdeRecarga
from features.all_datasets.mailing import FeaturesMailing
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
from sklearn.preprocessing import Imputer

class ExecuteClaroMig:
	def __init__(self):
		print('Executing Features...')


	def run_features(self, database, npz_type, useplans=False,usehistoric=False):
		print('Labels...')
		try:
			loaded = np.load('{}_labels.npz'.format(npz_type))
			y = loaded['y']
		except:
			print("Generating features...")
			analysis = FeaturesLabel(database)
			y = analysis.gen_features()
			np.savez_compressed('{}_labels.npz'.format(npz_type), y=y)

		s = y.shape[0]
		x = y.reshape((s, 1))
		x = x * 0
	#		print y

		print('Features Iuri lag Claro...')

		try:
			loaded = np.load('{}_features_iuri_lag.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:   
			print("Generating features...")                   
			analysis = AnalysisFeaturesIuriLag(database)
			x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_iuri_lag.npz'.format(npz_type), x_comp=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		''' Essas features pioram os resultados
                print('Features Recarga com lag, media, desvio (recharge_lagmeddesv) ...')  
                try:
                        loaded = np.load('{}_features_recarga_lag_vivo.npz'.format(npz_type))
                        try:
                                x_comp = loaded['x_comp']
                        except:
                                x_comp = loaded['x']                    
                        x = np.concatenate([x, x_comp], axis=1)
                except:
                        print("Generating features...")
                        analysis = AnalysisRechargeLagVivo(database)
                        x_comp = analysis.gen_features()
                        np.savez_compressed('{}_features_recarga_lag_vivo.npz'.format(npz_type), x_comp=x_comp)
                        x = np.concatenate([x, x_comp], axis=1)
		'''

		print('Features Recarga...')
		try:
			loaded = np.load('{}_features_recarga.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = AnalysisRecharge(database)
			x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_recarga.npz'.format(npz_type), x_comp=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
	

		print('Features Recarga Intervalos...')
		try:
			loaded = np.load('{}_features_recarga_intervalos.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = AnalysisRechargeTimeIntervals(database)
			x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_recarga_intervalos.npz'.format(npz_type), x_comp=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		print('Features Servicos Recarga...')
		try:
			loaded = np.load('{}_features_servico_recarga.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except: 
			print("Generating features...")
			analysis = FeaturesServicosdeRecarga(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_servico_recarga.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		print('Features Mailing...')
		try:
			loaded = np.load('{}_features_mailing.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except :
			print("Generating features...")
			analysis = FeaturesMailing(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_mailing.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		print('Features Servicos clientes...')
		try:
			loaded = np.load('{}_features_servico_clientes.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:	
			print("Generating features...")
			analysis = FeaturesServicosdoCliente(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_servico_clientes.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		'''
		print('Features Dia / Mes...')
		try:
			loaded = np.load('{}_features_dia_mes.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesDiaMes(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_dia_mes.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		print('Features Dia / Semana...')
		try:
			loaded = np.load('{}_features_dia_semana.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:	
			print("Generating features...")
			analysis = FeaturesDiaSemana(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_dia_semana.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		'''
		print('Features Clientes...')
		try:
			loaded = np.load('{}_features_clientes.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesClientes(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_clientes.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		print('Features Ligacoes Clientes...')
		try:
			loaded = np.load('{}_features_ligacoes_clientes.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesLicacoesClientes(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_ligacoes_clientes.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		print('Features Fidelidade Prezao...')
		try:
			loaded = np.load('{}_features_fidelidade_prezao.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:	
			print("Generating features...")
			analysis = FeatureFidelidadePrezao(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_fidelidade_prezao.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		'''
		print('Features Hora...')
		try:
			loaded=np.load('{}_features_hora.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesHora(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_hora.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		'''
		print('Features Quantidade Prezao...')
		try:
			loaded=np.load('{}_features_qtd_prezao.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesQtdPrezao(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_qtd_prezao.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		print('Features Whitelist...')
		try:
			loaded = np.load('{}_features_whitelist.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			

			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesWhitelist(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_whitelist.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)
			
			
		if usehistoric==True:
			print('Features Historic...')
			try:
				loaded = np.load('{}_features_historic.npz'.format(npz_type))
				try:
					x_comp = loaded['x_comp']
				except:
					x_comp = loaded['x']			

				x = np.concatenate([x, x_comp], axis=1)
			except:
				print("Generating features...")
				analysis = AnalysisHistoric(database)
				x_comp = analysis.gen_features()
				np.savez_compressed('{}_features_historic.npz'.format(npz_type), x=x_comp)
				x = np.concatenate([x, x_comp], axis=1)
                
		if useplans==True:
			print('Features Coef Var...')
			try:
				loaded = np.load('{}_features_coef_var.npz'.format(npz_type))
				try:
					x_comp = loaded['x_comp']
				except:
					x_comp = loaded['x']			

				x = np.concatenate([x, x_comp], axis=1)
			except:
				print("Generating features...")
				analysis = AnalysisCoefVar(database, 'files3_claro_mig')
				x_comp = analysis.gen_features()
				np.savez_compressed('{}_features_coef_var.npz'.format(npz_type), x=x_comp)
				x = np.concatenate([x, x_comp], axis=1)
			
		
		
		return y, x
	
	def run_rf(self, prunning=5, balanced='balanced', imputer=False):

		scores='_rf_scores.txt'
		if(imputer):
			imp = Imputer(missing_values=-1, strategy='median').fit(self.x_train)
			self.x_train = imp.transform(self.x_train)
			self.x_test = imp.transform(self.x_test)
		print('Running model...')
		forest = RandomForestClassifier(train=self.x_train, test=self.x_test , prunning=prunning, n_estimators=100, balanced=balanced, scores=str(prunning)+'_'+str(balanced)+'_'+str(imputer)+scores, Y_train=self.y_train)


		index_1 = np.where(self.y_train == 1)
		data_y_balanced_1 = self.y_train[index_1]
		index_0 = np.where(self.y_train == 0)
		data_y_balanced_0 = self.y_train[index_0[0]]  # [0:30*len(data_y_balanced_1[:])]]
		data_y_balanced = np.array(np.hstack([data_y_balanced_0, data_y_balanced_1]))

		data_x_balanced_1 = self.x_train[index_1]
		data_x_balanced_0 = self.x_train[index_0[0]]  # [0:30*len(data_x_balanced_1[:])]]
		data_x_balanced = np.array(np.vstack([data_x_balanced_0, data_x_balanced_1]))
		del data_x_balanced_0

		clf = MLPClassifier(solver='sgd', alpha=1e-5, hidden_layer_sizes=(50, 100, 50), max_iter=50, random_state=1,
							verbose=1)

		filename = 'clf_model_tT4.sav'
		try:
			clf = joblib.load(filename)
		except:
			clf.fit(np.nan_to_num(data_x_balanced), data_y_balanced)
			# save the model to disk
			joblib.dump(clf, filename)
		pred = clf.predict(np.nan_to_num(self.x_test))
		print(classification_report(self.y_test[:len(pred)], pred))

		#predprob = clf.predict_proba(np.nan_to_num(self.x_test))[:, 1]
		forest.preprocess_input_vectors()
		pred,predprob = forest.build_rf_model()
		# forest.model_evaluation(prediction, self.y_test)
		f1,prec,rec,acc = forest.model_evaluation2(pred, predprob,self.y_test)
	        print ("f1_binary: %f" % f1)
	        print ("Accuracy: %f" % acc)
	        print ("Precision: %f" % prec)
	        print ("Recall: %f" % rec)
		plt.hist(predprob)

		plt.title("Histogram")
		plt.xlabel("Value")
		plt.ylabel("Frequency")
		plt.show()


                PrecisionGraphics(str(prunning)+'_'+str(balanced)+'_'+str(imputer)+scores, str(prunning)+'_'+str(balanced)+'_'+str(imputer)+"_precisionvsrecall.pdf")
                NDCGGraphics(str(prunning)+'_'+str(balanced)+'_'+str(imputer)+scores, str(prunning)+'_'+str(balanced)+'_'+str(imputer)+"_NDCG.pdf")
                ids=GetIds("files3_claro_mig_model_2")
                ids.gen_features(str(prunning)+'_'+str(balanced)+'_'+str(imputer)+"_ids.txt")


		
	def run(self):
		self.y_train, self.x_train = self.run_features(npz_type='treino', database='files3_claro_mig_train', useplans=False,usehistoric=False)
		self.y_test, self.x_test = self.run_features(npz_type='teste_2', database='files3_claro_mig_model_2', useplans=False,usehistoric=False)

		print("Best Configuration for Ranking:")
		self.run_rf(5, None)

#		print("Best Configuration for Classification")
#		self.run_rf(5, 'balanced')


		
if __name__ == '__main__':
    claro_mig = ExecuteClaroMig()
    claro_mig.run()






