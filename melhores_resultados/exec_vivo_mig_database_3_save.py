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
from features.vivo_mig.features_planos_coef_var import AnalysisCoefVar
from features.claro_mig.features_historic import AnalysisHistoric
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



	def run_features(self, database, npz_type, useplans=True,usehistoric=True):
		print('Features Iuri Claro (recarga_lag_plano)...') #OK#ok features_iuri.py
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
			
		'''features da claro migracao!
#		print('Features Iuri lag Claro (recharge_claro)...') # ok features_recarga_claro_iuri_1.py
#		try:
#			loaded = np.load('../{}_features_iuri_lag_vm.npz'.format(npz_type))
#			try:
#				x_comp = loaded['x_comp']
#			except:
#				x_comp = loaded['x']			
#			x = np.concatenate([x, x_comp], axis=1)
#		except:   
#			print("Generating features...")                   
#			analysis = AnalysisFeaturesIuriLag(database)
#			x_comp = analysis.gen_features()
#			np.savez_compressed('../{}_features_iuri_lag_vm.npz'.format(npz_type), x_comp=x_comp)
#			x = np.concatenate([x, x_comp], axis=1)


#		print('Features Recarga...') #NOVAS FEATURES!!!
#		try:
#			loaded = np.load('../{}_features_recarga_vm.npz'.format(npz_type))
#			try:
#				x_comp = loaded['x_comp']
#			except:
#				x_comp = loaded['x']			
#			x = np.concatenate([x, x_comp], axis=1)
#		except:
#			print("Generating features...")
#			analysis = AnalysisRecharge(database)
#			x_comp = analysis.gen_features()
#			np.savez_compressed('../{}_features_recarga_vm.npz'.format(npz_type), x_comp=x_comp)
#			x = np.concatenate([x, x_comp], axis=1)

#		print('Features COf. VAR (plans2)...') features_planos_coef_var.py 
#		try:
#			loaded = np.load('../{}_features_planos_coef_var_vm.npz'.format(npz_type))
#			try:
#				x_comp = loaded['x_comp']
#			except:
#				x_comp = loaded['x']			
#			x = np.concatenate([x, x_comp], axis=1)
#		except:
#			print("Generating features...")
#			analysis = AnalysisCoefVar(database)
#			x_comp = analysis.gen_features()
#			np.savez_compressed('../{}_features_planos_coef_var_vm.npz'.format(npz_type), x_comp=x_comp)
#			x = np.concatenate([x, x_comp], axis=1)

#		print('Features Servicos clientes...') #ok client_services.py
#		try:
#			loaded = np.load('{}_features_servico_clientes_vm.npz'.format(npz_type))
#			try:
#				x_comp = loaded['x_comp']
#			except:
#				x_comp = loaded['x']			
#			x = np.concatenate([x, x_comp], axis=1)
#		except:	
#			print("Generating features...")
#			analysis = FeaturesServicosdoCliente(database)
#			y_comp, x_comp = analysis.gen_features()
#			np.savez_compressed('{}_features_servico_clientes_vm.npz'.format(npz_type), x=x_comp)
#			x = np.concatenate([x, x_comp], axis=1)

		'''
		print('Features Recarga com lag, media, desvio (recharge_lagmeddesv) ...') #OK #ok mundiale_features/features/claro_mig/features_recarga_lag_vivo.py

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


		print('Features Recarga Intervalos (recharge_interval)...')  #OK#ok features_recarga_intervalos_temp.py
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

		print('Features Servicos Recarga (recarga)...') #OK#ok features/all_datasets/client_recargaserv.py
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

		print('Features Mailing...') #OK features/all_datasets/mailing.py 
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


		print('Features Dia / Mes...') #OK#ok dia_mes.py
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

		print('Features Dia / Semana...') #OK#ok dia_semana.py
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

		print('Features Clientes (localizacao) ...') #OK#ok ~/mundiale_features/features/all_datasets/features_clientes.py
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

		print('Features Ligacoes Clientes (qtdcalls)...') #OK#ok features_ligacoes_clientes.py
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


		print('Features Hora...') #OK#ok hora.py
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



		print('Features Idade (idade) ...') #OK  ~/mundiale_features/features/all_datasets/vivo_idade.py
		try:
			loaded=np.load('{}_features_vivo_idade_vm.npz'.format(npz_type))
			try:
				x_comp = loaded['x_comp']
			except:
				x_comp = loaded['x']			
			x = np.concatenate([x, x_comp], axis=1)
		except:
			print("Generating features...")
			analysis = FeaturesIdade(database)
			y_comp, x_comp = analysis.gen_features()
			np.savez_compressed('{}_features_vivo_idade_vm.npz'.format(npz_type), x=x_comp)
			x = np.concatenate([x, x_comp], axis=1)

		if usehistoric==True:
			print('Features Historic...')
			try:
				loaded = np.load('{}_features_historic_vm.npz'.format(npz_type))
				try:
					x_comp = loaded['x_comp']
				except:
					x_comp = loaded['x']			

				x = np.concatenate([x, x_comp], axis=1)
			except:
				print("Generating features...")
				analysis = AnalysisHistoric(database)
				x_comp = analysis.gen_features()
				np.savez_compressed('{}_features_historic_vm.npz'.format(npz_type), x=x_comp)
				x = np.concatenate([x, x_comp], axis=1)

		if useplans==True:
			print('Features Coef Var...')
			try:
				loaded = np.load('{}_features_coef_var_vm.npz'.format(npz_type))
				try:
					x_comp = loaded['x_comp']
				except:
					x_comp = loaded['x']			
					
				x = np.concatenate([x, x_comp], axis=1)
			except:
				print("Generating features...")
				analysis = AnalysisCoefVar(database, 'files3_vivo_mig')
				x_comp = analysis.gen_features()
				np.savez_compressed('{}_features_coef_var_vm.npz'.format(npz_type), x=x_comp)
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
		forest.preprocess_input_vectors()
		pred,predprob = forest.build_rf_model()
#		forest.model_evaluation(prediction, self.y_test)
		f1,prec,rec,acc = forest.model_evaluation2(pred, predprob,self.y_test)
	        print ("f1_binary: %f" % f1)
	        print ("Accuracy: %f" % acc)
	        print ("Precision: %f" % prec)
	        print ("Recall: %f" % rec)

                PrecisionGraphics(str(prunning)+'_'+str(balanced)+'_'+str(imputer)+scores, str(prunning)+'_'+str(balanced)+'_'+str(imputer)+"_precisionvsrecall.pdf")
                NDCGGraphics(str(prunning)+'_'+str(balanced)+'_'+str(imputer)+scores, str(prunning)+'_'+str(balanced)+'_'+str(imputer)+"_NDCG.pdf")
                ids=GetIds("files3_vivo_mig_model")
                ids.gen_features(str(prunning)+'_'+str(balanced)+'_'+str(imputer)+"_ids.txt")


		
        def run(self):
                self.y_train, self.x_train = self.run_features(npz_type='treino', database='files3_vivo_mig', useplans=True,usehistoric=True)
                self.y_test, self.x_test = self.run_features(npz_type='teste', database='files3_vivo_mig_model', useplans=True,usehistoric=True)


                print("Best Configuration")
                self.run_rf(6, None)
	
if __name__ == '__main__':
    claro_mig = ExecuteClaroMig()
    claro_mig.run()

