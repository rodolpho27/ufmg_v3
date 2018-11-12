import numpy as np
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


	def run_features(self, database, npz_type):
		print('Features Iuri Claro...')
		try:
			loaded=np.load('{}_features_iuri.npz'.format(npz_type))
			try:
				x = loaded['x_comp']
			except:
				x = loaded['x']
			y = loaded['y']
		except:         
			print("Generating features...")               
			analysis = AnalysisFeaturesIuri(database)
			y, x = analysis.gen_features()
			np.savez_compressed('{}_features_iuri.npz'.format(npz_type), x=x, y=y)
			
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

		return y, x
	
	def run_rf(self, prunning=5, balanced=None, imputer=False):

		scores='_rf_scores.txt'
		if(imputer):
			imp = Imputer(missing_values=-1, strategy='median').fit(self.x_train)
			self.x_train = imp.transform(self.x_train)
			self.x_test = imp.transform(self.x_test)
		print('Gerando modelo...')
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
                ids=GetIds("files3_claro_mig_model")
                ids.gen_features(str(prunning)+'_'+str(balanced)+'_'+str(imputer)+"_ids.txt")

		
	def run(self):
		self.y_train, self.x_train = self.run_features(npz_type='treino', database='files3_claro_mig')
		self.y_test, self.x_test = self.run_features(npz_type='teste', database='files3_claro_mig_model')

		print("Testando o RF Padrao")
		self.run_rf(6,None)

		print("Testando diferentes podas...")
		self.run_rf(1)
		self.run_rf(2)
		self.run_rf(3)
		self.run_rf(4)
		self.run_rf(5)
		self.run_rf(6)
		print("Testando poda=100 com diferentes balanceamentos...")
		self.run_rf(5,'balanced_subsample')
		self.run_rf(5, {1:100})
		self.run_rf(5, {1:200})
		self.run_rf(5, {1:300})

		print("Testando diferentes podas com imputer...")
		self.run_rf(1,None,True)
		self.run_rf(2,None,True)
		self.run_rf(3,None,True)
		self.run_rf(4,None,True)
		self.run_rf(5,None,True)
		print("Testando poda=100 com diferentes balanceamentos e imputer de dados...")
		self.run_rf(5,'balanced_subsample',True)
		self.run_rf(5, {1:100},True)
		self.run_rf(5, {1:200},True)
		self.run_rf(5, {1:300},True)
		
if __name__ == '__main__':
    claro_mig = ExecuteClaroMig()
    claro_mig.run()
