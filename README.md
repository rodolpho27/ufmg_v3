

Para a execução dos scripts é necessária a instalação do Python 2.7.

Além dos pacotes contidos na biblioteca padrão, é necessária a instalação dos seguintes módulos:
- scykit-learn;
- panda;
- MySQLdb; (apt-get install python-mysqldb 
			sudo pip install mysqlclient #MySQL
			easy_install --upgrade pytz  #PyTZ
			sudo apt-get install python3-dateutil 
			pip install python-dateutil #DateUtil
			pip install sqlalchemy


)

Os scripts não necessitam de parâmetros de entrada para sua execução.

Os dados serão lidos diretamente do banco de dados mysql, sendo que é necessário que a base tenha "F4c4&D4d0$@%#" definido como a senha de root.

O output gerado pelo código são as medidas de F1, Precisão, Revocação e Acurácia. Além disso, é também gerado um arquivo de texto contendo os scores
de cada chamada e os IDs das chamadas na base de dados. Por fim, sao tambm
gerados os graficos de NDCG@N e precisao vs revocacao no formado PDF.


Para a execução dos resultados com o melhor refinamento, basta executar:

	python melhores_resultados/exec_claro_mig_database_3_save.py

ou:
	python melhores_resultados/exec_vivo_mig_database_3_save.py





Os scripts contendo as analises experimentais estão localizados no diretório
"analises_experimentais". 
As análises de desbalanceamento, data inputation e poda podem ser executadas com
os seguintes comandos:
	 python analises_experimentais/balanceamento_prunning_inputation_claro/exp_balanceamento_prunning.py

ou:
	 python analises_experimentais/balanceamento_prunning_inputation_vivo/exp_balanceamento_prunning.py



Em todas as execucoes, os scripts geram como saida os resultados de
efetividade, arquivos com scores, IDs de chamadas e graficos, como mencionado
anteriormente.

