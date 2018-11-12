Esse diretório contém todas as features implementadas.

O Sub-diretório all_datasets contém features que podem ser geradas em todos
os datasets

Os demais subdiretórios (claro_mig, vivo_mig e vivo_up) contêm as
implementações de features que necessitam considerar as especificidades de
cada dataset.



Cada uma das features pode ser gerada através do script nome_da_feature.py

Cada script possui uma explicação de como deve se rodar em: nome_da_feature.txt

o padrão de rodar o script é:
	python all_datasets/nome_da_feature.py -db nome_do_banco_de_dados -o caminho_de_saidas
	

As saídas são os arquivos csv com as features podem ser utilizados para execução do
modelo, no diretório "modelos" desse projeto.

