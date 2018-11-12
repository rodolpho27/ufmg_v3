Cada uma das features pode ser gerada através do script nome_da_feature.py

Cada script possui uma explicação de como deve se rodar em: nome_da_feature.txt

o padrão de rodar o script é:
	python nome_da_feature.py -db nome_do_bando_de_dados -o caminho_de_saidas
	
As saídas são, geralmente *.csv e *_noids.csv. As que serão usadas como entrada para os outros processos são as do tipo *_noids.csv, pois elas não contém os ids de cliente e chamadas.
