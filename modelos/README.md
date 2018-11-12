O código RF recebe dois conjuntos de features para cada chamada em CSV divididas em treino/teste.
O código utiliza a biblioteca scikit-learn para para predição, e a biblioteca
pandas para leitura dos csvs. A primeira coluna no CSV é nomeada "rotulo" e
contém as variáveis resposta (usuário aceita ou não o produto) e as demais colunas
contêm as features utilizadas na classificacao. 

Parâmetros:
-i features de treino
-o features de teste
-t número de árovres
-b utiliza ou não o tratamento de desbalanceamento
-n poda as arvores para conter no mínimo n nós folha.
-s scores preditos e reais

Input:
python ./rf.py -i treino_sample.csv -o teste_sample.csv -b 1 -p 0 -t 100 -s scores_preditos.txt

Saída:
Efetividade em Macro-F1, Micro-F1, F1 binária, precisão e revocação
e scores scores de cada chamada, na segunda coluna do arquivo scores_preditos.txt


A execução do modelo com janelas temporais é descrita no diretório scripts/janela_temporal_deslizante/
A avaliação do ranking obtido pelos modelos é descrito no diretório scripts/avaliacao_ranking