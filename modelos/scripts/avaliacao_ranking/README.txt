A avaliação do raking é baseada em duas medidas:
NDCG@N e 
PRECISION@N

A implementação dessas medidas foi obtida através da ferramenta de avaliação
Eval-Score-3.0.pl, disponível nesse diretório. Essa ferramenta de avaliação
foi utilizada no desafio oficial de learning-to-rank da microsoft 
(obtida de
https://www.microsoft.com/en-us/research/project/letor-learning-rank-information-retrieval/#!letor-3-0).

Com o objetivo de simplificar a execução, criamos o script  ndcg_precision.sh
que formata a saída das predições do modelo (a saída da execução do modelo
com o  rf.py) para a entrada dessa estratégia de avaliação. 

A execução é:

./ndcg_precision.sh classification_output.txt

onde classification_output.txt  é o arquivo de saída gerado pelo modelo de
predição (rf.py).



Os resultados em NDCG são armazenados no arquivo ndcg_results.txt
