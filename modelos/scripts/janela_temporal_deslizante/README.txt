A janela temporal deslizante gera os resultados a partir de uma única base de dados, particionando a mesma em
segmentos temporais do mesmo tamanho.



Input:

./testa_janela_deslizante.sh dataset.csv timestamps.csv ../../../rf.py   output.txt


onde:
- dataset.csv é a coleção com as features e labels
- timestamps.csv são os timestamps das chamadas do dataset, obtidos a partir do código get_timestamp.py
Ex.: python get_timestamp.py  -db files3_claro_mig -o timestamps.csv
- rf.py é o classificador 
- output.txt é o arquivo de saída.



Output:
Resultados dos experimentos em 5 partes, denominadas:
output.txt.part1, output.txt.part2, ... output.txt.part5

Cada saída corresponde ao ao output do classificador em uma das partes da
janela temporal deslizante. Logo, esse arquivo contém a efetividade em Macro-F1, Micro-F1, F1 binária, precisão e revocação
da parte, bem como os scores das chamadas na parte




