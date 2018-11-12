#!/bin/bash

dataset=$1
timestamps=$2
classifier=$3
output=$4



#ordena de acordo com a data
pr -m -s" |" -t $timestamps $dataset | sort -n | awk -F"|"  '{print $2}' > aux.csv 

#executa o classificador
#./roda_rf.sh aux.csv resultsu/tudo


size=`wc -l aux.csv  | awk '{print int($1/6)+1}'`

split -l $size aux.csv
head -n 1 aux.csv > heads
cat heads xab > xabt ; mv xabt xab
cat heads xac > xact ; mv xact xac
cat heads xad > xadt ; mv xadt xad
cat heads xae > xaet ; mv xaet xae
cat heads xaf > xaft ; mv xaft xaf


python $classifier -i xaa -o xab -s ${output}.part1
python $classifier -i xab -o xac -s ${output}.part2
python $classifier -i xac -o xad -s ${output}.part3
python $classifier -i xad -o xae -s ${output}.part4
python $classifier -i xae -o xaf -s ${output}.part5

rm xaa xab xac xad xae xaf xaat xabt xact xadt xaet xaft heads

