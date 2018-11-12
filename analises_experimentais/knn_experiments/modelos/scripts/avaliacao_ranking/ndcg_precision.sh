#!/bin/bash

resultsfile=$1


#for x in xab xac xad xae xaf; do

cat $resultsfile | grep "predicted_real" | awk '{print $2}' > auxres
cat $resultsfile | grep "predicted_real" | awk 'BEGIN{c=1;}{print int($3)" qid:2 2:0.0 #docid = "c; c=c+1}' > auxreal

perl Eval-Score-3.0.pl auxreal auxres ndcgresults.txt 0

