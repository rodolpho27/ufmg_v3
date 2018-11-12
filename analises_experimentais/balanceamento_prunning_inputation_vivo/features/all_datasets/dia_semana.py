# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse
import time

class FeaturesDiaSemana:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="localhost",
                     user="root",
                     passwd="F4c4&D4d0$@%#",
                     db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """ SELECT 0 AS rotulo, WEEK(start_attendance) AS unix_time
        FROM calls AS ca WHERE ca.id_status NOT IN(16,17);"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"timestamp_dia_semana.csv", 'w'))
        writer.writerow(['rotulo','seg','ter','qua','qui','sex','sab','dom'])
        for row in cursor.fetchall():
            auxv=[int(row[0]==1)]
            for hora in range(0,7):        
                if (hora==row[1]):
                    auxv.append(1)
                else:
                    auxv.append(0)


            writer.writerow(auxv)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 AS rotulo, WEEK(start_attendance) AS unix_time
        FROM sales"""
	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output+"timestamp_dia_semana.csv", 'w' if sales == 0 else 'a'))
        for row in cursor.fetchall():
            auxv=[int(row[0]==1)]
            for hora in range(0,7):        
                if (hora==row[1]):
                    auxv.append(1)
                else:
                    auxv.append(0)
            writer.writerow(auxv)
            
    def gen_features(self,output=''):
        """ Gera features e salva em arquivo CSV
        Lê arquivo CSV gerado e retorna np arrays das features
        """
        
        self._get_clients_infocalls(output)
        self._get_clients_infosales(output)
        self.db.close()
        
        #print("Eliminando ids das features")
        #self.read_file('timestamp.csv', (1,2), output)
    
        x = np.loadtxt(open('timestamp_dia_semana.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('timestamp_dia_semana.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x      
    
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    
    args = parser.parse_args()

    features = FeaturesDiaSemana(args.database)    
    
    y,x = features.gen_features(args.output)
    
    print("Done!")
