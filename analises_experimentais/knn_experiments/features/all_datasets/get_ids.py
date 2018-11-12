# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse
import time

class GetIds:  
    def __init__(self, database):
        self.db = MySQLdb.connect(host="localhost",
                     user="root",
                     passwd="F4c4&D4d0$@%#",
                     db=database)

    def _get_clients_infocalls(self, output=''):
        #query das calls
        query = """ SELECT ca.id 
        FROM calls AS ca WHERE ca.id_status NOT IN(16,17);"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output, 'w'))
        writer.writerow(['ids'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output=''):
    
    
        #query das sales
        query = """SELECT sales.id
        FROM sales"""
	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output, 'w' if sales == 0 else 'a'))
        for row in cursor.fetchall():
            writer.writerow(row)
    

    def gen_features(self):
        query = """ SELECT UNIX_TIMESTAMP(start_attendance) AS unix_time
        FROM calls AS ca WHERE ca.id_status NOT IN(16,17);"""

        x = []
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Saida calls")
        for row in cursor.fetchall():
            x.append(row)

        query = """SELECT UNIX_TIMESTAMP(start_attendance) AS unix_time
        FROM sales"""
    
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Saida sales")
        for row in cursor.fetchall():
            x.append(row) 
        
        return np.array(x)
    
    def gen_features(self,output=''):
        """ Gera features e salva em arquivo CSV
        Lê arquivo CSV gerado e retorna np arrays das features
        """

        self._get_clients_infocalls(output)
        self._get_clients_infosales(output)
        self.db.close()

        #print("Eliminando ids das features")
        #self.read_file('ids.csv', (1,2), output)
    
#        x = np.loadtxt(open('ids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
#        y = np.loadtxt(open('ids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        y = np.loadtxt(open(output, "rb"), dtype=float, delimiter=",", converters={0:force_float}, skiprows=1)
        
        return y  

def force_float(value):
    try:
        float(value)
        return value
    except:
        return 0            

if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    args = parser.parse_args()

    clients = Analysis(args.database)    
    clients._get_clients_infocalls(args.output)
    
    clients._get_clients_infosales(args.output)
    clients.db.close()

    
    print("Done!")
