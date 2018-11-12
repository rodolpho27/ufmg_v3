# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesVivoMailing:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="localhost",
                     user="root",
                     passwd="F4c4&D4d0$@%#",
                     db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 AS rotulo, id_call AS id_chamada,id_client AS id_cliente,
	(CASE
		WHEN cli.type_mailing IS NULL THEN -1
		WHEN cli.type_mailing LIKE '%mailing_padrao%' THEN 1
		WHEN cli.type_mailing NOT LIKE '%mailing_padrao%' THEN 0
	END) AS mailing_padrao,
	(CASE
		WHEN cli.type_mailing IS NULL THEN -1
		WHEN cli.type_mailing LIKE '%whitelist%' THEN 1
		WHEN cli.type_mailing NOT LIKE '%whitelist%' THEN 0
	END) AS whitelist
	FROM 
	(SELECT ca.id as id_call, cl.id as id_client,cl.type_mailing,client_birthday
	FROM clients as cl
	INNER JOIN calls as ca ON ca.id_client=cl.id WHERE ca.id_status NOT IN(16,17)) as cli;"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"vivo_mailing.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'mailing_padrao','whitelist'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 AS rotulo, id_sale AS id_chamada,id_client AS id_cliente,
	(CASE
		WHEN cli.type_mailing IS NULL THEN -1
		WHEN cli.type_mailing LIKE '%mailing_padrao%' THEN 1
		WHEN cli.type_mailing NOT LIKE '%mailing_padrao%' THEN 0
	END) AS mailing_padrao,
	(CASE
		WHEN cli.type_mailing IS NULL THEN -1
		WHEN cli.type_mailing LIKE '%whitelist%' THEN 1
		WHEN cli.type_mailing NOT LIKE '%whitelist%' THEN 0
	END) AS whitelist
	FROM 
	(SELECT ca.id as id_sale, cl.id as id_client,cl.type_mailing,cl.client_birthday 
	FROM clients as cl
	INNER JOIN sales as ca ON ca.id_client=cl.id) as cli;""" 
	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output+"vivo_mailing.csv", 'w' if sales == 0 else 'a'))
        for row in cursor.fetchall():
            writer.writerow(row)
            
            
    def read_file(self,filename, cols, output):

        df = pd.read_csv(filename)
        #elimina colunas onde estao ids.
        df=df.drop(df.columns[[cols]], axis=1)
        df.to_csv(os.path.splitext(output+filename)[0]+'_noids.csv', index=False)
    
    
    def gen_features(self,output=''):
        """ Gera features e salva em arquivo CSV
        Lê arquivo CSV gerado e retorna np arrays das features
        """
        
        self._get_clients_infocalls(output)
        self._get_clients_infosales(output)
        self.db.close()
        
        print("Eliminando ids das features")
        self.read_file('vivo_mailing.csv', (1,2), output)
    
        x = np.loadtxt(open('vivo_mailing_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('vivo_mailing_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x 
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    
    args = parser.parse_args()

    features = FeaturesVivoMailing(args.database)    
    
    y,x = features.gen_features(args.output)
  
    print("Done!")
