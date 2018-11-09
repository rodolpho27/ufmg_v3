# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesIdade:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="10.4.2.17",
                                  user="marcus.goncalves",
                                  passwd="wit2019*",
                                  db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 AS rotulo, ca.id AS id_chamada, cl.id as id_cliente, 
        (CASE
	WHEN client_birthday IS NULL THEN -1
	WHEN YEAR(CURDATE()) -year(client_birthday) < 0 OR YEAR(CURDATE()) -year(client_birthday) > 99 THEN -2
	ELSE YEAR(CURDATE()) -year(client_birthday) 
	END) AS cl_bday,
	(CASE 
	WHEN client_birthday IS NULL THEN -1
	WHEN YEAR(CURDATE()) -year(client_birthday) > 0 AND YEAR(CURDATE()) -year(client_birthday) < 18 THEN -3
	WHEN YEAR(CURDATE()) -year(client_birthday) > 17 AND YEAR(CURDATE()) -year(client_birthday) < 40 THEN -4
	WHEN YEAR(CURDATE()) -year(client_birthday) > 40 AND YEAR(CURDATE()) -year(client_birthday) < 60 THEN -5
	WHEN YEAR(CURDATE()) -year(client_birthday) > 60 AND YEAR(CURDATE()) -year(client_birthday) < 99 THEN -6
	ELSE 0
	END) AS classe_idade
	FROM clients as cl right join calls as ca on ca.id_client=cl.id where ca.id_status NOT IN(16,17);"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"vivo_idade.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'cl_bday','classe_idade'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 AS rotulo, ca.id as id_chamada,cl.id as id_cliente,   
        (CASE
	WHEN client_birthday IS NULL THEN -1
	WHEN YEAR(CURDATE()) -year(client_birthday) < 0 OR YEAR(CURDATE()) -year(client_birthday) > 99 THEN -2
	ELSE YEAR(CURDATE()) -year(client_birthday) 
	END) AS cl_bday,
	(CASE 
	WHEN client_birthday IS NULL THEN -1
	WHEN YEAR(CURDATE()) -year(client_birthday) > 0 AND YEAR(CURDATE()) -year(client_birthday) < 18 THEN -3
	WHEN YEAR(CURDATE()) -year(client_birthday) > 17 AND YEAR(CURDATE()) -year(client_birthday) < 40 THEN -4
	WHEN YEAR(CURDATE()) -year(client_birthday) > 40 AND YEAR(CURDATE()) -year(client_birthday) < 60 THEN -5
	WHEN YEAR(CURDATE()) -year(client_birthday) > 60 AND YEAR(CURDATE()) -year(client_birthday) < 99 THEN -6
	ELSE 0
	END) AS classe_idade
	FROM clients as cl right join sales as ca on ca.id_client=cl.id;""" 
	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output+"vivo_idade.csv", 'w' if sales == 0 else 'a'))
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
        self.read_file('vivo_idade.csv', (1,2), output)
    
        x = np.loadtxt(open('vivo_idade_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('vivo_idade_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x 
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    
    args = parser.parse_args()

    features = FeaturesIdade(args.database)    
    
    y,x = features.gen_features(args.output)
    
    print("Done!")
