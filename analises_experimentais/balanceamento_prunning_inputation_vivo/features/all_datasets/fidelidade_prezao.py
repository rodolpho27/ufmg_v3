# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeatureFidelidadePrezao:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="localhost",
                     user="root",
                     passwd="F4c4&D4d0$@%#",
                     db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT  0 AS rotulo, tab.id_call AS id_chamada, tab.id_client1 AS id_cliente,  
        IF(proporcao.prop IS NULL, -1, proporcao.prop)
        FROM (SELECT soma.id_client, (soma.total_meses_fiel/total_meses) AS prop
        FROM (SELECT res.id_client, SUM(res.var) AS total_meses_fiel
        FROM (SELECT id_client, month(date_recharge),
        (CASE 
	WHEN count(id_client) >= 1 THEN 1
	ELSE 0
	END) as var
	FROM client_recharges 
	WHERE type LIKE  '%prezao%' 
	GROUP BY id_client, month(date_recharge)) AS res
	GROUP BY res.id_client) AS soma 
	INNER JOIN
	(SELECT id_client ,count(distinct(month(date_recharge))) AS total_meses  
	FROM client_recharges  
	GROUP BY id_client) AS total ON total.id_client=soma.id_client) as proporcao 
	RIGHT JOIN (SELECT sa.id as id_call, cli.id as id_client1 
	FROM clients as cli INNER JOIN calls as sa ON sa.id_client=cli.id where sa.id_status NOT IN (16,17)) as tab ON tab.id_client1=proporcao.id_client;"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"fidelidade_prezao.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'prop'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 AS rotulo, tab.id_sale as id_chamada, tab.id_client1 as id_cliente,  
        IF(proporcao.prop IS NULL, -1, proporcao.prop)
        FROM (SELECT soma.id_client, (soma.total_meses_fiel/total_meses) AS prop
        FROM (SELECT res.id_client, SUM(res.var) AS total_meses_fiel
        FROM (SELECT id_client, month(date_recharge),
        (CASE 
	WHEN count(id_client) >= 1 THEN 1
	ELSE 0 
	END) as var
	FROM client_recharges 
	WHERE type LIKE  '%prezao%' 
	GROUP BY id_client, month(date_recharge)) AS res
	GROUP BY res.id_client) AS soma 
	INNER JOIN
	(SELECT id_client ,count(distinct(month(date_recharge))) AS total_meses  
	FROM client_recharges  
	GROUP BY id_client) AS total ON total.id_client=soma.id_client) as proporcao 
	RIGHT JOIN (SELECT sa.id as id_sale, cli.id as id_client1 
	FROM clients as cli INNER JOIN sales as sa ON sa.id_client=cli.id) as tab ON tab.id_client1=proporcao.id_client;"""
         
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output+"fidelidade_prezao.csv", 'w' if sales == 0 else 'a'))
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
        self.read_file('fidelidade_prezao.csv', (1,2), output)
    
        x = np.loadtxt(open('fidelidade_prezao_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('fidelidade_prezao_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x   
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    
    args = parser.parse_args()

    features = FeatureFidelidadePrezao(args.database)    

    y,x = features.gen_features(args.output)
    
    print("Done!")
