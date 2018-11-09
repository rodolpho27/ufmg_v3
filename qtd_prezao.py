# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesQtdPrezao:
  
    def __init__(self, database):
        self.db = MySQLdb.connect(host="10.4.2.17",
                                  user="marcus.goncalves",
                                  passwd="wit2019*",
                                  db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 AS rotulo, id_call AS id_chamada, 
	IF(id_client IS NULL,-1,id_client) AS id_cliente,
	IF(prezao_muito_mais IS NULL,-1,prezao_muito_mais) AS prezao_muito_mais,
	IF(prezao_muito_mais_mensal IS NULL,-1,prezao_muito_mais_mensal) AS prezao_muito_mais_mensal,
	IF(prezao_fala_mais_mensal IS NULL,-1,prezao_fala_mais_mensal) AS prezao_fala_mais_mensal,
	IF(prezao_ilimitado IS NULL,-1,prezao_ilimitado) AS prezao_ilimitado
	FROM
	(select serv.id_client, sum(serv.prezao_muito_mais) AS prezao_muito_mais, sum(serv.prezao_muito_mais_mensal) AS prezao_muito_mais_mensal, sum(serv.prezao_fala_mais_mensal) AS prezao_fala_mais_mensal, sum(serv.prezao_ilimitado) AS prezao_ilimitado
	FROM 
	(SELECT serv.id_client,
	IF(serv.type LIKE 'prezao muito+' OR serv.type LIKE '%prezao muito mais', count(serv.id_client),0) AS prezao_muito_mais,
	IF(serv.type LIKE 'prezao muito mais mensal', count(serv.id_client),0) AS prezao_muito_mais_mensal,
	IF(serv.type LIKE 'prezao fala mais mensal', count(serv.id_client),0) AS prezao_fala_mais_mensal,
	IF(serv.type LIKE 'prezao ilimitado', count(serv.id_client),0) AS prezao_ilimitado
	FROM (SELECT DISTINCT id_client, type, value, date_recharge,time_recharge
	FROM client_recharges) as serv GROUP BY serv.id_client, serv.type) as serv
	GROUP BY serv.id_client) as serv1
	RIGHT JOIN (SELECT sa.id as id_call, cli.id as id_client1
	FROM clients as cli INNER JOIN calls as sa ON sa.id_client=cli.id where sa.id_status NOT IN (16,17)) as cl ON serv1.id_client=cl.id_client1;"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"qtd_prezao.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'prezao_muito_mails','prezao_muito_mais_mensal', 'prezao_fala_mais_mensal', 'prezao_ilimitado'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 AS rotulo, id_sale AS id_chamada , 
	IF(id_client IS NULL,-1,id_client) AS id_cliente,
	IF(prezao_muito_mais IS NULL,-1,prezao_muito_mais) AS prezao_muito_mais,
	IF(prezao_muito_mais_mensal IS NULL,-1,prezao_muito_mais_mensal) AS prezao_muito_mais_mensal,
	IF(prezao_fala_mais_mensal IS NULL,-1,prezao_fala_mais_mensal) AS prezao_fala_mais_mensal,
	IF(prezao_ilimitado IS NULL,-1,prezao_ilimitado) AS prezao_ilimitado
	FROM
	(select serv.id_client, sum(serv.prezao_muito_mais) AS prezao_muito_mais, sum(serv.prezao_muito_mais_mensal) AS prezao_muito_mais_mensal, sum(serv.prezao_fala_mais_mensal) AS prezao_fala_mais_mensal, sum(serv.prezao_ilimitado) AS prezao_ilimitado
	FROM 
	(SELECT serv.id_client,
	IF(serv.type LIKE 'prezao muito+' OR serv.type LIKE '%prezao muito mais', count(serv.id_client),0) AS prezao_muito_mais,
	IF(serv.type LIKE 'prezao muito mais mensal', count(serv.id_client),0) AS prezao_muito_mais_mensal,
	IF(serv.type LIKE 'prezao fala mais mensal', count(serv.id_client),0) AS prezao_fala_mais_mensal,
	IF(serv.type LIKE 'prezao ilimitado', count(serv.id_client),0) AS prezao_ilimitado
	FROM (SELECT DISTINCT id_client, type, value, date_recharge,time_recharge
	FROM client_recharges) as serv GROUP BY serv.id_client, serv.type) as serv
	GROUP BY serv.id_client) as serv1
	RIGHT JOIN (SELECT sa.id as id_sale, cli.id as id_client1
	FROM clients as cli INNER JOIN sales as sa ON sa.id_client=cli.id) as cl
	ON serv1.id_client=cl.id_client1;"""
         
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output+"qtd_prezao.csv", 'w' if sales == 0 else 'a'))
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
        self.read_file('qtd_prezao.csv', (1,2), output)
    
        x = np.loadtxt(open('qtd_prezao_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('qtd_prezao_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x 
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    
    args = parser.parse_args()

    features = FeaturesQtdPrezao(args.database)    
    
    y,x = features.gen_features(args.output)   
    
    print("Done!")
