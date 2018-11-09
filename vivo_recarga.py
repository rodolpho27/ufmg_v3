# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesVivoRecarga:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="10.4.2.17",
                                  user="marcus.goncalves",
                                  passwd="wit2019*",
                                  db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 as rotulo, id_call AS id_chamda, 
	IF(id_client IS NULL,-1,id_client) AS id_client,
	IF(rec_fisica IS NULL,-1,rec_fisica) AS rec_fisica,
	IF(rec_pdv IS NULL,-1,rec_pdv) AS rec_pdv,
	IF(rec_combo IS NULL, -1, rec_combo) AS rec_combo,
	IF(rec_credito_antecipado IS NULL,-1,rec_credito_antecipado) AS rec_credito_antecipado,
	IF(rec_servcel IS NULL,-1,rec_servcel) AS rec_servcel
	FROM
	(select serv.id_client, sum(serv.rec_fisica) AS rec_fisica, sum(serv.rec_pdv) AS rec_pdv, sum(rec_combo) AS rec_combo, sum(serv.rec_credito_antecipado) AS rec_credito_antecipado, sum(serv.rec_servcel) AS rec_servcel
	FROM
	(SELECT serv.id_client,
	IF(serv.type LIKE '%fisica%', count(serv.id_client),0) AS rec_fisica,
	IF(serv.type LIKE '%pdv%', count(serv.id_client),0) AS rec_pdv,
	IF(serv.type LIKE '%combo%', count(serv.id_client),0) AS rec_combo,
	IF(serv.type LIKE 'contratacao de vivo credito antecipado', count(serv.id_client),0) AS rec_credito_antecipado,
	IF(serv.type LIKE '%servcel%', count(serv.id_client),0) AS rec_servcel
	FROM (SELECT DISTINCT id_client, type, value, date_recharge,time_recharge
	FROM client_recharges) as serv GROUP BY serv.id_client, serv.type) as serv
	GROUP BY serv.id_client) as serv1
	RIGHT JOIN (SELECT sa.id as id_call, cli.id as id_client1
	FROM clients as cli INNER JOIN calls as sa ON sa.id_client=cli.id WHERE sa.id_status NOT IN (16,17)) as cl
	ON serv1.id_client=cl.id_client1;"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"vivo_recarga.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'rec_fisica','rec_pdv', 'rec_combo', 'rec_credito_antecipado', 'rec_servcel'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 AS rotulo, id_sale AS id_chamada, 
	IF(id_client IS NULL,-1,id_client) AS id_cliente,
	IF(rec_fisica IS NULL,-1,rec_fisica) AS rec_fisica,
	IF(rec_pdv IS NULL,-1,rec_pdv) AS rec_pdv,
	IF(rec_combo IS NULL, -1, rec_combo) AS rec_combo,
	IF(rec_credito_antecipado IS NULL,-1,rec_credito_antecipado) AS rec_credito_antecipado,
	IF(rec_servcel IS NULL,-1,rec_servcel) AS rec_servcel
	FROM
	(select serv.id_client, sum(serv.rec_fisica) AS rec_fisica, sum(serv.rec_pdv) AS rec_pdv, sum(rec_combo) AS rec_combo, sum(serv.rec_credito_antecipado) AS rec_credito_antecipado, sum(serv.rec_servcel) AS rec_servcel
	FROM
	(SELECT serv.id_client,
	IF(serv.type LIKE '%fisica%', count(serv.id_client),0) AS rec_fisica,
	IF(serv.type LIKE '%pdv%', count(serv.id_client),0) AS rec_pdv,
	IF(serv.type LIKE '%combo%', count(serv.id_client),0) AS rec_combo,
	IF(serv.type LIKE 'contratacao de vivo credito antecipado', count(serv.id_client),0) AS rec_credito_antecipado,
	IF(serv.type LIKE '%servcel%', count(serv.id_client),0) AS rec_servcel
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
        writer = csv.writer(open(output+"vivo_recarga.csv", 'w' if sales == 0 else 'a'))
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
        self.read_file('vivo_recarga.csv', (1,2), output)
    
        x = np.loadtxt(open('vivo_recarga_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('vivo_recarga_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    args = parser.parse_args()

    features = FeaturesVivoRecarga(args.database)   
    
    y,x = features.gen_features(args.output)
    
    print("Done!")
