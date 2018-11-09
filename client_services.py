# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesServicosdoCliente:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="10.4.2.17",
                                  user="marcus.goncalves",
                                  passwd="wit2019*",
                                  db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 AS rotulo, id_call AS id_chamada, 
	IF(id_client IS NULL,-1,id_client) AS id_cliente,
	IF(inter_avulsa IS NULL,-1,inter_avulsa) AS inter_avulsa,
	IF(blacklist IS NULL,-1,blacklist) AS blacklist,
	IF(pct_100mb_wh IS NULL,-1,pct_100mb_wh) AS pct_100mb_wh,
	IF(pct_200mb_wh IS NULL,-1,pct_200mb_wh) AS pct_200mb_wh,
	IF(som_cham IS NULL,-1,som_cham) AS som_cham,
	IF(som_cham_ili IS NULL,-1,som_cham_ili) AS som_cham_ili,
	IF(claro_rec IS NULL,-1,claro_rec) AS claro_rec,
	IF(claro_rec_prem IS NULL,-1,claro_rec_prem) AS claro_rec_prem,
	IF(rec_avulso IS NULL,-1,rec_avulso) AS rec_avulso,
	IF(claro_contato IS NULL,-1,claro_contato) AS claro_contato,
	IF(claro_conecta IS NULL,-1,claro_conecta) AS claro_conecta
	FROM
	(select serv.id_client, sum(serv.inter_avulsa) AS inter_avulsa, sum(serv.blacklist) AS blacklist, sum(serv.pct_100mb_wh) AS pct_100mb_wh, sum(serv.pct_200mb_wh) AS pct_200mb_wh, sum(som_cham) AS som_cham, sum(som_cham_ili)  AS som_cham_ili, sum(claro_rec) AS claro_rec, sum(claro_rec_prem) AS claro_rec_prem, sum(rec_avulso) AS rec_avulso, sum(claro_contato) AS claro_contato, sum(claro_conecta) AS claro_conecta
	FROM 
	(SELECT serv.id_client,
	IF(serv.description LIKE '%internet avulsa%', count(serv.id_client),0) AS inter_avulsa,
	IF(serv.description LIKE '%consulta blacklist%', count(serv.id_client),0) AS blacklist,
	IF(serv.description LIKE 'pct diario 100mb%whatsapp%', count(serv.id_client),0) AS pct_100mb_wh,
	IF(serv.description LIKE 'pct diario 200mb%whatsapp%', count(serv.id_client),0) AS pct_200mb_wh,
	IF(serv.description LIKE 'som de chamada', count(serv.id_client),0) AS som_cham,
	IF(serv.description LIKE 'som de chamada ilimitado', count(serv.id_client),0) AS som_cham_ili,
	IF(serv.description LIKE 'claro recado', count(serv.id_client),0) AS claro_rec,
	IF(serv.description LIKE 'claro recado premium', count(serv.id_client),0) AS claro_rec_prem,
	IF(serv.description LIKE 'recado avulso', count(serv.id_client),0) AS rec_avulso,
	IF(serv.description LIKE '%contato%', count(serv.id_client),0) AS claro_contato,
	IF(serv.description LIKE 'claro conecta', count(serv.id_client),0) AS claro_conecta
	FROM (SELECT DISTINCT id_client, date, time, description, value, created_at
	FROM client_services) as serv GROUP BY serv.id_client, serv.description) as serv
	GROUP BY serv.id_client) as serv1
	RIGHT JOIN (SELECT sa.id as id_call, cli.id as id_client1
	FROM clients as cli INNER JOIN calls as sa ON sa.id_client=cli.id WHERE sa.id_status NOT IN(16,17)) as cl
	ON serv1.id_client=cl.id_client1;"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"servicos.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'inter_avulsa','blacklist', 'pct_100mb_wh', 'pct_200mb_wh', 'som_cham', 'som_cham_ili', 'claro_rec', 'claro_rec_prem', 'rec_avulso', 'claro_contato', 'claro_conecta'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 as rotulo, id_sale as id_chamada, 
        IF(id_client IS NULL,-1,id_client) AS id_cliente,
	IF(inter_avulsa IS NULL,-1,inter_avulsa) AS inter_avulsa,
	IF(blacklist IS NULL,-1,blacklist) AS blacklist,
	IF(pct_100mb_wh IS NULL,-1,pct_100mb_wh) AS pct_100mb_wh,
	IF(pct_200mb_wh IS NULL,-1,pct_200mb_wh) AS pct_200mb_wh,
	IF(som_cham IS NULL,-1,som_cham) AS som_cham,
	IF(som_cham_ili IS NULL,-1,som_cham_ili) AS som_cham_ili,
	IF(claro_rec IS NULL,-1,claro_rec) AS claro_rec,
	IF(claro_rec_prem IS NULL,-1,claro_rec_prem) AS claro_rec_prem,
	IF(rec_avulso IS NULL,-1,rec_avulso) AS rec_avulso,
	IF(claro_contato IS NULL,-1,claro_contato) AS claro_contato,
	IF(claro_conecta IS NULL,-1,claro_conecta) AS claro_conecta
	FROM
	(select serv.id_client, sum(serv.inter_avulsa) AS inter_avulsa, sum(serv.blacklist) AS blacklist, sum(serv.pct_100mb_wh) AS pct_100mb_wh, sum(serv.pct_200mb_wh) AS pct_200mb_wh, sum(som_cham) AS som_cham, sum(som_cham_ili)  AS som_cham_ili, sum(claro_rec) AS claro_rec, sum(claro_rec_prem) AS claro_rec_prem, sum(rec_avulso) AS rec_avulso, sum(claro_contato) AS claro_contato, sum(claro_conecta) AS claro_conecta
	FROM 
	(SELECT serv.id_client,
	IF(serv.description LIKE '%internet avulsa%', count(serv.id_client),0) AS inter_avulsa,
	IF(serv.description LIKE '%consulta blacklist%', count(serv.id_client),0) AS blacklist,
	IF(serv.description LIKE 'pct diario 100mb%whatsapp%', count(serv.id_client),0) AS pct_100mb_wh,
	IF(serv.description LIKE 'pct diario 200mb%whatsapp%', count(serv.id_client),0) AS pct_200mb_wh,
	IF(serv.description LIKE 'som de chamada', count(serv.id_client),0) AS som_cham,
	IF(serv.description LIKE 'som de chamada ilimitado', count(serv.id_client),0) AS som_cham_ili,
	IF(serv.description LIKE 'claro recado', count(serv.id_client),0) AS claro_rec,
	IF(serv.description LIKE 'claro recado premium', count(serv.id_client),0) AS claro_rec_prem,
	IF(serv.description LIKE 'recado avulso', count(serv.id_client),0) AS rec_avulso,
	IF(serv.description LIKE '%contato%', count(serv.id_client),0) AS claro_contato,
	IF(serv.description LIKE 'claro conecta', count(serv.id_client),0) AS claro_conecta
	FROM (SELECT DISTINCT id_client, date, time, description, value, created_at
	FROM client_services) as serv GROUP BY serv.id_client, serv.description) as serv
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
        writer = csv.writer(open(output+"servicos.csv", 'w' if sales == 0 else 'a'))
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
        self.read_file('servicos.csv', (1,2), output)
    
        x = np.loadtxt(open('servicos_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('servicos_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x    
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    
    args = parser.parse_args()

    features = FeaturesServicosdoCliente(args.database)    
    
    y,x = features.gen_features(args.output)
    
    print("Done!")
