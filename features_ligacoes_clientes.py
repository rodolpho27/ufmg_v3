# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesLicacoesClientes:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="10.4.2.17",
                                  user="marcus.goncalves",
                                  passwd="wit2019*",
                                  db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 as rotulo, cons_ld.id_call as id_chamada, cons_ld.id as id_cliente, IF(total_loc!=-1 and total_ld=-1,0,total_ld) AS total_ld,IF(total_ld!=-1 and total_loc=-1,0,total_loc) AS total_local 
        FROM 
	(SELECT cons_loc.id_call,cons_loc.id, sum(cons_loc.count_desc) AS total_loc 
	FROM (SELECT cl.id_call, cl.id, cl_ca.description, IF(cl_ca.id_client IS NULL,-1,COUNT(cl_ca.description)) as count_desc 
	FROM (SELECT cli.id, sa.id as id_call FROM clients as cli inner join calls as sa on sa.id_client=cli.id WHERE sa.id_status NOT IN (16,17)) as cl  
	LEFT JOIN (SELECT cl_ca.id_client, cl_ca.description FROM client_calls as cl_ca WHERE cl_ca.description LIKE '%LOCAL%') AS cl_ca on cl.id=cl_ca.id_client GROUP BY cl.id, cl_ca.description, cl.id_call) AS cons_loc group by cons_loc.id_call,cons_loc.id) AS cons_loc
	INNER JOIN  
	(SELECT cons_ld.id_call,cons_ld.id, sum(cons_ld.count_desc) AS total_ld 
	FROM (SELECT cl.id_call, cl.id, cl_ca.description, IF(cl_ca.id_client IS NULL,-1,COUNT(cl_ca.description)) as count_desc 
	FROM (SELECT cli.id, sa.id as id_call FROM clients as cli inner join calls as sa on sa.id_client=cli.id) as cl  
	LEFT JOIN (SELECT cl_ca.id_client, cl_ca.description FROM client_calls as cl_ca WHERE cl_ca.description LIKE '%LD%' OR cl_ca.description LIKE '%LONGA DISTANCIA%' OR cl_ca.description LIKE '%INTERURBANA%') AS cl_ca on cl.id=cl_ca.id_client GROUP BY cl.id, cl_ca.description, cl.id_call) AS cons_ld group by cons_ld.id_call,cons_ld.id) AS cons_ld ON cons_ld.id_call=cons_loc.id_call;"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"ligacoes.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'total_ld','total_local'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 as rotulo, cons_ld.id_sale as id_chamada, cons_ld.id as id_cliente, IF(total_loc!=-1 and total_ld=-1,0,total_ld) AS total_ld,IF(total_ld!=-1 and total_loc=-1,0,total_loc) AS total_local 
        FROM 
	(SELECT cons_loc.id_sale,cons_loc.id, sum(cons_loc.count_desc) AS total_loc 
	FROM (SELECT cl.id_sale, cl.id, cl_ca.description, IF(cl_ca.id_client IS NULL,-1,COUNT(cl_ca.description)) as count_desc 
	FROM (SELECT cli.id, sa.id as id_sale FROM clients as cli inner join sales as sa on sa.id_client=cli.id) as cl  
	LEFT JOIN (SELECT cl_ca.id_client, cl_ca.description FROM client_calls as cl_ca WHERE cl_ca.description LIKE '%LOCAL%') AS cl_ca on cl.id=cl_ca.id_client GROUP BY cl.id, cl_ca.description, cl.id_sale) AS cons_loc group by cons_loc.id_sale,cons_loc.id) AS cons_loc
	INNER JOIN  
	(SELECT cons_ld.id_sale,cons_ld.id, sum(cons_ld.count_desc) AS total_ld 
	FROM (SELECT cl.id_sale, cl.id, cl_ca.description, IF(cl_ca.id_client IS NULL,-1,COUNT(cl_ca.description)) as count_desc 
	FROM (SELECT cli.id, sa.id as id_sale FROM clients as cli inner join sales as sa on sa.id_client=cli.id) as cl  
	LEFT JOIN (SELECT cl_ca.id_client, cl_ca.description FROM client_calls as cl_ca WHERE cl_ca.description LIKE '%LD%' OR cl_ca.description LIKE '%LONGA DISTANCIA%' OR cl_ca.description LIKE '%INTERURBANA%') AS cl_ca on cl.id=cl_ca.id_client 
	GROUP BY cl.id, cl_ca.description, cl.id_sale) AS cons_ld group by cons_ld.id_sale,cons_ld.id) AS cons_ld ON cons_ld.id_sale=cons_loc.id_sale;""" 
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output+"ligacoes.csv", 'w' if sales == 0 else 'a'))
        for row in cursor.fetchall():
            writer.writerow(row)
            
            
    def read_file(self,filename, cols, output):

        df = pd.read_csv(filename)
        #elimina colunas onde estao ids.
        df=df.drop(df.columns[[cols]], axis=1)
        df.to_csv(os.path.splitext(output+filename)[0]+'_noids.csv', index=False)
    
    
    def gen_features(self, output=''):
        """ Gera features e salva em arquivo CSV
        Lê arquivo CSV gerado e retorna np arrays das features
        """
        
        self._get_clients_infocalls(output)
        self._get_clients_infosales(output)
        self.db.close()
            
        print("Eliminando ids das features")
        self.read_file('ligacoes.csv', (1,2), output)
    
        x = np.loadtxt(open('ligacoes_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('ligacoes_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        #print(y[3], x[3])
        print(x[0],y[0], len(x), len(y))
        return y, x         
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    
    args = parser.parse_args()

    features = FeaturesLicacoesClientes(args.database)    
    
    y,x = features.gen_features(args.output)
    
    print("Done!")
