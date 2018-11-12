# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesMailing:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="localhost",
                     user="root",
                     passwd="F4c4&D4d0$@%#",
                     db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 as rotulo, id_call as id_chamada,id_client as id_cliente,
	IF(ma.name LIKE '%SERASA%',1,0) AS serasa,
	IF(ma.name LIKE '%Cli_bom_relacionamento%',1,0) AS cli_bom_rel,
	IF(ma.name LIKE '%MPLAY%',1,0) AS mplay,
	IF(ma.name LIKE '%COMPLEMENTARMQ45%',1,0) AS compl_m45,
	IF(ma.name LIKE '%Inadimplentes_no_grupo_WL%',1,0) AS inadim,
	IF(ma.name LIKE '%Reprovados_no_crivo_WL%',1,0) AS rep_crivo,
	IF(ma.name LIKE '%PreControle_area_cabeada%',1,0) AS pre_are_cab,
	IF(ma.name LIKE '%NOMESINVALIDOS%',1,0) AS nome_inv,
	IF(ma.name LIKE '%PUBLICO%',1,0) AS pub,
	IF(ma.name LIKE '%90DIAS%',1,0) AS noventa_dias,
	IF(ma.name LIKE '%31a59%',1,0) AS 31a59,
	IF(ma.name LIKE '%RESCORE90%',1,0) AS recore90,
	IF(ma.name LIKE '%DOC10%',1,0) AS doc10,
	IF(ma.name LIKE '%COMPLEMENTAR45%',1,0) AS comp_45,
	IF(ma.name LIKE '%COMPLEMENTAR%' AND ma.name NOT LIKE '%COMPLEMENTAR45%' AND ma.name NOT LIKE '%COMPLEMENTARMQ45%',1,0) AS complementar
	FROM 
	(SELECT ca.id as id_call, cli.id as id_client, cli.id_mailing 
	FROM clients as cli 
	INNER JOIN calls as ca ON ca.id_client=cli.id WHERE ca.id_status NOT IN (16,17)) as cl
	LEFT JOIN mailing as ma ON ma.id=cl.id_mailing;"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"mailing.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'serasa','cli_bom_rel', 'mplay','compl_45', 'inadim', 'rep_crivo', 'pre_are_cab', 'nome_inv', 'pub', 'noventa_dias', '31a59', 'recore90', 'doc10', 'comp_45', 'complementar'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 as rotulo, id_sale as id_chamada,id_client as id_cliente,
	IF(ma.name LIKE '%SERASA%',1,0) AS serasa,
	IF(ma.name LIKE '%Cli_bom_relacionamento%',1,0) AS cli_bom_rel,
	IF(ma.name LIKE '%MPLAY%',1,0) AS mplay,
	IF(ma.name LIKE '%COMPLEMENTARMQ45%',1,0) AS compl_m45,
	IF(ma.name LIKE '%Inadimplentes_no_grupo_WL%',1,0) AS inadim,
	IF(ma.name LIKE '%Reprovados_no_crivo_WL%',1,0) AS rep_crivo,
	IF(ma.name LIKE '%PreControle_area_cabeada%',1,0) AS pre_are_cab,
	IF(ma.name LIKE '%NOMESINVALIDOS%',1,0) AS nome_inv,
	IF(ma.name LIKE '%PUBLICO%',1,0) AS pub,
	IF(ma.name LIKE '%90DIAS%',1,0) AS noventa_dias,
	IF(ma.name LIKE '%31a59%',1,0) AS 31a59,
	IF(ma.name LIKE '%RESCORE90%',1,0) AS recore90,
	IF(ma.name LIKE '%DOC10%',1,0) AS doc10,
	IF(ma.name LIKE '%COMPLEMENTAR45%',1,0) AS comp_45,
	IF(ma.name LIKE '%COMPLEMENTAR%' AND ma.name NOT LIKE '%COMPLEMENTAR45%' AND ma.name NOT LIKE '%COMPLEMENTARMQ45%',1,0) AS complementar
	FROM 
	(SELECT sa.id as id_sale, cli.id as id_client, cli.id_mailing 
	FROM clients as cli 
	INNER JOIN sales as sa ON sa.id_client=cli.id) as cl
	LEFT JOIN mailing as ma ON ma.id=cl.id_mailing;""" 
	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output+"mailing.csv", 'w' if sales == 0 else 'a'))
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
        self.read_file('mailing.csv', (1,2), output)
    
        x = np.loadtxt(open('mailing_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('mailing_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x 
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informacoes de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saidas.')
    
    args = parser.parse_args()

    features = FeaturesMailing(args.database) 
    
    y,x = features.gen_features(args.output)   

    
    print("Done!")
