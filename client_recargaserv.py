# -*- coding: utf-8 -*-

import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesServicosdeRecarga:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="10.4.2.17",
                                  user="marcus.goncalves",
                                  passwd="wit2019*",
                                  db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 AS rotulo, id_call as id_chamada, 
	IF(id_client IS NULL,-1,id_client) AS id_cliente,
	IF(bonus_voz IS NULL,-1,bonus_voz) AS bonus_voz,
	IF(bonus_esp_rec IS NULL,-1,bonus_esp_rec) AS bonus_esp_rec,
	IF(rec_10 IS NULL, -1, rec_10) AS rec_10,
	IF(chip_pre_rec_10 IS NULL,-1,chip_pre_rec_10) AS chip_pre_rec_10,
	IF(on_net_3 IS NULL,-1,on_net_3) AS on_net_3,
	IF(on_net_7 IS NULL,-1,on_net_3) AS on_net_7,
	IF(prezao_14 IS NULL,-1,prezao_14) AS prezao_14,
	IF(off_net_7 IS NULL,-1,off_net_7) AS off_net_7
	FROM
	(select serv.id_client, sum(serv.bonus_voz) AS bonus_voz, sum(serv.bonus_esp_rec) AS bonus_esp_rec, sum(rec_10) AS rec_10, sum(serv.chip_pre_rec_10) AS chip_pre_rec_10, sum(serv.on_net_3) AS on_net_3, sum(serv.on_net_7) AS on_net_7,sum(prezao_14) AS prezao_14,sum(off_net_7)  AS off_net_7
	FROM
	(SELECT serv.id_client,
	IF(serv.type LIKE 'bonus patrocinado voz', count(serv.id_client),0) AS bonus_voz,
	IF(serv.type LIKE 'bonus especial de recarga%', count(serv.id_client),0) AS bonus_esp_rec,
	IF(serv.type LIKE '%10', count(serv.id_client),0) AS rec_10,
	IF(serv.type LIKE '%chip pre%recarga%10', count(serv.id_client),0) AS chip_pre_rec_10,
	IF(serv.type LIKE '%on net 3%', count(serv.id_client),0) AS on_net_3,
	IF(serv.type LIKE '%on net 7%', count(serv.id_client),0) AS on_net_7,
	IF(serv.type LIKE 'prezao 14%', count(serv.id_client),0) AS prezao_14,
	IF(serv.type LIKE 'off net 7%', count(serv.id_client),0) AS off_net_7
	FROM (SELECT DISTINCT id_client, type, value, date_recharge,time_recharge
	FROM client_recharges) as serv GROUP BY serv.id_client, serv.type) as serv
	GROUP BY serv.id_client) as serv1
	RIGHT JOIN (SELECT sa.id as id_call, cli.id as id_client1
	FROM clients as cli INNER JOIN calls as sa ON sa.id_client=cli.id WHERE sa.id_status NOT IN(16,17)) as cl
	ON serv1.id_client=cl.id_client1;"""

	
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"recarga_servicos.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'bonus_voz','bonus_esp_rec', 'rec_10', 'chip_pre_rec_10', 'on_net_3', 'on_net_7', 'prezao_14', 'off_net_7'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 as rotulo, id_sale as id_chamada, 
	IF(id_client IS NULL,-1,id_client) AS id_cliente,
	IF(bonus_voz IS NULL,-1,bonus_voz) AS bonus_voz,
	IF(bonus_esp_rec IS NULL,-1,bonus_esp_rec) AS bonus_esp_rec,
	IF(rec_10 IS NULL, -1, rec_10) AS rec_10,
	IF(chip_pre_rec_10 IS NULL,-1,chip_pre_rec_10) AS chip_pre_rec_10,
	IF(on_net_3 IS NULL,-1,on_net_3) AS on_net_3,
	IF(on_net_7 IS NULL,-1,on_net_3) AS on_net_7,
	IF(prezao_14 IS NULL,-1,prezao_14) AS prezao_14,
	IF(off_net_7 IS NULL,-1,off_net_7) AS off_net_7
	FROM
	(select serv.id_client, sum(serv.bonus_voz) AS bonus_voz, sum(serv.bonus_esp_rec) AS bonus_esp_rec, sum(rec_10) AS rec_10, sum(serv.chip_pre_rec_10) AS chip_pre_rec_10, sum(serv.on_net_3) AS on_net_3, sum(serv.on_net_7) AS on_net_7, sum(prezao_14) AS prezao_14,sum(off_net_7)  AS off_net_7
	FROM
	(SELECT serv.id_client,
	IF(serv.type LIKE 'bonus patrocinado voz', count(serv.id_client),0) AS bonus_voz,
	IF(serv.type LIKE 'bonus especial de recarga%', count(serv.id_client),0) AS bonus_esp_rec,
	IF(serv.type LIKE '%10', count(serv.id_client),0) AS rec_10,
	IF(serv.type LIKE '%chip pre%recarga%10', count(serv.id_client),0) AS chip_pre_rec_10,
	IF(serv.type LIKE '%on net 3%', count(serv.id_client),0) AS on_net_3,
	IF(serv.type LIKE '%on net 7%', count(serv.id_client),0) AS on_net_7,
	IF(serv.type LIKE 'prezao 14%', count(serv.id_client),0) AS prezao_14,
	IF(serv.type LIKE 'off net 7%', count(serv.id_client),0) AS off_net_7
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
        writer = csv.writer(open(output+"recarga_servicos.csv", 'w' if sales == 0 else 'a'))
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
        self.read_file('recarga_servicos.csv', (1,2), output)
    
        x = np.loadtxt(open('recarga_servicos_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,1:]
        y = np.loadtxt(open('recarga_servicos_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1)[:,:1]
        
        return y, x     
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informacoes de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saidas.')
    
    args = parser.parse_args()

    features = FeaturesServicosdeRecarga(args.database)    
    
    y,x = features.gen_features(args.output)
    
    print("Done!")
