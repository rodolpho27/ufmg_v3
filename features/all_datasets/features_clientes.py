# -*- coding: utf-8 -*-
import numpy as np
import csv
import os
import MySQLdb
import pandas as pd
import argparse

class FeaturesClientes:  

    def __init__(self, database):
        self.db = MySQLdb.connect(host="10.4.2.17",
                                  user="marcus.goncalves",
                                  passwd="wit2019*",
                                  db=database)

    def _get_clients_infocalls(self, output):
        #query das calls
        query = """SELECT 0 as rotulo, ca.id AS id_chamada, cl.id AS id_cliente,cl.ddd AS ddd, cl.client_regional AS regional, cl.state_name AS estado_chip,cl.cpf_code AS estado_nasc
                FROM clients AS cl INNER JOIN calls AS ca ON ca.id_client=cl.id 
                WHERE ca.id_status NOT IN (16,17); """
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela calls...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        writer = csv.writer(open(output+"features_clientes.csv", 'w'))
        writer.writerow(['rotulo','id_chamada', 'id_cliente', 'ddd', 'regional', 'estado_chip', 'estado_nasc'])
        for row in cursor.fetchall():
            writer.writerow(row)

    def _get_clients_infosales(self, output):
    
        #query das sales
        query = """SELECT 1 as rotulo, ca.id AS id_chamada, cl.id AS id_cliente,cl.ddd AS ddd, cl.client_regional AS regional, cl.state_name AS estado_chip,cl.cpf_code AS estado_nasc
                FROM clients AS cl INNER JOIN sales AS ca ON ca.id_client=cl.id """ 
        print("Conectando ao servidor...")
        cursor = self.db.cursor()
        print("Executando query da tabela sales...")
        cursor.execute(query)

        print("Escrevendo saída para .csv")
        sales=1
        writer = csv.writer(open(output+"features_clientes.csv", 'w' if sales == 0 else 'a'))
        for row in cursor.fetchall():
            writer.writerow(row)
            sales = sales + 1
        
    def _transform_to_catcodes(self, output):
        df = pd.read_csv(output+'features_clientes.csv', sep=",",dtype=object)
        print("Tranformando atributos para categorias numéricas")
        df["estado_chip"] = df["estado_chip"].astype('category')
        df['regional'] = df['regional'].astype('category')
        df["estado_nasc"] = df["estado_nasc"].astype('category')
        df["estado_chip"] = df["estado_chip"].cat.codes
        df["regional"] = df["regional"].cat.codes
        df['estado_chip'].fillna(-1 , inplace=True)
        df['regional'].fillna(-1 , inplace=True)
        df['ddd'].fillna(-1 , inplace=True)
        
        df.to_csv('features_clientes_justcat.csv',mode = 'w', index=False)

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
        self._transform_to_catcodes(output)
            
        print("Eliminando ids das features")
        self.read_file('features_clientes_justcat.csv', (1,2), output)
    
        x = np.loadtxt(open('features_clientes_justcat'+'_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1,converters={1:force_float,2:force_float,3:force_float,4:force_float})[:,1:]
        y = np.loadtxt(open('features_clientes_justcat'+'_noids.csv', "rb"), dtype=float, delimiter=",", skiprows=1,converters={1:force_float,2:force_float,3:force_float,4:force_float})[:,:1]
        #print(y[3], x[3])
        
        return y, x     
    


def force_float(value):
    try:
        float(value)
        return value
    except:
        return -1
    
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Gera features das informações de clientes presentes nas bases.')
    parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas.')
    
    args = parser.parse_args()

    features = FeaturesClientes(args.database)    
    
    y,x = features.gen_features(args.output)
    
    print("Done!")
