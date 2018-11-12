import sqlalchemy
from sqlalchemy import *
from sqlalchemy.sql import *
from math import radians, cos, sin, asin, sqrt, ceil, log
import numpy as np
import csv
import os
import sys, operator
import timeit
from datetime import datetime

date_format = "%Y-%m-%d"


#-----------------------------------------------------------------------------------
#IMPLEMENTACAO DAS FEATURES VIVO:
##qtd - quantidade de ligacoes
##first - feature identificadora para primeira ligacao
#-----------------------------------------------------------------------------------

class AnalysisHistoric:  
    def __init__(self, db='files3_vivo_mig'):
        self.engine = sqlalchemy.create_engine('mysql://root:F4c4&D4d0$@%#@localhost') #connect to server
        self.db = db

    def _get_calls(self):        
        query = """SELECT calls.id_client, calls.id, calls.id_status
                    FROM {}.calls
                    WHERE calls.id_status NOT IN (16,17);""".format(self.db)
        conn = self.engine.connect()
        res = conn.execute(text(query))
        conn.close()
        res = res.fetchall()

        calls_clients = {}
        calls_status = {}
        for client, call, status in res:
            calls_clients[call] = client
            calls_status[call] = status

        return calls_clients, calls_status


    def _get_sales(self):        
        query = """SELECT sales.id_client, sales.id, sales.id_status
                    FROM {}.sales""".format(self.db)
        conn = self.engine.connect()
        res = conn.execute(text(query))
        conn.close()
        res = res.fetchall()

        calls_clients = {}
        calls_status = {}
        for client, call, status in res:
            calls_clients[call] = client
            calls_status[call] = status

        return calls_clients, calls_status


    def _get_call_historic(self):        
        query = """SELECT t.id, t.id_client, t.row_number,
                    CASE WHEN t.row_number = 1 THEN 1 ELSE 0 END as first_call
                    FROM(
                    SELECT a.id_client, a.id, count(*) as row_number
                    FROM {}.calls a
                    JOIN {}.calls b ON a.id_client = b.id_client AND a.id >= b.id
                    GROUP BY a.id_client, a.id) AS t""".format(self.db, self.db)
        conn = self.engine.connect()
        res = conn.execute(text(query))
        conn.close()
        res = res.fetchall()

        calls_historic = {}
        for _id, client, qtd, first in res:
            calls_historic[_id] = {'qtd': qtd-1, 'first': first}

        return calls_historic


    def _get_sales_historic(self):        
        query = """SELECT t.id, t.id_client, t.row_number,
                    CASE WHEN t.row_number = 1 THEN 1 ELSE 0 END as first_call
                    FROM(
                    SELECT a.id_client, a.id, count(*) as row_number
                    FROM {}.sales a
                    JOIN {}.sales b ON a.id_client = b.id_client AND a.id >= b.id
                    GROUP BY a.id_client, a.id) AS t""".format(self.db, self.db)
        conn = self.engine.connect()
        res = conn.execute(text(query))
        conn.close()
        res = res.fetchall()

        sales_historic = {}
        for _id, client, qtd, first in res:
            sales_historic[_id] = {'qtd': qtd-1, 'first': first}

        return sales_historic


    def gen_features(self):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.sales_clients, self.sales_status= self._get_sales()
        self.calls_historic = self._get_call_historic()
        self.sales_historic = self._get_sales_historic()

        x = np.empty([len(self.calls_clients) + len(self.sales_clients), 2])
        index = 0
        for call in self.calls_clients:
            x[index][0] = self.calls_historic[call]['qtd']
            x[index][1] = self.calls_historic[call]['first']
            
            index += 1

        for call in self.sales_clients:
            x[index][0] = self.sales_historic[call]['qtd']
            x[index][1] = self.sales_historic[call]['first']
            index += 1 
        end = timeit.default_timer()
        print('Time {}'.format(end - start))
        return x

if __name__ == '__main__':
    planos = AnalysisRechargeLagVivo()    
    planos.run()
