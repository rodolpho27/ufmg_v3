import sqlalchemy
from sqlalchemy import *
from sqlalchemy.sql import *
from math import radians, cos, sin, asin, sqrt, ceil, log
import numpy as np
import csv
import os
import sys, operator
import timeit
import argparse

class Analysis:  
    def __init__(self):
        self.engine = sqlalchemy.create_engine('mysql://root:F4c4&D4d0$@%#@localhost') #connect to server

    def _get_calls(self):        
        query = """SELECT calls.id_client, calls.id, calls.id_status
                    FROM files3_vivo_mig.calls
                    WHERE calls.id_status <> 16 OR calls.id_status <> 17"""
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
                    FROM files3_vivo_mig.sales"""
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


    def _get_recharges(self):
        query = """SELECT client_recharges.id_client, DATEDIFF(CURRENT_DATE(), MAX(client_recharges.date_recharge)) as recency, 
                        COUNT(1) as frequency,
                        AVG(client_recharges.value) AS average_value, 
                        STDDEV(client_recharges.value)  AS std_value 
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.calls
                    ON client_recharges.id_client = calls.id_client
                    GROUP BY client_recharges.id_client;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        to_append = {}
        clients = []
        for client, recency, freq, average, std in res:
            clients.append(client)
            to_append[client] =   {
                                'id_client': client,
                                'recency': recency,
                                'frequency': freq,
                                'average': average,
                                'std': std
                            }


        query = """SELECT client_recharges.id_client, DATEDIFF(CURRENT_DATE(), MAX(client_recharges.date_recharge)) as recency, 
                        COUNT(1) as frequency,
                        AVG(client_recharges.value) AS average_value, 
                        STDDEV(client_recharges.value)  AS std_value 
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.sales
                    ON client_recharges.id_client = sales.id_client
                    GROUP BY client_recharges.id_client;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        for client, recency, freq, average, std in res:
            clients.append(client)
            to_append[client] =   {
                                'id_client': client,
                                'recency': recency,
                                'frequency': freq,
                                'average': average,
                                'std': std
                            }
        return to_append


    def _get_recharges_dow(self):
        query = """SELECT client_recharges.id_client, DAYOFWEEK(client_recharges.date_recharge) AS dow, COUNT(1) AS frequency 
                       FROM files3_vivo_mig.client_recharges 
                       INNER JOIN files3_vivo_mig.calls
                       ON calls.id_client = client_recharges.id_client
                       GROUP BY client_recharges.id_client, dow;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        clients_dow = {}
        clients = []
        for client, dow, freq in res:
            _dow = int(dow)
            clients.append(client)
            if client not in clients_dow:
                clients_dow[client] = {1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0, 5: 0.0, 6: 0.0, 7: 0.0}
            clients_dow[client][_dow] = freq


        query = """SELECT client_recharges.id_client, DAYOFWEEK(client_recharges.date_recharge) AS dow, COUNT(1) AS frequency 
                       FROM files3_vivo_mig.client_recharges 
                       INNER JOIN files3_vivo_mig.sales
                       ON sales.id_client = client_recharges.id_client
                       GROUP BY client_recharges.id_client, dow;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        for client, dow, freq in res:
            _dow = int(dow)
            clients.append(client)
            if client not in clients_dow:
                clients_dow[client] = {1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0, 5: 0.0, 6: 0.0, 7: 0.0}
            clients_dow[client][_dow] = freq 

        for client in clients_dow:
            total = sum(clients_dow[client].values())
            for dow in clients_dow[client]:
                clients_dow[client][dow] /= float(total)

        return clients_dow


    def _get_recharges_hour(self):
        query = """SELECT client_recharges.id_client, HOUR(client_recharges.time_recharge) AS hour, COUNT(1) AS frequency 
                       FROM files3_vivo_mig.client_recharges
                       INNER JOIN files3_vivo_mig.calls
                       ON calls.id_client = client_recharges.id_client
                       GROUP BY client_recharges.id_client, hour;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        clients_hour = {}
        clients = []
        for client, hour, freq in res:
            if hour:
                _hour = int(hour)
                clients.append(client)
                if client not in clients_hour:
                    clients_hour[client] = {}
                    for h in range(0, 24):
                        clients_hour[client][h] = 0.0
                clients_hour[client][_hour] = freq


        query = """SELECT client_recharges.id_client, HOUR(client_recharges.time_recharge) AS hour, COUNT(1) AS frequency 
                       FROM files3_vivo_mig.client_recharges
                       INNER JOIN files3_vivo_mig.sales
                       ON sales.id_client = client_recharges.id_client
                       GROUP BY client_recharges.id_client, hour;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        for client, hour, freq in res:
            if hour:
                _hour = int(hour)
                clients.append(client)
                if client not in clients_hour:
                    clients_hour[client] = {}
                    for h in range(0, 24):
                        clients_hour[client][h] = 0.0
                clients_hour[client][_hour] = freq

        for client in clients_hour:
            total = sum(clients_hour[client].values())
            for hour in clients_hour[client]:
                clients_hour[client][hour] /= float(total)

        return clients_hour


    def run(self, outputfile):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.sales_clients, self.sales_status= self._get_sales()
        self.to_append = self._get_recharges()
        self.clients_dow = self._get_recharges_dow()
        self.clients_hour = self._get_recharges_hour()

        index_set = ['label', 'id_status', 'recency', 'frequency', 'average', 'std',
                        'dow_1', 'dow_2', 'dow_3', 'dow_4', 'dow_5', 'dow_6', 'dow_7',
                        'hour_0', 'hour_1', 'hour_2', 'hour_3', 'hour_4', 'hour_5', 'hour_6', 'hour_7', 'hour_8', 'hour_9',
                        'hour_10', 'hour_11', 'hour_12', 'hour_13', 'hour_14', 'hour_15', 'hour_16', 'hour_17', 'hour_18',
                        'hour_19', 'hour_20', 'hour_21', 'hour_22', 'hour_23']
        self.filename = outputfile
        csvfile = open(self.filename, 'wb')
        writer = csv.DictWriter(csvfile, fieldnames=index_set, delimiter=",")
        writer.writeheader()
        for call in self.calls_clients:
            client = self.calls_clients[call]
            writer.writerow({'label': 0,
                             'id_status': self.calls_status[call],
                             'recency': self.to_append[client]['recency'] if client in self.to_append else None,
                             'frequency': self.to_append[client]['frequency'] if client in self.to_append else None, 
                             'average': self.to_append[client]['average'] if client in self.to_append else None, 
                             'std': self.to_append[client]['std'] if client in self.to_append else None,
                             'dow_1': self.clients_dow[client][1] if client in self.clients_dow else None, 
                             'dow_2': self.clients_dow[client][2] if client in self.clients_dow else None, 
                             'dow_3': self.clients_dow[client][3] if client in self.clients_dow else None, 
                             'dow_4': self.clients_dow[client][4] if client in self.clients_dow else None, 
                             'dow_5': self.clients_dow[client][5] if client in self.clients_dow else None, 
                             'dow_6': self.clients_dow[client][6] if client in self.clients_dow else None, 
                             'dow_7': self.clients_dow[client][7] if client in self.clients_dow else None,
                             'hour_0': self.clients_hour[client][0] if client in self.clients_hour else None, 
                             'hour_1': self.clients_hour[client][1] if client in self.clients_hour else None, 
                             'hour_2': self.clients_hour[client][2] if client in self.clients_hour else None, 
                             'hour_3': self.clients_hour[client][3] if client in self.clients_hour else None, 
                             'hour_4': self.clients_hour[client][4] if client in self.clients_hour else None, 
                             'hour_5': self.clients_hour[client][5] if client in self.clients_hour else None, 
                             'hour_6': self.clients_hour[client][6] if client in self.clients_hour else None, 
                             'hour_7': self.clients_hour[client][7] if client in self.clients_hour else None, 
                             'hour_8': self.clients_hour[client][8] if client in self.clients_hour else None, 
                             'hour_9': self.clients_hour[client][9] if client in self.clients_hour else None,
                             'hour_10': self.clients_hour[client][10] if client in self.clients_hour else None, 
                             'hour_11': self.clients_hour[client][11] if client in self.clients_hour else None, 
                             'hour_12': self.clients_hour[client][12] if client in self.clients_hour else None, 
                             'hour_13': self.clients_hour[client][13] if client in self.clients_hour else None, 
                             'hour_14': self.clients_hour[client][14] if client in self.clients_hour else None, 
                             'hour_15': self.clients_hour[client][15] if client in self.clients_hour else None, 
                             'hour_16': self.clients_hour[client][16] if client in self.clients_hour else None, 
                             'hour_17': self.clients_hour[client][17] if client in self.clients_hour else None, 
                             'hour_18': self.clients_hour[client][18] if client in self.clients_hour else None,
                             'hour_19': self.clients_hour[client][19] if client in self.clients_hour else None, 
                             'hour_20': self.clients_hour[client][20] if client in self.clients_hour else None, 
                             'hour_21': self.clients_hour[client][21] if client in self.clients_hour else None, 
                             'hour_22': self.clients_hour[client][22] if client in self.clients_hour else None, 
                             'hour_23': self.clients_hour[client][23] if client in self.clients_hour else None
                             })
        
        for call in self.sales_clients:
            client = self.sales_clients[call]
            writer.writerow({'label': 1,
                             'id_status': self.sales_status[call],
                             'recency': self.to_append[client]['recency'] if client in self.to_append else None,
                             'frequency': self.to_append[client]['frequency'] if client in self.to_append else None, 
                             'average': self.to_append[client]['average'] if client in self.to_append else None, 
                             'std': self.to_append[client]['std'] if client in self.to_append else None,
                             'dow_1': self.clients_dow[client][1] if client in self.clients_dow else None, 
                             'dow_2': self.clients_dow[client][2] if client in self.clients_dow else None, 
                             'dow_3': self.clients_dow[client][3] if client in self.clients_dow else None, 
                             'dow_4': self.clients_dow[client][4] if client in self.clients_dow else None, 
                             'dow_5': self.clients_dow[client][5] if client in self.clients_dow else None, 
                             'dow_6': self.clients_dow[client][6] if client in self.clients_dow else None, 
                             'dow_7': self.clients_dow[client][7] if client in self.clients_dow else None,
                             'hour_0': self.clients_hour[client][0] if client in self.clients_hour else None, 
                             'hour_1': self.clients_hour[client][1] if client in self.clients_hour else None, 
                             'hour_2': self.clients_hour[client][2] if client in self.clients_hour else None, 
                             'hour_3': self.clients_hour[client][3] if client in self.clients_hour else None, 
                             'hour_4': self.clients_hour[client][4] if client in self.clients_hour else None, 
                             'hour_5': self.clients_hour[client][5] if client in self.clients_hour else None, 
                             'hour_6': self.clients_hour[client][6] if client in self.clients_hour else None, 
                             'hour_7': self.clients_hour[client][7] if client in self.clients_hour else None, 
                             'hour_8': self.clients_hour[client][8] if client in self.clients_hour else None, 
                             'hour_9': self.clients_hour[client][9] if client in self.clients_hour else None,
                             'hour_10': self.clients_hour[client][10] if client in self.clients_hour else None, 
                             'hour_11': self.clients_hour[client][11] if client in self.clients_hour else None, 
                             'hour_12': self.clients_hour[client][12] if client in self.clients_hour else None, 
                             'hour_13': self.clients_hour[client][13] if client in self.clients_hour else None, 
                             'hour_14': self.clients_hour[client][14] if client in self.clients_hour else None, 
                             'hour_15': self.clients_hour[client][15] if client in self.clients_hour else None, 
                             'hour_16': self.clients_hour[client][16] if client in self.clients_hour else None, 
                             'hour_17': self.clients_hour[client][17] if client in self.clients_hour else None, 
                             'hour_18': self.clients_hour[client][18] if client in self.clients_hour else None,
                             'hour_19': self.clients_hour[client][19] if client in self.clients_hour else None, 
                             'hour_20': self.clients_hour[client][20] if client in self.clients_hour else None, 
                             'hour_21': self.clients_hour[client][21] if client in self.clients_hour else None, 
                             'hour_22': self.clients_hour[client][22] if client in self.clients_hour else None, 
                             'hour_23': self.clients_hour[client][23] if client in self.clients_hour else None
                             })
        csvfile.close()
        end = timeit.default_timer()
        print('Time {}'.format(end - start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gera features relacionados a recarga.')
    # parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas -CSV file.')
    args = parser.parse_args()

    planos = Analysis(outputfile=args.output)    
    planos.run()
