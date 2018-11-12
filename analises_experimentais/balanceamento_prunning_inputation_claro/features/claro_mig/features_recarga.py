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

class AnalysisRecharge:  
    def __init__(self, db='files3_claro_mig'):
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
                    FROM {}.sales;""".format(self.db)
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
                        STDDEV(client_recharges.value)  AS std_value, SUM(client_recharges.value) as total_amount
                    FROM {}.client_recharges
                    INNER JOIN {}.calls
                    ON client_recharges.id_client = calls.id_client
                    GROUP BY client_recharges.id_client;""".format(self.db, self.db)
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        to_append = {}
        clients = []
        for client, recency, freq, average, std, total in res:
            clients.append(client)
            to_append[client] =   {
                                'id_client': client,
                                'recency': recency,
                                'frequency': freq,
                                'average': average,
                                'std': std,
                                'total_amount': total
                            }


        query = """SELECT client_recharges.id_client, DATEDIFF(CURRENT_DATE(), MAX(client_recharges.date_recharge)) as recency, 
                        COUNT(1) as frequency,
                        AVG(client_recharges.value) AS average_value, 
                        STDDEV(client_recharges.value)  AS std_value, SUM(client_recharges.value) as total_amount
                    FROM {}.client_recharges
                    INNER JOIN {}.sales
                    ON client_recharges.id_client = sales.id_client
                    GROUP BY client_recharges.id_client;""".format(self.db, self.db)
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        for client, recency, freq, average, std, totals in res:
            clients.append(client)
            to_append[client] =   {
                                'id_client': client,
                                'recency': recency,
                                'frequency': freq,
                                'average': average,
                                'std': std,
                                'total_amount': total
                            }
        return to_append


    def _get_recharges_dow(self):
        query = """SELECT client_recharges.id_client, DAYOFWEEK(client_recharges.date_recharge) AS dow, COUNT(1) AS frequency 
                       FROM {}.client_recharges 
                       INNER JOIN {}.calls
                       ON calls.id_client = client_recharges.id_client
                       GROUP BY client_recharges.id_client, dow;""".format(self.db, self.db)
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
                       FROM {}.client_recharges 
                       INNER JOIN {}.sales
                       ON sales.id_client = client_recharges.id_client
                       GROUP BY client_recharges.id_client, dow;""".format(self.db, self.db)
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
                       FROM {}.client_recharges
                       INNER JOIN {}.calls
                       ON calls.id_client = client_recharges.id_client
                       GROUP BY client_recharges.id_client, hour;""".format(self.db, self.db)
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
                       FROM {}.client_recharges
                       INNER JOIN {}.sales
                       ON sales.id_client = client_recharges.id_client
                       GROUP BY client_recharges.id_client, hour;""".format(self.db, self.db)
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

        index_set = ['label', 'recency', 'frequency', 'average', 'std', 'total_amount',
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
                             'recency': self.to_append[client]['recency'] if client in self.to_append else None,
                             'frequency': self.to_append[client]['frequency'] if client in self.to_append else None, 
                             'average': self.to_append[client]['average'] if client in self.to_append else None, 
                             'std': self.to_append[client]['std'] if client in self.to_append else None,
                             'total_amount': self.to_append[client]['total_amount'] if client in self.to_append else None,
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
                             'recency': self.to_append[client]['recency'] if client in self.to_append else None,
                             'frequency': self.to_append[client]['frequency'] if client in self.to_append else None, 
                             'average': self.to_append[client]['average'] if client in self.to_append else None, 
                             'std': self.to_append[client]['std'] if client in self.to_append else None,
                             'total_amount': self.to_append[client]['total_amount'] if client in self.to_append else None,
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

    def gen_features(self):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.sales_clients, self.sales_status= self._get_sales()
        self.to_append = self._get_recharges()
        self.clients_dow = self._get_recharges_dow()
        self.clients_hour = self._get_recharges_hour()

        x = np.empty([len(self.calls_clients) + len(self.sales_clients), 36])
        index = 0
        for call in self.calls_clients:
            client = self.calls_clients[call]
            x[index][0] = self.to_append[client]['recency'] if client in self.to_append else None
            x[index][1] = self.to_append[client]['frequency'] if client in self.to_append else None
            x[index][2] = self.to_append[client]['average'] if client in self.to_append else None
            x[index][3] = self.to_append[client]['std'] if client in self.to_append else None
            x[index][4] = self.to_append[client]['total_amount'] if client in self.to_append else None
            if client in self.clients_dow:
                for i in range(len(self.clients_dow[client])):
                    x[index][i + 5] = self.clients_dow[client][i] if i in self.clients_dow[client] else None
            else:
                for i in range(7):
                    x[index][i + 5] = None
            if client in self.clients_hour:
                for i in range(24):
                    x[index][i + 12] = self.clients_hour[client][i] if i in self.clients_hour[client] else None
            else:
                for i in range(24):
                    x[index][i + 12] = None
            index += 1
        
        for call in self.sales_clients:
            client = self.sales_clients[call]
            x[index][0] = self.to_append[client]['recency'] if client in self.to_append else None
            x[index][1] = self.to_append[client]['frequency'] if client in self.to_append else None
            x[index][2] = self.to_append[client]['average'] if client in self.to_append else None
            x[index][3] = self.to_append[client]['std'] if client in self.to_append else None
            x[index][4] = self.to_append[client]['total_amount'] if client in self.to_append else None
            if client in self.clients_dow:
                for i in range(len(self.clients_dow[client])):
                    x[index][i + 5] = self.clients_dow[client][i] if i in self.clients_dow[client] else None
            else:
                for i in range(7):
                    x[index][i + 5] = None
            if client in self.clients_hour:
                for i in range(24):
                    x[index][i + 12] = self.clients_hour[client][i] if i in self.clients_hour[client] else None
            else:
                for i in range(24):
                    x[index][i + 12] = None
            index += 1

        end = timeit.default_timer()
        print('Time {}'.format(end - start))
        return x


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gera features relacionados a recarga.')
    # parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saidas -CSV file.')
    args = parser.parse_args()

    planos = AnalysisRecharge()
    planos.run(outputfile=args.output)
