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
##media_freq_recarga
##desvio_freq_recarga
##lag1-lag10
#-----------------------------------------------------------------------------------

class AnalysisRechargeLagVivo:  
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


    def _get_recharges(self):
        query = """SELECT client_recharges.id_client, client_recharges.date_recharge, client_recharges.value
                    FROM {}.client_recharges
                    INNER JOIN {}.calls
                    ON client_recharges.id_client = calls.id_client
                    ORDER BY client_recharges.date_recharge DESC;""".format(self.db, self.db)
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        clients_dates = {}
        clients_values = {}
        clients = []
        for client, date, value in res:
            clients.append(client)
            if date:
                if client not in clients_dates:
                    clients_dates[client] = []
                    clients_values[client] = []
                date_ = datetime.strptime(date, date_format) 
                clients_dates[client].append(date_)
                clients_values[client].append(value)

        interval_dates = {}
        for client in clients_dates:
            interval_dates[client] = []
            if len(clients_dates[client]) > 1:
                for i in range(0, len(clients_dates[client])-1):
                    delta = clients_dates[client][i+1] - clients_dates[client][i]
                    interval_dates[client].append(delta.days)
            else:
                interval_dates[client].append(0)

        del clients_dates
        
        lag_values = {}
        for client in clients_values:
            if len(clients_values[client]) > 1:
               for v in range(len(clients_values[client])-1):
                    value = clients_values[client][v] / clients_values[client][v+1] if clients_values[client][v+1] else 0
                    if client not in lag_values:
                        lag_values[client] = []
                    lag_values[client].append(value)

        del clients_values

        to_append = {}
        
        for client in lag_values:
        	to_append[client] = {'mean': np.mean(np.array(interval_dates[client])),
                                 'std': np.std(np.array(interval_dates[client])),
                                 'lag_1': lag_values[client][0] if client in lag_values and len(lag_values[client]) >= 1 else -1, 
                                 'lag_2': lag_values[client][1] if client in lag_values and len(lag_values[client]) >= 2 else -1,
                                 'lag_3': lag_values[client][2] if client in lag_values and len(lag_values[client]) >= 3 else -1,
                                 'lag_4': lag_values[client][3] if client in lag_values and len(lag_values[client]) >= 4 else -1,
                                 'lag_5': lag_values[client][4] if client in lag_values and len(lag_values[client]) >= 5 else -1,
                                 'lag_6': lag_values[client][5] if client in lag_values and len(lag_values[client]) >= 6 else -1,
                                 'lag_7': lag_values[client][6] if client in lag_values and len(lag_values[client]) >= 7 else -1,
                                 'lag_8': lag_values[client][7] if client in lag_values and len(lag_values[client]) >= 8 else -1,
                                 'lag_9': lag_values[client][8] if client in lag_values and len(lag_values[client]) >= 9 else -1,
                                 'lag_10': lag_values[client][9] if client in lag_values and len(lag_values[client]) >= 10 else -1,
                                }

        del interval_dates

        query = """SELECT client_recharges.id_client, client_recharges.date_recharge, client_recharges.value
                    FROM {}.client_recharges
                    INNER JOIN {}.sales
                    ON client_recharges.id_client = sales.id_client
                    ORDER BY client_recharges.date_recharge DESC;""".format(self.db, self.db)
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        clients_dates = {}
        clients_values = {}
        clients = []
        for client, date, value in res:
            clients.append(client)
            if date:
                if client not in clients_dates:
                    clients_dates[client] = []
                    clients_values[client] = []
                date_ = datetime.strptime(date, date_format) 
                clients_dates[client].append(date_)
                clients_values[client].append(value)

        interval_dates = {}
        for client in clients_dates:
            interval_dates[client] = []
            if len(clients_dates[client]) > 1:
                for i in range(0, len(clients_dates[client])-1):
                    delta = clients_dates[client][i+1] - clients_dates[client][i]
                    interval_dates[client].append(delta.days)
            else:
                interval_dates[client].append(0)

        del clients_dates
        
        lag_values = {}
        for client in clients_values:
            if len(clients_values[client]) > 1:
               for v in range(len(clients_values[client])-1):
                    value = clients_values[client][v] / clients_values[client][v-1] if clients_values[client][v-1] else 0
                    if client not in lag_values:
                        lag_values[client] = []
                    lag_values[client].append(value)

        del clients_values

        for client in lag_values:
            to_append[client] = {'mean': np.mean(np.array(interval_dates[client])),
                                 'std': np.std(np.array(interval_dates[client])),
                                 'lag_1': lag_values[client][0] if client in lag_values and len(lag_values[client]) >= 1 else -1, 
                                 'lag_2': lag_values[client][1] if client in lag_values and len(lag_values[client]) >= 2 else -1,
                                 'lag_3': lag_values[client][2] if client in lag_values and len(lag_values[client]) >= 3 else -1,
                                 'lag_4': lag_values[client][3] if client in lag_values and len(lag_values[client]) >= 4 else -1,
                                 'lag_5': lag_values[client][4] if client in lag_values and len(lag_values[client]) >= 5 else -1,
                                 'lag_6': lag_values[client][5] if client in lag_values and len(lag_values[client]) >= 6 else -1,
                                 'lag_7': lag_values[client][6] if client in lag_values and len(lag_values[client]) >= 7 else -1,
                                 'lag_8': lag_values[client][7] if client in lag_values and len(lag_values[client]) >= 8 else -1,
                                 'lag_9': lag_values[client][8] if client in lag_values and len(lag_values[client]) >= 9 else -1,
                                 'lag_10': lag_values[client][9] if client in lag_values and len(lag_values[client]) >= 10 else -1,
                                }
        return to_append, interval_dates



    def run(self):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.sales_clients, self.sales_status= self._get_sales()
        self.to_append, self.interval_dates = self._get_recharges()

        index_set = ['label', 'mean', 'std', 'lag_1', 'lag_2', 'lag_3', 'lag_4', 'lag_5', 'lag_6', 'lag_7', 'lag_8', 'lag_9', 'lag_10']
        self.filename = 'features_recharge_lag_vivo_iuri.csv'
        csvfile = open(self.filename, 'wb')
        writer = csv.DictWriter(csvfile, fieldnames=index_set, delimiter=",")
        writer.writeheader()

        for call in self.calls_clients:
            client = self.calls_clients[call]
            writer.writerow({'label': 0,
                             'mean': self.to_append[client]['mean'] if client in self.to_append else -1,
                             'std': self.to_append[client]['std'] if client in self.to_append else -1,
                             'lag_1': self.to_append[client]['lag_1'] if client in self.to_append else -1,
                             'lag_2': self.to_append[client]['lag_2'] if client in self.to_append else -1,
                             'lag_3': self.to_append[client]['lag_3'] if client in self.to_append else -1,
                             'lag_4': self.to_append[client]['lag_4'] if client in self.to_append else -1,
                             'lag_5': self.to_append[client]['lag_5'] if client in self.to_append else -1,
                             'lag_6': self.to_append[client]['lag_6'] if client in self.to_append else -1,
                             'lag_7': self.to_append[client]['lag_7'] if client in self.to_append else -1,
                             'lag_8': self.to_append[client]['lag_8'] if client in self.to_append else -1,
                             'lag_9': self.to_append[client]['lag_9'] if client in self.to_append else -1,
                             'lag_10': self.to_append[client]['lag_10'] if client in self.to_append else -1
                             })

        for call in self.sales_clients:
            client = self.sales_clients[call]
            writer.writerow({'label': 1,
                             'mean': self.to_append[client]['mean'] if client in self.to_append else -1,
                             'std': self.to_append[client]['std'] if client in self.to_append else -1,
                             'lag_1': self.to_append[client]['lag_1'] if client in self.to_append else -1,
                             'lag_2': self.to_append[client]['lag_2'] if client in self.to_append else -1,
                             'lag_3': self.to_append[client]['lag_3'] if client in self.to_append else -1,
                             'lag_4': self.to_append[client]['lag_4'] if client in self.to_append else -1,
                             'lag_5': self.to_append[client]['lag_5'] if client in self.to_append else -1,
                             'lag_6': self.to_append[client]['lag_6'] if client in self.to_append else -1,
                             'lag_7': self.to_append[client]['lag_7'] if client in self.to_append else -1,
                             'lag_8': self.to_append[client]['lag_8'] if client in self.to_append else -1,
                             'lag_9': self.to_append[client]['lag_9'] if client in self.to_append else -1,
                             'lag_10': self.to_append[client]['lag_10'] if client in self.to_append else -1
                             })
        end = timeit.default_timer()
        print('Time {}'.format(end - start))


    def gen_features(self):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.sales_clients, self.sales_status= self._get_sales()
        self.to_append, self.interval_dates = self._get_recharges()

        x = np.empty([len(self.calls_clients) + len(self.sales_clients), 12])
        index = 0
        for call in self.calls_clients:
            client = self.calls_clients[call]
            x[index][0] = self.to_append[client]['mean'] if client in self.to_append else -1
            x[index][1] = self.to_append[client]['std'] if client in self.to_append else -1
            x[index][2] = self.to_append[client]['lag_1'] if client in self.to_append else -1
            x[index][3] = self.to_append[client]['lag_2'] if client in self.to_append else -1
            x[index][4] = self.to_append[client]['lag_3'] if client in self.to_append else -1
            x[index][5] = self.to_append[client]['lag_4'] if client in self.to_append else -1
            x[index][6] = self.to_append[client]['lag_5'] if client in self.to_append else -1
            x[index][7] = self.to_append[client]['lag_6'] if client in self.to_append else -1
            x[index][8] = self.to_append[client]['lag_7'] if client in self.to_append else -1
            x[index][9] = self.to_append[client]['lag_8'] if client in self.to_append else -1
            x[index][10] = self.to_append[client]['lag_9'] if client in self.to_append else -1
            x[index][11] = self.to_append[client]['lag_10'] if client in self.to_append else -1
            index += 1

        for call in self.sales_clients:
            client = self.sales_clients[call]
            x[index][0] = self.to_append[client]['mean'] if client in self.to_append else -1
            x[index][1] = self.to_append[client]['std'] if client in self.to_append else -1
            x[index][2] = self.to_append[client]['lag_1'] if client in self.to_append else -1
            x[index][3] = self.to_append[client]['lag_2'] if client in self.to_append else -1
            x[index][4] = self.to_append[client]['lag_3'] if client in self.to_append else -1
            x[index][5] = self.to_append[client]['lag_4'] if client in self.to_append else -1
            x[index][6] = self.to_append[client]['lag_5'] if client in self.to_append else -1
            x[index][7] = self.to_append[client]['lag_6'] if client in self.to_append else -1
            x[index][8] = self.to_append[client]['lag_7'] if client in self.to_append else -1
            x[index][9] = self.to_append[client]['lag_8'] if client in self.to_append else -1
            x[index][10] = self.to_append[client]['lag_9'] if client in self.to_append else -1
            x[index][11] = self.to_append[client]['lag_10'] if client in self.to_append else -1
            index += 1 
        end = timeit.default_timer()
        print('Time {}'.format(end - start))
        return x

if __name__ == '__main__':
    planos = AnalysisRechargeLagVivo()    
    planos.run()
