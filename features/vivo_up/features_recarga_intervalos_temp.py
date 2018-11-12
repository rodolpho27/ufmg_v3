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
import argparse

date_format = "%Y-%m-%d"

class Analysis:  
    def __init__(self):
        self.engine = sqlalchemy.create_engine('mysql://root:F4c4&D4d0$@%#@localhost') #connect to server

    def _get_calls(self):        
        query = """SELECT calls.id_client, calls.id, calls.id_status
                    FROM files3_vivo_up.calls
                    WHERE calls.id_status NOT IN (16,17);"""
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
                    FROM files3_vivo_up.sales"""
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
        query = """SELECT client_recharges.id_client, client_recharges.date_recharge
                    FROM files3_vivo_up.client_recharges
                    INNER JOIN files3_vivo_up.calls
                    ON client_recharges.id_client = calls.id_client
                    ORDER BY client_recharges.date_recharge DESC;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        clients_dates = {}
        clients = []
        for client, date in res:
            clients.append(client)
            if date:
	            if client not in clients_dates:
	            	clients_dates[client] = []
	            date_ = datetime.strptime(date, date_format) 
	            clients_dates[client].append(date_)

        interval_dates = {}
        for client in clients_dates:
            interval_dates[client] = []
            if len(clients_dates[client]) > 1:
                for i in range(0, len(clients_dates[client])-1):
                    delta = clients_dates[client][i] - clients_dates[client][i+1]
                    interval_dates[client].append(delta.days)
            else:
                interval_dates[client].append(0)

        del clients_dates
        
        lag_values = {}
        for client in interval_dates:
            if len(interval_dates[client]) > 1:
               for v in range(len(interval_dates[client])-1):
                    value = interval_dates[client][v] / interval_dates[client][v+1] if interval_dates[client][v+1] else 0
                    if client not in lag_values:
                        lag_values[client] = []
                    lag_values[client].append(value)

        to_append = {}
        
        for client in interval_dates:
        	to_append[client] = {'lag_1': lag_values[client][0] if client in lag_values and len(lag_values[client]) >= 1 else -1, 
                                 'lag_2': lag_values[client][1] if client in lag_values and len(lag_values[client]) >= 2 else -1,
                                 'lag_3': lag_values[client][2] if client in lag_values and len(lag_values[client]) >= 3 else -1,
                                 'lag_4': lag_values[client][3] if client in lag_values and len(lag_values[client]) >= 4 else -1,
                                 'lag_5': lag_values[client][4] if client in lag_values and len(lag_values[client]) >= 5 else -1
                                }

        query = """SELECT client_recharges.id_client, client_recharges.date_recharge
                    FROM files3_vivo_up.client_recharges
                    INNER JOIN files3_vivo_up.sales
                    ON client_recharges.id_client = sales.id_client
                    ORDER BY client_recharges.date_recharge DESC;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        clients_dates = {}
        clients = []
        for client, date in res:
            clients.append(client)
            if date:
                if client not in clients_dates:
                    clients_dates[client] = []
                date_ = datetime.strptime(date, date_format) 
                clients_dates[client].append(date_)

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
        for client in interval_dates:
            if len(interval_dates[client]) > 1:
               for v in range(len(interval_dates[client])-1):
                    value = interval_dates[client][v] / interval_dates[client][v-1] if interval_dates[client][v-1] else 0
                    if client not in lag_values:
                        lag_values[client] = []
                    lag_values[client].append(value)


        for client in interval_dates:
            to_append[client] = {'lag_1': lag_values[client][0] if client in lag_values and len(lag_values[client]) >= 1 else -1, 
                                 'lag_2': lag_values[client][1] if client in lag_values and len(lag_values[client]) >= 2 else -1,
                                 'lag_3': lag_values[client][2] if client in lag_values and len(lag_values[client]) >= 3 else -1,
                                 'lag_4': lag_values[client][3] if client in lag_values and len(lag_values[client]) >= 4 else -1,
                                 'lag_5': lag_values[client][4] if client in lag_values and len(lag_values[client]) >= 5 else -1
                                }
        return to_append, interval_dates



    def run(self, outputfile):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.sales_clients, self.sales_status= self._get_sales()
        self.to_append, self.interval_dates = self._get_recharges()

        index_set = ['label', 'lag_temp_1', 'lag_temp_2', 'lag_temp_3', 'lag_temp_4', 'lag_temp_5']
        self.filename = outputfile
        csvfile = open(self.filename, 'wb')
        writer = csv.DictWriter(csvfile, fieldnames=index_set, delimiter=",")
        writer.writeheader()
        for call in self.calls_clients:
            client = self.calls_clients[call]
            writer.writerow({'label': 0,
                             'lag_temp_1': self.to_append[client]['lag_1'] if client in self.to_append else -1,
                             'lag_temp_2': self.to_append[client]['lag_2'] if client in self.to_append else -1,
                             'lag_temp_3': self.to_append[client]['lag_3'] if client in self.to_append else -1,
                             'lag_temp_4': self.to_append[client]['lag_4'] if client in self.to_append else -1,
                             'lag_temp_5': self.to_append[client]['lag_5'] if client in self.to_append else -1
                             })
        
        for call in self.sales_clients:
            client = self.sales_clients[call]
            writer.writerow({'label': 1,
                             'lag_temp_1': self.to_append[client]['lag_1'] if client in self.to_append else -1,
                             'lag_temp_2': self.to_append[client]['lag_2'] if client in self.to_append else -1,
                             'lag_temp_3': self.to_append[client]['lag_3'] if client in self.to_append else -1,
                             'lag_temp_4': self.to_append[client]['lag_4'] if client in self.to_append else -1,
                             'lag_temp_5': self.to_append[client]['lag_5'] if client in self.to_append else -1
                             })
        csvfile.close()
        end = timeit.default_timer()
        print('Time {}'.format(end - start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gera features relacionados aos intervalos temporais de recargas de clientes.')
    # parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas -CSV file.')
    args = parser.parse_args()

    planos = Analysis()    
    planos.run(outputfile=args.output)
