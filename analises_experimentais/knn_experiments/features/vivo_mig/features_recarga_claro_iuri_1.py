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


#-----------------------------------------------------------------------------------
#IMPLEMENTACAO DAS FEATURES CLARO:
##rec_online_freq - quantidade de recargas online realizadas pelo cliente.
##rec_online_std - desvio padrao do total de recargas online realizadas pelo cliente
##rec_online_media - valor medio do total de recargas online realizado pelo cliente
##lag_2_prezao - intervalo (em dias) entre a penultima e a ultima ativacao do servico prezao realizada pelo cliente
##lag_m_prezao - intervalo medio (em dias) entre as ativacoes do servico prezao realizadas pelo cliente
##lag_std_prezao - desvio padrao do intervalo medio (em dias) entre as ativacoes do servico prezao realizadas pelo cliente
##lag_std_sos - desvio padrao do intervalo medio (em dias) entre as recargas de emergencia realizadas pelo cliente
##lag_m_sos - intervalo medio (em dias) entre as ativacoes do servico sos realizadas pelo cliente
##lag_2_sos - intervalo (em dias) entre a penultima e a ultima ativacao do servico sos realizada pelo cliente
#-----------------------------------------------------------------------------------

class Analysis:  
    def __init__(self):
        self.engine = sqlalchemy.create_engine('mysql://root:F4c4&D4d0$@%#@localhost') #connect to server

    def _get_calls(self):        
        query = """SELECT calls.id_client, calls.id, calls.id_status
                    FROM files3_vivo_mig.calls
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


    def _get_recharges_prezao(self):
        query = """SELECT client_recharges.id_client, client_recharges.date_recharge, client_recharges.value
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.calls
                    ON client_recharges.id_client = calls.id_client
                    WHERE client_recharges.type like '%Prezao%'
                    ORDER BY client_recharges.date_recharge DESC;"""
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
                                 'lag_1': lag_values[client][0] if client in lag_values and len(lag_values[client]) >= 1 else -1
                                }

        del interval_dates

        query = """SELECT client_recharges.id_client, client_recharges.date_recharge, client_recharges.value
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.sales
                    ON client_recharges.id_client = sales.id_client
                    WHERE client_recharges.type like '%Prezao%'
                    ORDER BY client_recharges.date_recharge DESC;"""
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
                                 'lag_1': lag_values[client][0] if client in lag_values and len(lag_values[client]) >= 1 else -1
                                }
        del interval_dates
        return to_append


    def _get_recharges_sos(self):
        query = """SELECT client_recharges.id_client, client_recharges.date_recharge, client_recharges.value
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.calls
                    ON client_recharges.id_client = calls.id_client
                    WHERE client_recharges.type like '%SOS%'
                    ORDER BY client_recharges.date_recharge DESC;"""
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
                                 'lag_1': lag_values[client][0] if client in lag_values and len(lag_values[client]) >= 1 else -1
                                }

        del interval_dates

        query = """SELECT client_recharges.id_client, client_recharges.date_recharge, client_recharges.value
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.sales
                    ON client_recharges.id_client = sales.id_client
                    WHERE client_recharges.type like '%SOS%'
                    ORDER BY client_recharges.date_recharge DESC;"""
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
                                 'lag_1': lag_values[client][0] if client in lag_values and len(lag_values[client]) >= 1 else -1
                                }
        del interval_dates
        return to_append


    def _get_recharges_online(self):
        query = """SELECT client_recharges.id_client, COUNT(client_recharges.id_client) AS rec_online_freq, 
                        AVG(client_recharges.value) AS rec_online_media, 
                        STDDEV(client_recharges.value)  AS rec_online_std
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.calls
                    ON client_recharges.id_client = calls.id_client
                    WHERE client_recharges.type like '%Online%'
                    GROUP BY client_recharges.id_client"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        to_append = {}

        for client, rec_online_freq, rec_online_media, rec_online_std in res:
            to_append[client] = {'rec_online_freq': rec_online_freq,
                                 'rec_online_media': rec_online_media,
                                 'rec_online_std': rec_online_std}

        query = """SELECT client_recharges.id_client, COUNT(client_recharges.id_client) AS rec_online_freq, 
                        AVG(client_recharges.value) AS rec_online_media, 
                        STDDEV(client_recharges.value)  AS rec_online_std
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.sales
                    ON client_recharges.id_client = sales.id_client
                    WHERE client_recharges.type like '%Online%'
                    GROUP BY client_recharges.id_client LIMIT 3;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        for client, rec_online_freq, rec_online_media, rec_online_std in res:
            if client not in to_append:
                to_append[client] = {'rec_online_freq': rec_online_freq,
                                     'rec_online_media': rec_online_media,
                                     'rec_online_std': rec_online_std}

        return to_append


    def run(self, outputfile):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.sales_clients, self.sales_status= self._get_sales()
        self.to_append_online = self._get_recharges_online()
        self.to_append_prezao = self._get_recharges_prezao()
        self.to_append_sos = self._get_recharges_sos()

        index_set = ['label', 'rec_online_freq', 'rec_online_media', 'rec_online_std', 'lag_2_prezao', 'lag_m_prezao', 
                        'lag_std_prezao', 'lag_2_sos', 'lag_m_sos', 'lag_std_sos']
        self.filename = outputfile
        csvfile = open(self.filename, 'wb')
        writer = csv.DictWriter(csvfile, fieldnames=index_set, delimiter=",")
        writer.writeheader()
        for call in self.calls_clients:
            client = self.calls_clients[call]
            writer.writerow({'label': 0,
                             'rec_online_freq': self.to_append_online[client]['rec_online_freq'] if client in self.to_append_online else -1,
                             'rec_online_media': self.to_append_online[client]['rec_online_media'] if client in self.to_append_online else -1,
                             'rec_online_std': self.to_append_online[client]['rec_online_std'] if client in self.to_append_online else -1,
                             'lag_2_prezao': self.to_append_prezao[client]['lag_1'] if client in self.to_append_prezao else -1,
                             'lag_m_prezao': self.to_append_prezao[client]['mean'] if client in self.to_append_prezao else -1,
                             'lag_std_prezao': self.to_append_prezao[client]['std'] if client in self.to_append_prezao else -1,
                             'lag_2_sos': self.to_append_sos[client]['lag_1'] if client in self.to_append_sos else -1,
                             'lag_m_sos': self.to_append_sos[client]['mean'] if client in self.to_append_sos else -1,
                             'lag_std_sos': self.to_append_sos[client]['std'] if client in self.to_append_sos else -1
                             })

        for call in self.sales_clients:
            client = self.sales_clients[call]
            writer.writerow({'label': 1,
                             'rec_online_freq': self.to_append_online[client]['rec_online_freq'] if client in self.to_append_online else -1,
                             'rec_online_media': self.to_append_online[client]['rec_online_media'] if client in self.to_append_online else -1,
                             'rec_online_std': self.to_append_online[client]['rec_online_std'] if client in self.to_append_online else -1,
                             'lag_2_prezao': self.to_append_prezao[client]['lag_1'] if client in self.to_append_prezao else -1,
                             'lag_m_prezao': self.to_append_prezao[client]['mean'] if client in self.to_append_prezao else -1,
                             'lag_std_prezao': self.to_append_prezao[client]['std'] if client in self.to_append_prezao else -1,
                             'lag_2_sos': self.to_append_sos[client]['lag_1'] if client in self.to_append_sos else -1,
                             'lag_m_sos': self.to_append_sos[client]['mean'] if client in self.to_append_sos else -1,
                             'lag_std_sos': self.to_append_sos[client]['std'] if client in self.to_append_sos else -1
                             })
        csvfile.close()
        end = timeit.default_timer()
        print('Time {}'.format(end - start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gera features relacionados aos planos.')
    # parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas -CSV file.')
    args = parser.parse_args()

    planos = Analysis()    
    planos.run(outputfile=args.output)
