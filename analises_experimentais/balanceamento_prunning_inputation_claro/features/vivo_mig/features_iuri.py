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

#-----------------------------------------------------------------------------------
#IMPLEMENTAÇÃO DAS FEATURES CLARO:
# $10
# $13
# $15
# $20
# plano_valor
# plano_minutos
# plano_dados
# lag_m_recarga
# lag_2_recarga
# lag_2_pct
#-----------------------------------------------------------------------------------

date_format = "%Y-%m-%d"

class Analysis:  
    def __init__(self):
        self.engine = sqlalchemy.create_engine('mysql://root:F4c4&D4d0$@%#@localhost') #connect to server

    def _get_clients(self):
        query = """SELECT clients.id, clients.id_plan
                    FROM files3_vivo_mig.clients;"""

        conn = self.engine.connect()
        res = conn.execute(text(query))
        conn.close()
        res = res.fetchall()

        clients_plans = {}
        for client, plans in res:
            if plans:
                clients_plans[client] = plans.split(',')
        return clients_plans


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
        calls = set()
        for client, call, status in res:
            calls.add(call)
            calls_clients[call] = client
            calls_status[call] = status
        return list(calls), calls_clients, calls_status


    def _get_sales(self):        
        query = """SELECT sales.id_plan, sales.id, sales.id_client, sales.id_status
                    FROM files3_vivo_mig.sales"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        conn.close()
        res = res.fetchall()

        calls_plans = {}
        calls_status = {}
        calls_clients = {}
        sales = set()
        for plan, call, client, status in res:
            calls_plans[call] = plan
            calls_status[call] = status
            calls_clients[call] = client
            sales.add(call)

        return sales, calls_plans, calls_status, calls_clients


    def _get_plans(self):
        query = """SELECT plans.id, plans.value, plans.minutes, plans.data
                    FROM files3_vivo_mig.plans;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        to_append = {}
        for plan, value, minutes, data in res:
            if minutes:
                if minutes.lower().strip() == 'ilimitado':
                    minutes_ = 1000
                else:
                    minutes_ = ''
                    for c in minutes:
                        if c.isdigit():
                            minutes_ += c
            else:
                minutes_ = None
            if data:
                data = data.replace('500', '0.5')
                data = data.replace('700', '0.7')
                data = data.replace('800', '0.8')
                data = data.replace('512Kbps', '0.000512')
                data = data.replace('1MB', '0.001')
                data = data.replace('2MB', '0.002')
                data_values = []
                data_split = data.split('+')
                for d in data_split:
                    aux = ''
                    for c in d:
                        if c.isdigit():
                            aux += c
                        elif c == '.' or c == ',':
                            aux += '.'
                    data_values.append(float(aux))
                data_ = sum(data_values)
            else:
                data_ = None

            plan_ = int(plan)
            to_append[plan_] =   {
                                'value': value,
                                'minutes': float(minutes_) if minutes_ else None,
                                'data': float(data_) if data_ else None
                            }


        
        return to_append

    def _get_recharges(self):
        query = """SELECT client_recharges.id_client, client_recharges.type, client_recharges.value, client_recharges.date_recharge
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.calls
                    ON client_recharges.id_client = calls.id_client
                    ORDER BY client_recharges.date_recharge DESC;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        to_append = {}
        clients_dates = {}
        for client, type_, value, date in res:
            if client not in to_append:
                to_append[client] = {'fifteen': 0, 'twenty': 0, 'thirteen': 0, 'ten': 0}
            if value:
                if int(value) == 15:
                    to_append[client]['fifteen'] += 1
                if int(value) == 20:
                    to_append[client]['twenty'] += 1
                if int(value) == 13:    
                    to_append[client]['thirteen'] += 1
                if int(value) == 10:    
                    to_append[client]['ten'] += 1
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

        return to_append, interval_dates
            

    def _get_recharges_summarized(self):
        query = """SELECT client_recharges.id_client, client_recharges.type, AVG(client_recharges.value) AS average_value, 
                        STDDEV(client_recharges.value)  AS std_value, SUM(client_recharges.value) as total_amount
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.calls
                    ON client_recharges.id_client = calls.id_client
                    GROUP BY client_recharges.id_client, client_recharges.type;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        to_append = {}
        clients = []
        for client, type_, average, std, total in res:
            clients.append(client)
            if client not in to_append:
                to_append[client] = {}

            to_append[client][type_] =   {'average': average,
                                          'std': std,
                                          'total_amount': total}


        query = """SELECT client_recharges.id_client, client_recharges.type, AVG(client_recharges.value) AS average_value, 
                        STDDEV(client_recharges.value)  AS std_value, SUM(client_recharges.value) as total_amount
                    FROM files3_vivo_mig.client_recharges
                    INNER JOIN files3_vivo_mig.sales
                    ON client_recharges.id_client = sales.id_client
                    GROUP BY client_recharges.id_client, client_recharges.type;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        for client, type_, average, std, total in res:
            clients.append(client)
            if client not in to_append:
                to_append[client] = {}
            if type_.strip():
                to_append[client][type_.strip()] =   {'average': average,
                                                      'std': std,
                                                      'total_amount': total}
        return to_append


    def run(self, outputfile):
        print('Get calls...')
        start = timeit.default_timer()
        self.calls, self.calls_clients, self.calls_status= self._get_calls()
        end = timeit.default_timer()
        print('Time {}'.format(end-start))

        print('Get clients...')
        start = timeit.default_timer()
        self.clients_plans = self._get_clients()
        end = timeit.default_timer()
        print('Time {}'.format(end-start))

        print('Get clients...')
        start = timeit.default_timer()
        self.sales, self.sales_plans, self.sales_status, self.sales_clients = self._get_sales()
        end = timeit.default_timer()
        print('Time {}'.format(end-start))

        print('Get clients...')
        start = timeit.default_timer()
        self.to_append = self._get_plans()
        end = timeit.default_timer()
        print('Time {}'.format(end-start))

        print('Get clients...')
        start = timeit.default_timer()
        self.count_recharges, self.interval_dates = self._get_recharges()
        end = timeit.default_timer()
        print('Time {}'.format(end-start))

        print('Fill csv files...')
        start = timeit.default_timer()
        index_set = ['label','plano_valor', 'plano_minutos', 'plano_dados', '$15', '$20', '$13', '$10', 'lag_m_recarga', 'lag_2_recarga', 'lag_2_pct', 'recarga retomada']
        self.filename = 'features_calls_iuri.csv'
        csvfile = open(self.filename, 'wb')
        csvfile.write('{}'.format(index_set[0]))
        for h in range(1, len(index_set)):
            csvfile.write(',{}'.format(index_set[h]))
        csvfile.write('\n')

        print(self.to_append.keys())
        for call in self.calls:
            client = self.calls_clients[call]
            if client in self.clients_plans:
                plans = []
                client_values = []
                client_minutes = []
                client_data = []
                for plan in self.clients_plans[client]:
                    plan_ = int(plan)
                    if self.to_append[plan_]['value']:
                        plans.append(plan_)
                        client_values.append(self.to_append[plan_]['value'])
                    if self.to_append[plan_]['minutes']:
                        client_values.append(self.to_append[plan_]['minutes'])
                    if self.to_append[plan_]['data']:
                        client_data.append(self.to_append[plan_]['data'])
                if len(client_values) > 0:
                    client_values.append(-1)
                if len(client_minutes) > 0:
                    client_minutes.append(-1)
                if len(client_data) > 0:
                    client_data.append(-1)    
                csvfile.write('0,{},{},{}'.format(np.mean(np.asarray(client_values)) if client_values else -1, 
                                                np.mean(np.asarray(client_minutes)) if client_minutes else -1, 
                                                np.mean(np.asarray(client_data)) if client_data else -1))
            else:
                csvfile.write('0,-1,-1,-1')
            if client in self.count_recharges:
                csvfile.write(',{},{},{},{},{},{},{}'.format(self.count_recharges[client]['fifteen'], self.count_recharges[client]['twenty'], 
                                                    self.count_recharges[client]['thirteen'], self.count_recharges[client]['ten'],
                                                    np.mean(np.asarray(self.interval_dates[client])),
                                                    self.interval_dates[client][0] if client in self.interval_dates and len(self.interval_dates[client]) >= 1 else -1,
                                                    self.interval_dates[client][0]+self.interval_dates[client][1] if client in self.interval_dates and len(self.interval_dates[client]) >= 2 else -1,
                                                    1 if len([x for x in np.asarray(self.interval_dates[client])==60 if x == True]) > 0 else 0))
            else:
                csvfile.write(',-1,-1,-1,-1,-1,-1,-1')
            csvfile.write('\n')
        
        for call in self.sales:
            client = self.sales_clients[call]
            if client in self.clients_plans:
                plans = []
                client_values = []
                client_minutes = []
                client_data = []
                for plan in self.clients_plans[client]:
                    plan_ = int(plan)
                    if self.to_append[plan_]['value']:
                        plans.append(plan_)
                        client_values.append(self.to_append[plan_]['value'])
                    if self.to_append[plan_]['minutes']:
                        client_values.append(self.to_append[plan_]['minutes'])
                    if self.to_append[plan_]['data']:
                        client_data.append(self.to_append[plan_]['data'])
                if len(client_values) > 0:
                    client_values.append(-1)
                if len(client_minutes) > 0:
                    client_minutes.append(-1)
                if len(client_data) > 0:
                    client_data.append(-1)    
                csvfile.write('1,{},{},{}'.format(np.mean(np.asarray(client_values)) if client_values else -1, 
                                                np.mean(np.asarray(client_minutes)) if client_minutes else -1, 
                                                np.mean(np.asarray(client_data)) if client_data else -1))
            else:
                csvfile.write('1,-1,-1,-1')
            if client in self.count_recharges:
                csvfile.write(',{},{},{},{},{},{},{}'.format(self.count_recharges[client]['fifteen'], self.count_recharges[client]['twenty'], 
                                                    self.count_recharges[client]['thirteen'], self.count_recharges[client]['ten'],
                                                    np.mean(np.asarray(self.interval_dates[client])),
                                                    self.interval_dates[client][0] if client in self.interval_dates and len(self.interval_dates[client]) >= 1 else -1,
                                                    self.interval_dates[client][0]+self.interval_dates[client][1] if client in self.interval_dates and len(self.interval_dates[client]) >= 2 else -1,
                                                    1 if len([x for x in np.asarray(self.interval_dates[client])==60 if x == True]) > 0 else 0))
            else:
                csvfile.write(',-1,-1,-1,-1,-1,-1,-1')
            csvfile.write('\n')        
        csvfile.close()
        end = timeit.default_timer()
        print('Time {}'.format(end - start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gera features relacionados planos.')
    # parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas -CSV file.')
    args = parser.parse_args()

    planos = Analysis()    
    planos.run(outputfile=args.output)
