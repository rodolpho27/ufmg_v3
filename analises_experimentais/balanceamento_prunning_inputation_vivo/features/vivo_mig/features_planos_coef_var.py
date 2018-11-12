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

#-----------------------------------------------------------------------------------
#IMPLEMENTACAO DAS FEATURES DE PLANOS:
## Features que calculam a razão do total gasto em recargas pelo valor dos planos.
#-----------------------------------------------------------------------------------

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
        for client, call, status in res:
            calls_clients[call] = client
            calls_status[call] = status

        return calls_clients, calls_status


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
        for plan, call, client, status in res:
            calls_plans[call] = plan
            calls_status[call] = status
            calls_clients[call] = client

        return calls_plans, calls_status, calls_clients


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

            to_append[client][type_] =   {
                                            'average': average,
                                            'std': std,
                                            'total_amount': total
                                         }


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
                to_append[client][type_.strip()] =   {
                                            'average': average,
                                            'std': std,
                                            'total_amount': total
                                         }
        return to_append

    def _get_types(self):
        query = """SELECT client_recharges.type
                    FROM files3_vivo_mig.client_recharges
                    GROUP BY client_recharges.type
                    ORDER BY client_recharges.type ASC;"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        res = res.fetchall()

        types_ = [r[0] for r in res]

        header = []
        for type_ in types_:
            if type_.strip():
                header.append(type_.strip() + '_cv')
                header.append(type_.strip() + '_ratio')

        return header, types_


    def run(self, outputfile):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.clients_plans = self._get_clients()
        self.sales_plans, self.sales_status, self.sales_clients = self._get_sales()
        self.to_append = self._get_plans()
        self.recharges = self._get_recharges()
        self.header, self.types = self._get_types()


        index_set = self.header
        self.filename = outputfile
        csvfile = open(self.filename, 'wb')
        csvfile.write('label,')
        csvfile.write('{}'.format(index_set[0]))
        for h in range(1, len(index_set)):
            csvfile.write(',{}'.format(index_set[h]))
        csvfile.write('\n')

        print(self.to_append.keys())
        for call in self.calls_clients:
            client = self.calls_clients[call]
            if client in self.clients_plans:
                plans = []
                values = []
                for plan in self.clients_plans[client]:
                    plan_ = int(plan)
                    if self.to_append[plan_]['value']:
                        plans.append(plan_)
                        values.append(self.to_append[plan_]['value'])
                min_index = np.argmin(np.asarray(values))
                min_plan = plans[min_index]
                min_value = values[min_index]
                if client in self.recharges and self.types[0] in self.recharges[client]:
                    cv = (min_value - self.recharges[client][self.types[0]]['average']) / (self.recharges[client][self.types[0]]['std'] + 1.0) if self.recharges[client][self.types[0]]['std'] else -1
                    ratio = self.recharges[client][self.types[0]]['total_amount'] / min_value if min_value > 0 else -1
                    csvfile.write('0,{},{}'.format(cv, ratio))
                else:
                    csvfile.write('0,-1,-1')
                for t in range(1, len(self.types)):
                    if client in self.recharges and self.types[t] in self.recharges[client]:
                        cv = (min_value - self.recharges[client][self.types[t]]['average']) / (self.recharges[client][self.types[t]]['std'] + 1.0) if self.recharges[client][self.types[t]]['std'] else -1
                        ratio = self.recharges[client][self.types[t]]['total_amount'] / min_value if min_value > 0 else -1
                        csvfile.write(',{},{}'.format(cv, ratio))
                    else:
                        csvfile.write(',-1,-1')
            csvfile.write('\n')

        for call in self.sales_plans:
            plan = self.sales_plans[call]
            client = self.sales_clients[call]
            # csvfile.write('#{}\n'.format(client))
            min_value = self.to_append[plan]['value']
            # if client in self.recharges:
            # 	for t in self.recharges[client]:
            # 		csvfile.write('{} {} {} {}\n'.format(self.recharges[client][t], self.recharges[client][t]['average'], self.recharges[client][t]['std'], min_value))
            
            if client in self.recharges and self.types[0] in self.recharges[client]:
                cv = (min_value - self.recharges[client][self.types[0]]['average']) / (self.recharges[client][self.types[0]]['std'] + 1.0) if self.recharges[client][self.types[0]]['std'] else -1
                ratio = self.recharges[client][self.types[0]]['total_amount'] / min_value if min_value > 0 else -1
                csvfile.write('1,{},{}'.format(cv, ratio))
            else:
                csvfile.write('1,-1,-1')
            for t in range(1, len(self.types)):
                if client in self.recharges and  self.types[t] in self.recharges[client]:
                    cv = (min_value - self.recharges[client][self.types[t]]['average']) / (self.recharges[client][self.types[t]]['std'] + 1.0) if self.recharges[client][self.types[t]]['std'] else -1
                    ratio = self.recharges[client][self.types[t]]['total_amount'] / min_value if min_value > 0 else -1
                    csvfile.write(',{},{}'.format(cv, ratio))
                else:
                    csvfile.write(',-1,-1')
            csvfile.write('\n')
        csvfile.close()
        end = timeit.default_timer()
        print('Time {}'.format(end - start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gera features relacionados que combina a informacao de recarga e planos.')
    # parser.add_argument('-db', '--database', required=True, help='Nome do banco de dados')
    parser.add_argument('-o', '--output', required=True, help='Caminho para salvar saídas -CSV file.')
    args = parser.parse_args()

    planos = Analysis()    
    planos.run(outputfile=args.output)
