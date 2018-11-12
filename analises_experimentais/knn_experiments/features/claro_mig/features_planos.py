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
##id_plan - identificador do plano
##value - valor do plano
##minutes - minutos que o plano oferece
##data - dados que o plano oferece
#-----------------------------------------------------------------------------------

class Analysis:  
    def __init__(self):
        self.engine = sqlalchemy.create_engine('mysql://root:F4c4&D4d0$@%#@localhost') #connect to server

    def _get_clients(self):
        query = """SELECT clients.id, clients.id_plan
                    FROM files3_claro_mig.clients;"""

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
                    FROM files3_claro_mig.calls
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
        query = """SELECT sales.id_plan, sales.id, sales.id_status
                    FROM files3_claro_mig.sales"""
        conn = self.engine.connect()
        res = conn.execute(text(query))
        conn.close()
        res = res.fetchall()

        calls_plans = {}
        calls_status = {}
        for plan, call, status in res:
            calls_plans[call] = plan
            calls_status[call] = status

        return calls_plans, calls_status


    def _get_plans(self):
        query = """SELECT plans.id, plans.value, plans.minutes, plans.data
                    FROM files3_claro_mig.plans;"""
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


    def run(self, outpufile):
        start = timeit.default_timer()
        self.calls_clients, self.calls_status= self._get_calls()
        self.clients_plans = self._get_clients()
        self.sales_plans, self.sales_status= self._get_sales()
        self.to_append = self._get_plans()

        index_set = ['label', 'id_plan', 'value', 'minutes', 'data']
        self.filename = outpufile
        csvfile = open(self.filename, 'wb')
        writer = csv.DictWriter(csvfile, fieldnames=index_set, delimiter=",")
        writer.writeheader()
        print(self.to_append.keys())
        for call in self.calls_clients:
            client = self.calls_clients[call]
            if client in self.clients_plans:
                for plan in self.clients_plans[client]:
                    plan_ = int(plan)
                    writer.writerow({'label': 0, 
                                     'id_plan': plan_,
                                     'value': self.to_append[plan_]['value'] if plan_ in self.to_append else None,
                                     'minutes': self.to_append[plan_]['minutes'] if plan_ in self.to_append else None,
                                     'data': self.to_append[plan_]['data'] if plan_ in self.to_append else None
                                     })
            else:
                writer.writerow({'label': 0, 
                                 'id_plan': None,
                                 'value': None,
                                 'minutes': None,
                                 'data': None
                                 })

        for call in self.sales_plans:
            plan = self.sales_plans[call]
            writer.writerow({'label': 1,
                             'id_plan': plan,
                             'value': self.to_append[plan]['value'] if plan in self.to_append else None,
                             'minutes': self.to_append[plan]['minutes'] if plan in self.to_append else None,
                             'data': self.to_append[plan]['data'] if plan in self.to_append else None
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
    planos.run(outpufile=args.output)
