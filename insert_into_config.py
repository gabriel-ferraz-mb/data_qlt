# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 16:21:20 2023

@author: gabriel.ferraz
"""

# import numpy as np
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import insert
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
#from joblib import Parallel, delayed

load_dotenv()

usuario = os.getenv('USER')
senha = os.getenv('PASSWORD')
host = os.getenv('HOST')
porta= os.getenv('PORT')
banco = os.getenv('DATABASE')

global engine
connection_str = f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}'
engine = create_engine(connection_str)

cult = 'milho safrinha'
pesq = 'mercado'
q = ''
cs = 'seeds'

colnames_dict =  {'CULTURA': 'Crop (S)',
 'ANO': 'Year, harvest (S)',
 'SUBSAFRA': 'Harvest time (S)',
 'VALOR_DE_MERCADO_MI_USD': 'Turnover excl. VAT ($ mio)',
 'ESTADO': 'Land (S)',
 'VOLUME_KG_SEMENTE': 'Tonnage (1000t)',
 'AREA_CULTIVADA_SEMENTE': 'Cultivated area sum 1000 ha (TCA)',
 'VARIEDADE_HIBRIDO': 'Variety (S)',
 'SEMENTE_TXT': 'Variety (S).1',
 'VALOR_DE_MERCADO_MI_EUR': 'Turnover excl. VAT (€ mio)',
 'EMPRESA_PRODUTORA_DO_HÍBRIDO': 'Distributor (S)',
 'DISTRIBUIDOR_TXT': 'Distributor (S).1'}

# =============================================================================
# Insert Query
# =============================================================================


stmt = insert('config.select_query_indicadores').values(cultura=cult, pesquisa=pesq, query=q)

with engine.connect() as conn:
    result = conn.execute(stmt)
    conn.commit()
    conn.close()
# =============================================================================
# Insert Dict     
# =============================================================================
insert_list = []

for k, v in colnames_dict.items():
    d = {'cultura':cult, 'pesquisa':pesq, 'cpp_seeds' : cs, 'chave':k, 'valor':v }
    insert_list.append(d)

conn = psycopg2.connect(connection_str)
cursor = conn.cursor()

insert_sql = f"INSERT INTO config.dictionary_translator ({', '.join(insert_list[0].keys())}) VALUES %s"

values = [tuple(row.values()) for row in insert_list]

execute_values(cursor, insert_sql, values)

conn.commit()

cursor.close()
conn.close()

