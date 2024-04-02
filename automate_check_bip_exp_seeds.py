
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 16:15:47 2023

automated_chcek_bib_exp_seeds.py

This script is used to: Apply automated test to Explorer DB Seeds compared to BIP Seeds

 
@author: gabriel.ferraz

Updated on Wed Dec  6 11:47:47 2023
"""
'''  BLOCO I

Libs import, dataset read and pre-processment

'''
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from pandas import ExcelWriter
# import numpy as np
from sqlalchemy import create_engine
#from joblib import Parallel, delayed
pd.options.mode.chained_assignment = None

load_dotenv()
usuario = os.getenv('USER')
senha = os.getenv('PASSWORD')
host = os.getenv('HOST')
porta= os.getenv('PORT')
banco = os.getenv('DATABASE')

global engine
connection_str = f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}'
engine = create_engine(connection_str)

conn = psycopg2.connect(connection_str)
cursor = conn.cursor()
cursor.execute("SELECT distinct(cultura) FROM config.dictionary_translator;")
rows = cursor.fetchall()
available = [i[0] for i in rows]
print("Culturas disponíveis: " + str(available), end='\n------------------------------\n') 
conn.close()

global cultura
user= input("Nome do usuário: ")
print('\n------------------------------\n')
cultura  = input("Cultura escolhida: ")
if cultura not in str(rows[0]):
    print("Cultura ainda não está cadastrada. Favor escolher uma das seguintes culturas: "+ str(available),end='\n------------------------------\n')
# print('\n------------------------------\n')


def save_xls(list_dfs, xls_path):
    with ExcelWriter(xls_path) as writer:
        for n, df in enumerate(list_dfs):
            df.to_excel(writer,'sheet%s' % n)



def get_dict(cult, pesq, cs):
    conn = psycopg2.connect(connection_str)
    cursor = conn.cursor()
    cursor.execute("SELECT chave,valor FROM config.dictionary_translator where cultura = '{0}' and pesquisa = '{1}' and cpp_seeds = '{2}';".format(cultura, pesq, cs))
    rows = cursor.fetchall()
    result_dict = {row[0]: row[1] for row in rows} 
    conn.close()
    return result_dict

def generate_comparison_seeds(data, rm_list, bip_data, merge_by):
    
    # data = exp_seeds
    # rm_list =rm_total
    # bip_data = grouped_bip
    # merge_by = mb_total    
    
    seeds_colnames_list= list(seeds_dict.values())
    seeds_colnames_list = set(seeds_colnames_list) - set(rm_list)

    data_c = data.copy()
    exp_seeds_subset = data_c.loc[:, seeds_colnames_list]
    bip_input =  bip_data.loc[:, seeds_colnames_list]
    
    merged_df_seeds = pd.merge(exp_seeds_subset, bip_input, on=merge_by, suffixes=('_exp', '_bip'), how = 'right')

    common_columns_seeds = set(exp_seeds_subset.columns) - set(merge_by)

    for column in common_columns_seeds:
        exp_col = f'{column}_exp'
        bip_col = f'{column}_bip'
        merged_df_seeds[column] = merged_df_seeds[bip_col] / merged_df_seeds[exp_col]

    # Drop unnecessary columns
    result_df = merged_df_seeds[merge_by + list(common_columns_seeds)]
    return result_df

# =============================================================================
# PRODUTO - Apply comparison on product segmented data
# =============================================================================
def generate_comparison_code_seeds(data, rm_list, bip_data, merge_by, code_var, txt_var, dictionary):
    
    # data = exp_produto_seeds
    # rm_list =rm_produto
    # bip_data = grouped_bip_produto
    # merge_by = mb_produto 
    # code_var = code_var_produto
    # txt_var = txt_var_produto
    # dictionary = produto_dict
    
    seeds_colnames_list= list(seeds_dict.values())
    seeds_colnames_list = set(seeds_colnames_list) - set(rm_list)

    data_c = data.copy()
    exp_seeds_subset = data_c.loc[:, seeds_colnames_list]
    exp_seeds_subset[code_var] = exp_seeds_subset[code_var].astype(int).astype(str)
    
    seeds_colnames_list.remove(txt_var)
    bip_input =  bip_data.loc[:, seeds_colnames_list]
    bip_input.replace({code_var:dictionary}, inplace = True)

    merged_df_seeds = pd.merge(exp_seeds_subset, bip_input, on=merge_by, suffixes=('_exp', '_bip'), how = 'outer')
    
    txt_list = merge_by.copy()
    txt_list.append(txt_var)
    
    common_columns_seeds = list(set(exp_seeds_subset.columns) - set(txt_list))

    for column in common_columns_seeds:
        exp_col = f'{column}_exp'
        bip_col = f'{column}_bip'
        merged_df_seeds[column] = merged_df_seeds[bip_col] / merged_df_seeds[exp_col]

    # Drop unnecessary columns
    result_df = merged_df_seeds[merge_by + list(common_columns_seeds)]
    result_df_txt = pd.merge(exp_seeds_subset[[code_var, txt_var]], result_df, on=code_var, how = 'right')
    return result_df_txt

def main_seeds() -> None:

    path = r'C:\Users\{}\OneDrive - Kynetec\Documentos - Crop Subscription BR\INTERNO\TRANSFERÊNCIA\INTEGRAÇÃO - BIP E FARMTRAK\5. CHECAGEM RESULTADOS EXPLORER'.format(user)
    
    
    # bip_seeds = None
    for file in os.listdir(path+r'\{}'.format(cultura)):
        if file.startswith("SMER"):
            if file.lower().endswith(".csv"):
                try:
                    bip_seeds = pd.read_csv(path+r'\{0}\{1}'.format(cultura,file), sep = ';', encoding='iso-8859-1', decimal=',')
                except Exception:
                    print('Erro ao ler BIP')
            elif file.lower().endswith('.xlsx'):
                try:
                    bip_seeds = pd.read_excel(path+r'\{0}\{1}'.format(cultura,file))
                except Exception:
                    print('Erro ao ler BIP')
    
    
    produto_codes = pd.read_excel(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\bip 2023\@SPARK BASE\SPARK_BASE_INTEGRAÇÃO.xlsx', sheet_name='SEMENTES')
    produto_dict = pd.Series(produto_codes.FILTRO_STAMM.values.astype(int).astype(str),index=produto_codes.TXT_SEMENTE).to_dict()
    
    empresa_codes = pd.read_excel(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\bip 2023\@SPARK BASE\SPARK_BASE_INTEGRAÇÃO.xlsx', sheet_name='EMPRESA')
    empresa_dict = pd.Series(empresa_codes['Código STAMM'].values.astype(int).astype(str),index=empresa_codes.TXT_EMPRESA).to_dict()
    
    
    global seeds_dict
    seeds_dict = get_dict(cultura, 'mercado', 'seeds')
    
    bip_seeds.rename(columns=seeds_dict, inplace=True)
    
    if cultura == 'milho safrinha':
        bip_seeds = bip_seeds.drop(bip_seeds[(bip_seeds['Year, harvest (S)'] != 2023) & (bip_seeds['HISTORICO_REGIOES'] == 'New Regions')].index)
    
    try:
        bip_seeds['Year, harvest (S)'] = pd.to_numeric('20'+bip_seeds['Year, harvest (S)'].str[3:])
    except Exception:
        pass
    
    grouped_bip = bip_seeds.groupby(['Crop (S)','Year, harvest (S)']).sum().reset_index()
    grouped_bip_estado = bip_seeds.groupby(['Crop (S)','Year, harvest (S)', 'Land (S)']).sum().reset_index()
    grouped_bip_produto = bip_seeds.groupby(['Crop (S)','Year, harvest (S)',  'Variety (S)']).sum().reset_index()
    grouped_bip_distribuidor = bip_seeds.groupby(['Crop (S)','Year, harvest (S)', 'Distributor (S)']).sum().reset_index()
    
    
    try:
        exp_seeds =  pd.read_excel(path+r'\{0}\{1}'.format(cultura,r'\exp_seeds.xlsx'), sheet_name='Data Sheet')
        exp_estado_seeds =  pd.read_excel(path+r'\{0}\{1}'.format(cultura, r'\exp_estado_seeds.xlsx'), sheet_name='Data Sheet')
        exp_produto_seeds =  pd.read_excel(path+r'\{0}\{1}'.format(cultura, r'\exp_produto_seeds.xlsx'), sheet_name='Data Sheet')
        exp_distribuidor_seeds =  pd.read_excel(path+r'\{0}\{1}'.format(cultura, r'\exp_distribuidor_seeds.xlsx'), sheet_name='Data Sheet')
        
        input_list = [exp_seeds,exp_estado_seeds,exp_produto_seeds,exp_distribuidor_seeds ]
    except:
        print("Arquivos Explorer não existem")
        
    for exp_dataset in input_list:
    
        exp_dataset.ffill(inplace = True)
        exp_dataset['Tonnage (1000t)'] = exp_dataset['Tonnage (1000t)']*1000000
        
        if cultura == 'milho safrinha':
            exp_dataset.loc[(exp_dataset['Crop (S)'] == 'Milho') & (exp_dataset[ 'Harvest time (S)'] == '2ª safra'), 'Crop (S)'] = 'Milho Safrinha'
            exp_dataset.loc[(exp_dataset['Crop (S)'] == 'Milho') & (exp_dataset['Harvest time (S)'] == '1ª safra'), 'Crop (S)'] = 'Milho Verão'
            exp_dataset = exp_dataset.loc[~(exp_dataset['Harvest time (S)'] != '2ª safra')]
        elif cultura == 'milho verão':
            exp_dataset.loc[(exp_dataset['Crop (S)'] == 'Milho') & (exp_dataset['Harvest time (S)'] == '2ª safra'), 'Crop (S)'] = 'Milho Safrinha'
            exp_dataset.loc[(exp_dataset['Crop (S)'] == 'Milho') & (exp_dataset['Harvest time (S)'] == '1ª safra'), 'Crop (S)'] = 'Milho Verão'
            exp_dataset = exp_dataset.loc[~(exp_dataset['Harvest time (S)'] != '1ª safra')]
        # sec_path = r'\exp_seeds.xlsx'
       
    
    exp_estado_seeds['Land (S)'] = exp_estado_seeds['Land (S)'].str[-2:]
    
    rm_total = ['Harvest time (S)','Land (S)',
    'Variety (S)',
    'Variety (S).1',
    'Turnover excl. VAT (€ mio)',
    'Distributor (S)',
    'Distributor (S).1']
    mb_total = ['Year, harvest (S)','Crop (S)']
    rm_estado = ['Harvest time (S)',
    'Variety (S)',
    'Variety (S).1',
    'Turnover excl. VAT (€ mio)',
    'Distributor (S)',
    'Distributor (S).1']
    mb_estado = ['Year, harvest (S)','Crop (S)','Land (S)']
    
    result_df = generate_comparison_seeds(exp_seeds, rm_total, grouped_bip, mb_total)
    result_df_estado = generate_comparison_seeds(exp_estado_seeds, rm_estado, grouped_bip_estado, mb_estado)
    # eur_result_seeds = exp_seeds[['CULTURA', 'ANO',
    
    rm_produto = ['Harvest time (S)','Land (S)',
    'Turnover excl. VAT (€ mio)',
    'Distributor (S)',
    'Distributor (S).1']
    mb_produto = ['Year, harvest (S)','Crop (S)','Variety (S)']
    code_var_produto = 'Variety (S)'
    txt_var_produto = 'Variety (S).1'
    
    rm_distribuidor = ['Harvest time (S)','Land (S)',
    'Turnover excl. VAT (€ mio)',
    'Variety (S)',
    'Variety (S).1']
    mb_distribuidor = ['Year, harvest (S)','Crop (S)','Distributor (S)']
    code_var_distribuidor = 'Distributor (S)'
    txt_var_distribuidor = 'Distributor (S).1'
    
    
    result_df_produto = generate_comparison_code_seeds(exp_produto_seeds, rm_produto, 
                grouped_bip_produto, mb_produto, code_var_produto, txt_var_produto, produto_dict)
    result_df_distribuidor = generate_comparison_code_seeds(exp_distribuidor_seeds, rm_distribuidor, 
                grouped_bip_distribuidor, mb_distribuidor, code_var_distribuidor, txt_var_distribuidor, empresa_dict)



            
    result_list = [result_df, result_df_estado,
            result_df_produto, result_df_distribuidor]
    
    
    save_xls(result_list, path+r'\reuslt_seeds_{}.xlsx'.format(cultura))
