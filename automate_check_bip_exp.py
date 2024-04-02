# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 16:15:47 2023

automated_chcek_bib_exp.py

This script is used to: Apply automated test to Explorer DB CPP compared to BIP CPP

 
@author: gabriel.ferraz

Updated on Wed Dec  6 11:47:47 2023
"""

'''  BLOCO I

Libs import, dataset read and pre-processment

'''
from argparse import ArgumentParser
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from pandasql import sqldf
from pandas import ExcelWriter
from pandas import DataFrame
# import numpy as np
from sqlalchemy import create_engine
#from joblib import Parallel, delayed
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings('ignore')


def get_bip(cult:str, pesq:str, reg:str) -> DataFrame:
    # cult = cultura
    # pesq = 'indicadores'
    conn = psycopg2.connect(connection_str)
    cursor = conn.cursor()
    cursor.execute("SELECT query FROM config.select_query_indicadores where cultura = '{0}' and pesquisa = '{1}';".format(cultura, pesq))
    q = str(cursor.fetchone()[0]) 
    conn.close()
    bip = sqldf(q.format(reg, reg))
    return bip

def get_dict(cult:str, pesq:str, cs:str) -> dict:
    conn = psycopg2.connect(connection_str)
    cursor = conn.cursor()
    cursor.execute("SELECT chave,valor FROM config.dictionary_translator where cultura = '{0}' and pesquisa = '{1}' and cpp_seeds = '{2}';".format(cultura, pesq, cs))
    rows = cursor.fetchall()
    result_dict = {row[0]: row[1] for row in rows} 
    conn.close()
    return result_dict

def generate_comparison(data:DataFrame, dictionary:dict, gb_list:list, rm_list:list, bip_source:DataFrame) -> DataFrame:
       
    filtered_exp = data.ffill()#[mask]
    # filtered_exp.rename(columns=dictionary, inplace=True)
    
    if cultura == 'milho safrinha':
        filtered_exp.loc[(filtered_exp['Crop (S)'] == 'Milho') & (filtered_exp[ 'Harvest time (S)'] == '2ª safra'), 'Crop (S)'] = 'Milho Safrinha'
        filtered_exp.loc[(filtered_exp['Crop (S)'] == 'Milho') & (filtered_exp['Harvest time (S)'] == '1ª safra'), 'Crop (S)'] = 'Milho Verão'
        filtered_exp = filtered_exp.loc[~(filtered_exp['Harvest time (S)'] != '2ª safra')]
    elif cultura == 'milho verão':
        filtered_exp.loc[(filtered_exp['Crop (S)'] == 'Milho') & (filtered_exp['Harvest time (S)'] == '2ª safra'), 'Crop (S)'] = 'Milho Safrinha'
        filtered_exp.loc[(filtered_exp['Crop (S)'] == 'Milho') & (filtered_exp['Harvest time (S)'] == '1ª safra'), 'Crop (S)'] = 'Milho Verão'
        filtered_exp = filtered_exp.loc[~(filtered_exp['Harvest time (S)'] != '1ª safra')]

    grouped_exp = filtered_exp.groupby(gb_list).sum().reset_index()#filtered_exp.groupby(['CULTURA', 'SAFRA']).sum()#filtered_exp.groupby(['CULTURA', 'SAFRA']).sum()
                                                           
    colnames_list= list(dictionary.values())
    colnames_list = [e for e in colnames_list if e not in (rm_list)]
    grouped_exp_subset = grouped_exp.loc[:, colnames_list]
    
    bip_source.rename(columns=dictionary, inplace=True)
    bip_source = bip_source.loc[:, colnames_list]
    

    merged_df = pd.merge(grouped_exp_subset, bip_source, on=gb_list, suffixes=('_exp', '_bip'), how = 'inner')

    # Get the common column names for the division
    common_columns = set(bip_source.columns) -set(gb_list)

    # Divide corresponding columns
    for column in common_columns:
        exp_col = f'{column}_exp'
        bip_col = f'{column}_bip'
        merged_df[column] =round(merged_df[bip_col] / merged_df[exp_col],4)

    # Drop unnecessary columns
    result_df = merged_df[gb_list + list(common_columns)]
    return result_df

def generate_comparison_mer(data:DataFrame, dictionary:dict, gb_list:list, code_var:str,
                            txt_var:str, rm_list:list, bip_source:DataFrame, sparkbase_dict:dict) -> DataFrame:
    
    filtered_exp = data.ffill()#[mask]
    filtered_exp.rename(columns=dictionary, inplace=True)

    if cultura == 'milho safrinha':
        filtered_exp.loc[(filtered_exp['Crop (S)'] == 'Milho') & (filtered_exp['Harvest time (S)'] == '2ª safra'), 'Crop (S)'] = 'Milho Safrinha'
        filtered_exp.loc[(filtered_exp['Crop (S)'] == 'Milho') & (filtered_exp['Harvest time (S)'] == '1ª safra'), 'Crop (S)'] = 'Milho Verão'
        filtered_exp = filtered_exp.loc[~(filtered_exp['Harvest time (S)'] != '2ª safra')]
    elif cultura == 'milho verão':
        filtered_exp.loc[(filtered_exp['Crop (S)'] == 'Milho') & (filtered_exp['Harvest time (S)'] == '2ª safra'), 'Crop (S)'] = 'Milho Safrinha'
        filtered_exp.loc[(filtered_exp['Crop (S)'] == 'Milho') & (filtered_exp['Harvest time (S)'] == '1ª safra'), 'Crop (S)'] = 'Milho Verão'
        filtered_exp = filtered_exp.loc[~(filtered_exp['Harvest time (S)'] != '1ª safra')]
        
    gb_list_exp = gb_list.copy()     
    gb_list_exp.append(txt_var)
    
    grouped_exp = filtered_exp.groupby(gb_list_exp).sum().reset_index()#filtered_exp.groupby(['CULTURA', 'SAFRA']).sum()#filtered_exp.groupby(['CULTURA', 'SAFRA']).sum()
                                                           
    colnames_list= list(dictionary.values())
    colnames_list = [e for e in colnames_list if e not in (rm_list)]
    grouped_exp_subset = grouped_exp.loc[:, colnames_list]
    
    colnames_list.remove(txt_var)
    
    input_bip_mer = bip_source.copy()
    input_bip_mer.rename(columns=dictionary, inplace=True)
    input_bip_mer = input_bip_mer.loc[:, colnames_list]

    input_bip_mer.replace({code_var:sparkbase_dict}, inplace = True)
    grouped_bip_mer = input_bip_mer.groupby(gb_list).sum().reset_index()


    grouped_exp_subset[code_var] =grouped_exp_subset[code_var].astype(int).astype(str)
    grouped_exp_subset['Crop year (S)'] =grouped_exp_subset['Crop year (S)'].astype(int, errors = 'ignore').astype(str)
    grouped_bip_mer['Crop year (S)'] =grouped_bip_mer['Crop year (S)'].astype(int).astype(str)
    
    merged_df = pd.merge(grouped_exp_subset, grouped_bip_mer, on=gb_list, suffixes=('_exp', '_bip'), how = 'outer')

    # Get the common column names for the division
    common_columns = set(input_bip_mer.columns) - set(gb_list)

    # Divide corresponding columns
    for column in common_columns:
        exp_col = f'{column}_exp'
        bip_col = f'{column}_bip'
        merged_df[column] =round(merged_df[bip_col] / merged_df[exp_col],4)

    # Drop unnecessary columns
    
    result_df = merged_df[gb_list_exp + list(common_columns)]
    return result_df

def generate_comparison_seeds(data:DataFrame, rm_list:list, bip_data:DataFrame, merge_by:list) -> DataFrame:
    
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

def generate_comparison_code_seeds(data:DataFrame, rm_list:list, bip_data:DataFrame, merge_by:list, code_var:str, txt_var:str, dictionary:dict) -> DataFrame:
    
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

def save_xls(list_dfs:list, xls_path:str)-> None:
    with ExcelWriter(xls_path) as writer:
        for n, df in enumerate(list_dfs):
            df.to_excel(writer,'sheet%s' % n)
            
def main() -> None:
    
    
    path = r'C:\Users\{}\OneDrive - Kynetec\Documentos - Crop Subscription BR\INTERNO\TRANSFERÊNCIA\INTEGRAÇÃO - BIP E FARMTRAK\5. CHECAGEM RESULTADOS EXPLORER'.format(user)

    try:
        exp = pd.read_excel(path+r'\{}\exp.xlsx'.format(cultura), sheet_name='Data Sheet')
        exp_estado = pd.read_excel(path+r'\{}\exp_estado.xlsx'.format(cultura), sheet_name='Data Sheet')
        exp_estado['Land (S)'] = exp_estado['Land (S)'].str[-2:]
        exp_produto = pd.read_excel(path+r'\{}\exp_produto.xlsx'.format(cultura), sheet_name='Data Sheet')
        exp_distribuidor = pd.read_excel(path+r'\{}\exp_distribuidor.xlsx'.format(cultura), sheet_name='Data Sheet')
    except Exception: 
        print("Arquivos Explorer não existem")

    for file in os.listdir(path+r'\{}'.format(cultura)):
        if file.startswith("PIND"):
            pind = file
        elif file.startswith("PMER"):
            pmer = file

    global bip_ind
    global bip_mer
    
    try:
        bip_ind = pd.read_csv(path+r'\{0}\{1}'.format(cultura, pind), sep = ';', encoding='iso-8859-1', decimal=',')
        bip_ind['ADOÇÃO_EM_ÁREA'] = bip_ind['ADOÇÃO_EM_ÁREA'] * 100
        bip_mer = pd.read_csv(path+r'\{0}\{1}'.format(cultura, pmer), sep = ';', encoding='iso-8859-1', decimal=',')
    except Exception:
        print("Arquivos Bip não existem")

    # bip = bip[-3:]

    try:
        bip_ind['SAFRA'] = pd.to_numeric('20'+bip_ind['SAFRA'].str[3:])
        bip_mer['SAFRA'] = pd.to_numeric('20'+bip_mer['SAFRA'].str[3:])
    except Exception:
        pass

    bip_ind_total = get_bip(cultura, 'indicadores', 'PAIS')
    bip_ind_estado = get_bip(cultura, 'indicadores', 'ESTADO')


    produto_codes = pd.read_excel(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\bip 2023\@SPARK BASE\SPARK_BASE_INTEGRAÇÃO.xlsx', sheet_name='PRODUTOS')
    produto_dict = pd.Series(produto_codes.Filtro_STAMM.values.astype(int).astype(str),index=produto_codes.TXT_PRODUTO).to_dict()

    empresa_codes = pd.read_excel(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\bip 2023\@SPARK BASE\SPARK_BASE_INTEGRAÇÃO.xlsx', sheet_name='EMPRESA')
    empresa_dict = pd.Series(empresa_codes['Código STAMM'].values.astype(int).astype(str),index=empresa_codes.TXT_EMPRESA).to_dict()

    colnames_dict = get_dict(cultura, 'indicadores', 'cpp')
    colnames_dict_mer = get_dict(cultura, 'mercado', 'cpp')
            


    gb = ['Crop (S)','Crop year (S)'] 
    rm = ['Turnover excl. VAT (€ mio)', 'Harvest time (S)', 'Land (S)']
    ##############################
    gb_estado = ['Crop (S)', 'Crop year (S)', 'Land (S)']
    rm_estado = ['Turnover excl. VAT (€ mio)', 'Harvest time (S)']
    ##############################
    gb_produto = ['Crop (S)', 'Crop year (S)', 'Product (S)']
    code_var_produto = 'Product (S)'
    txt_var_produto = 'Product (S).1'
    rm_produto = ['Turnover excl. VAT (€ mio)',
     'Harvest time (S)',
     'Distributor (S)',
     'Distributor (S).1']
    ##############################
    gb_dist = ['Crop (S)', 'Crop year (S)','Distributor (S)']
    code_var_dist ='Distributor (S)'
    txt_var_dist = 'Distributor (S).1'
    rm_dist = ['Turnover excl. VAT (€ mio)',
     'Harvest time (S)',
     'Product (S)',
     'Product (S).1']
    
    
    result_df = generate_comparison(exp, colnames_dict, gb, rm, bip_ind_total)
    
    result_df_estado = generate_comparison(exp_estado, colnames_dict, gb_estado, rm_estado,bip_ind_estado)
    
    result_df_produto = generate_comparison_mer(exp_produto, colnames_dict_mer, gb_produto,
                                                code_var_produto, txt_var_produto,rm_produto,
                                                bip_mer,produto_dict)
    result_df_distribuidor = generate_comparison_mer(exp_distribuidor, colnames_dict_mer, gb_dist,
                                                     code_var_dist,  txt_var_dist,rm_dist,
                                                     bip_mer,empresa_dict)
    
    result_list = [result_df, result_df_estado,
            result_df_produto, result_df_distribuidor]
    
    
    save_xls(result_list, path+r'\reuslt_cpp_{}.xlsx'.format(cultura))

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

if __name__ == '__main__':  
       
    parser = ArgumentParser()
    parser.add_argument('-u', '--user', type=str, help='Usuário formato nome.sobrenome', required=True)
    parser.add_argument('-c', '--cultura', type=str,help='Cultura a ser analisada', required=True)
    args = parser.parse_args()
    
    load_dotenv()
    usuario = os.getenv('USER')
    senha = os.getenv('PASSWD')
    host = os.getenv('HOST')
    porta= os.getenv('PORTA')
    banco = os.getenv('DATABASE')
    global engine
    connection_str = f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}'
    engine = create_engine(connection_str)
 
    global cultura
    global user
    
    cultura = args.cultura
    user = args.user
    
    main()
    print('CPP done!')

    conn = psycopg2.connect(connection_str)
    cursor = conn.cursor()
    cursor.execute(f"SELECT cultura FROM config.dictionary_translator where cpp_seeds='seeds' and cultura='{cultura}';")
    if cursor.fetchone():
        main_seeds()
        print('Seeds done!')
    else:
        print('Seeds não disponível para essa cultura')


















