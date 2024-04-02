import os
import pandas as pd
import numpy as np
import logging
from argparse import ArgumentParser
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s\t%(levelname)s\t%(message)s'
)

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))

USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
DATABASE = os.getenv('DATABASE')

ENGINE = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

PANEL_COLS = ['CATEGORIA','CULTURA','SAFRA','SAFRA_UNIFICADA'] #TODO abstrair esses valores, definir conceito de Painel
EXCEL_FILENAME = 'qa_null_count_by_panel_teste.xlsx'

def main() -> None:
    for table in args['tables']:
        logging.info(f'Analysing table {table} ...')

        schemaname = table.split('.')[0]
        tablename = table.split('.')[1]
        
        # query table to dataframe  #TODO adicionar try/catch
        query = f'SELECT * FROM {schemaname}.{tablename}'
        df = pd.read_sql(query, ENGINE)
        
        # replace false Null registers #TODO validar as regras de negócio
        df.replace(to_replace='', value=None, inplace=True)
        
        # get columns with null values
        nulls = df.isna().sum().loc[lambda x:x>0]
        null_cols = nulls.index.values

        # organize result dataframe, filter columns, aggregate sum of null values by panel, and remove all rows with no-nulls (value != 0)
        filtered_cols = PANEL_COLS + null_cols.tolist()
        result = df[filtered_cols].groupby(PANEL_COLS).agg(lambda x: x.isnull().sum())
        result = result[(result!=0).any(axis=1)]

        if not result.empty:
            exists = os.path.isfile(os.path.join(ROOT_PATH,EXCEL_FILENAME))
            mode = 'a' if exists else 'w'

            result = result.reset_index()
            logging.info(f'saving results for {table} ...')
            with pd.ExcelWriter(EXCEL_FILENAME, mode=mode) as writer:
                result.to_excel(writer, sheet_name=tablename)
        else: 
            logging.info(f'{table} do not have null values.')

        logging.info(f'{table} complete!')

def check_args() -> None:
    for key, value in args.items():
        for el in value:
            if len(el.split('.')) < 2:
                raise Exception(f'Parameter {key} incorrect. Expected pattern schema.table')

if __name__ == '__main__':  

    parser = ArgumentParser()
    parser.add_argument('-t', '--tables', type=str, nargs='+', help='Tables to be analysed', required=True)
    args = vars(parser.parse_args())

    check_args()

    main()