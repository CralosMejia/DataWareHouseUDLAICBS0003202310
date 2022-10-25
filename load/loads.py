from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times








from util.db_connection import connect
from util.properties import getProperty


import traceback
import pandas as pd

def loads():
    try:
        ID = obt_process_ID()
        
        #LOAD
        load_channels(ID)
        load_countries(ID)
        load_products(ID)
        load_promotions(ID)
        load_times(ID)
        load_customers(ID)
        load_sales(ID)
        
    except:
        traceback.print_exc()
    finally:
        pass

def obt_process_ID():
    ses_db_stg = connect(getProperty("DBSTG"));
    table_process = pd.read_sql('SELECT ID FROM process_etl ORDER by ID DESC LIMIT 1', ses_db_stg)
    
    if(not table_process.empty):
        id = table_process['ID'][0]
    else:
        id = None;
    
    return id;