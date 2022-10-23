from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times
from transform.transform_customers import transform_customers
from transform.transform_countries import transform_countries
from transform.transform_channels import transform_channels
from extract.extract_countries import extract_countries
from extract.extract_channels import extract_channels
from extract.extract_customers import extract_customers
from extract.extract_products import extract_products
from extract.extract_promotions import extract_promotions
from extract.extract_sales import extract_sales
from extract.extract_times import extract_times
from transform.transform_products import transform_products
from transform.transform_promotions import transform_promotions
from transform.transform_sales import transform_sales
from transform.transform_times import transform_times
from transform.transforms import create_process_ID








from util.db_connection import connect
from util.properties import getProperty


import traceback
import pandas as pd


try:
    #Test connection DB
    """
    ses_db_stg = connect(getProperty("DBSTG"));

    df = pd.read_sql('SELECT USER,HOST FROM MYSQL.USER',ses_db_stg)
    print(df)
    """
    
    #EXTRACT
    # extract_channels()
    # extract_countries()
    # extract_customers()
    # extract_products()
    # extract_promotions()
    # extract_sales()
    # extract_times()
    
    # #TRANSFORMATION
    # transform_channels()
    # transform_countries()
    # transform_customers()
    # transform_products()
    # transform_promotions()
    # transform_sales()
    # transform_times()
    
    #LOAD
    # load_channels()
    # load_countries()
    # load_customers()
    # load_products()
    # load_promotions()
    #load_times()
    #load_sales()
    
    create_process_ID()
except:
    traceback.print_exc()
finally:
    pass