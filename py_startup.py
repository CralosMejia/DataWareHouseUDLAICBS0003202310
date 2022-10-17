from extract.extract_countries import extract_countries
from extract.extract_channels import extract_channels
from extract.extract_customers import extract_customers
from extract.extract_products import extract_products
from extract.extract_promotions import extract_promotions
from extract.extract_sales import extract_sales





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
    
    #Test extract channels
    #extract_channels();
    
    #Test extract countries
    #extract_countries();
    
    #Test extract customers
    #extract_customers();
    
    #Test extract products
    #extract_products()
    
    #Test extract promotions
    #extract_promotions()
    
    #Test extract sales
    extract_sales()
    
except:
    traceback.print_exc()
finally:
    pass