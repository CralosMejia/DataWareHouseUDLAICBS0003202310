from extract.extract_countries import extract_countries
from extract.extract_channels import extract_channels
from extract.extract_customers import extract_customers


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
    
    #Test extract countries
    extract_customers();
    
except:
    traceback.print_exc()
finally:
    pass