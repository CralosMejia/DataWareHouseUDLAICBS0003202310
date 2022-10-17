from util.db_connection import connect
from util.properties import getProperty
from extract.extract_channels import extract_channels


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
    extract_channels();
    
    
    
except:
    traceback.print_exc()
finally:
    pass