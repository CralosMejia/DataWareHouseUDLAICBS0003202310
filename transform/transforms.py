from datetime import datetime
from transform.transform_customers import transform_customers
from transform.transform_countries import transform_countries
from transform.transform_channels import transform_channels
from transform.transform_products import transform_products
from transform.transform_promotions import transform_promotions
from transform.transform_sales import transform_sales
from transform.transform_times import transform_times

from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd

def transformations():
    try:
        
        ID = create_process_ID();
        
        # #TRANSFORMATION
        transform_channels(ID)
        transform_countries(ID)
        transform_customers(ID)
        transform_products(ID)
        transform_promotions(ID)
        transform_sales(ID)
        transform_times(ID)
        
    except:
        traceback.print_exc()
    finally:
        pass
    
def create_process_ID():
    ses_db_stg = connect(getProperty("DBSTG"));
    
    process_dict = {
            "date_process":[]
        }
    
    process_dict["date_process"].append(datetime.now())
    
    df_process = pd.DataFrame(process_dict)
    df_process.to_sql('process_etl',ses_db_stg,if_exists='append',index=False)
    
    table_process = pd.read_sql('SELECT ID FROM process_etl ORDER by ID DESC LIMIT 1', ses_db_stg)
    
    if(not table_process.empty):
        id = table_process['ID'][0]
    else:
        id = None;
    
    return id;
    
    