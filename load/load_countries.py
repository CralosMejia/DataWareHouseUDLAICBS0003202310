from util.db_connection import connect
from util.properties import getProperty
from util.sql import merge

import traceback
import pandas as pd


def load_countries(ID):
    try:
        #"COUNTRY_ID","COUNTRY_NAME","COUNTRY_REGION","COUNTRY_REGION_ID"
        
        name_DB_stg = getProperty("DBSTG")
        name_DB_sor = getProperty("DBSOR")
        
        
        ses_db_stg = connect(name_DB_stg);
        ses_db_sor = connect(name_DB_sor);
        
        #Dictionary for values of chanels
        country_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            "process_id":[]
        }
        
        #Reading the csv file
        country_tra_table = pd.read_sql(f'SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,COUNTRY_REGION_ID FROM countries_tra WHERE PROCESS_ID = {ID}', ses_db_stg)
        
        if not country_tra_table.empty:
            for id,name,region,region_id in zip(
                country_tra_table["COUNTRY_ID"],
                country_tra_table["COUNTRY_NAME"],
                country_tra_table["COUNTRY_REGION"],
                country_tra_table["COUNTRY_REGION_ID"]
                ):
                
                country_dict["country_id"].append(id)
                country_dict["country_name"].append(name)
                country_dict["country_region"].append(region)
                country_dict["country_region_id"].append(region_id)
                country_dict["process_id"].append(ID)
                
        if country_dict["country_id"]:
            df_contries_load = pd.DataFrame(country_dict)
            merge(table_name='countries', natural_key_cols=['country_id'], dataframe= df_contries_load, db_context=ses_db_sor)
        ses_db_stg.dispose()
        ses_db_sor.dispose()

    except:
        traceback.print_exc()
    finally:
        pass