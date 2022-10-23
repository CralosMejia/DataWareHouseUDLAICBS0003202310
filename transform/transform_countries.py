from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def transform_countries(ID):
    try:
        #"COUNTRY_ID","COUNTRY_NAME","COUNTRY_REGION","COUNTRY_REGION_ID"
        
        name_DB = getProperty("DBSTG")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        country_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            "process_id":[]
        }
        
        #Reading the csv file
        country_ext_table = pd.read_sql('SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,COUNTRY_REGION_ID FROM countries_ext', ses_db_stg)
        
        if not country_ext_table.empty:
            for id,name,region,region_id in zip(
                country_ext_table["COUNTRY_ID"],
                country_ext_table["COUNTRY_NAME"],
                country_ext_table["COUNTRY_REGION"],
                country_ext_table["COUNTRY_REGION_ID"]
                ):
                
                country_dict["country_id"].append(id)
                country_dict["country_name"].append(name)
                country_dict["country_region"].append(region)
                country_dict["country_region_id"].append(region_id)
                country_dict["process_id"].append(ID)
                
        if country_dict["country_id"]:
            df_contries_tra = pd.DataFrame(country_dict)
            df_contries_tra.to_sql('countries_tra',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass