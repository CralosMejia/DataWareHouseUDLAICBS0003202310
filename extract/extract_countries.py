from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def extract_countries():
    try:
        #"COUNTRY_ID","COUNTRY_NAME","COUNTRY_REGION","COUNTRY_REGION_ID"
        
        name_DB = getProperty("DBSTG")
        path_coutries_csv = getProperty("PCSVCOUNTRIES")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        country_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }
        
        #Reading the csv file
        country_csv = pd.read_csv(path_coutries_csv)
        
        if not country_csv.empty:
            for id,name,region,region_id in zip(
                country_csv["COUNTRY_ID"],
                country_csv["COUNTRY_NAME"],
                country_csv["COUNTRY_REGION"],
                country_csv["COUNTRY_REGION_ID"]
                ):
                
                country_dict["country_id"].append(id)
                country_dict["country_name"].append(name)
                country_dict["country_region"].append(region)
                country_dict["country_region_id"].append(region_id)
                
        if country_dict["country_id"]:
            ses_db_stg.connect().execute('TRUNCATE TABLE countries')
            df_contries_ext = pd.DataFrame(country_dict)
            df_contries_ext.to_sql('countries',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass