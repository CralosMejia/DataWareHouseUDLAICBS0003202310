from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def extract_promotions():
    try:
        #"PROMO_ID","PROMO_NAME","PROMO_COST","PROMO_BEGIN_DATE","PROMO_END_DATE"
                
        name_DB = getProperty("DBSTG")
        path_promotions_csv = getProperty("PCSVPROMOTIONS")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        
        #Reading the csv file
        promotions_csv = pd.read_csv(path_promotions_csv)
        
        if not promotions_csv.empty:
            for id,name,promCost,promBegDate,promEndDate in zip(
                promotions_csv["PROMO_ID"],
                promotions_csv["PROMO_NAME"],
                promotions_csv["PROMO_COST"],
                promotions_csv["PROMO_BEGIN_DATE"],
                promotions_csv["PROMO_END_DATE"]
                ):
                
                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(name)
                promotions_dict["promo_cost"].append(promCost)
                promotions_dict["promo_begin_date"].append(promBegDate)
                promotions_dict["promo_end_date"].append(promEndDate)
                
        if promotions_dict["promo_id"]:
            ses_db_stg.connect().execute('TRUNCATE TABLE promotions')
            df_promotions_ext = pd.DataFrame(promotions_dict)
            df_promotions_ext.to_sql('promotions',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass