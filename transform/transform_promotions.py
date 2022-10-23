from transform.transformation import obt_date
from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def transform_promotions(ID):
    try:
        #"PROMO_ID","PROMO_NAME","PROMO_COST","PROMO_BEGIN_DATE","PROMO_END_DATE"
                
        name_DB = getProperty("DBSTG")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[],
            "process_id":[]
        }
        
        #Reading the csv file
        promotions_ext_table = pd.read_sql('SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_ext', ses_db_stg)
        
        if not promotions_ext_table.empty:
            for id,name,promCost,promBegDate,promEndDate in zip(
                promotions_ext_table["PROMO_ID"],
                promotions_ext_table["PROMO_NAME"],
                promotions_ext_table["PROMO_COST"],
                promotions_ext_table["PROMO_BEGIN_DATE"],
                promotions_ext_table["PROMO_END_DATE"]
                ):
                
                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(name)
                promotions_dict["promo_cost"].append(promCost)
                promotions_dict["promo_begin_date"].append(obt_date(promBegDate))
                promotions_dict["promo_end_date"].append(obt_date(promEndDate))
                promotions_dict["process_id"].append(ID)
                
        if promotions_dict["promo_id"]:
            df_promotions_tra = pd.DataFrame(promotions_dict)
            df_promotions_tra.to_sql('promotions_tra',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass