from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def load_promotions():
    try:
        #"PROMO_ID","PROMO_NAME","PROMO_COST","PROMO_BEGIN_DATE","PROMO_END_DATE"
                
        name_DB_stg = getProperty("DBSTG")
        name_DB_sor = getProperty("DBSOR")
        
        
        ses_db_stg = connect(name_DB_stg);
        ses_db_sor = connect(name_DB_sor);
        
        #Dictionary for values of chanels
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        
        #Reading the csv file
        promotions_tra_table = pd.read_sql('SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_tra', ses_db_stg)
        
        if not promotions_tra_table.empty:
            for id,name,promCost,promBegDate,promEndDate in zip(
                promotions_tra_table["PROMO_ID"],
                promotions_tra_table["PROMO_NAME"],
                promotions_tra_table["PROMO_COST"],
                promotions_tra_table["PROMO_BEGIN_DATE"],
                promotions_tra_table["PROMO_END_DATE"]
                ):
                
                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(name)
                promotions_dict["promo_cost"].append(promCost)
                promotions_dict["promo_begin_date"].append(promBegDate)
                promotions_dict["promo_end_date"].append(promEndDate)
                
        if promotions_dict["promo_id"]:
            df_promotions_load = pd.DataFrame(promotions_dict)
            df_promotions_load.to_sql('promotions',ses_db_sor,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass