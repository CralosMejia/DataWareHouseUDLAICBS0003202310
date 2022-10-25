from util.db_connection import connect
from util.properties import getProperty
from util.sql import merge

import traceback
import pandas as pd


def load_promotions(ID):
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
            "promo_end_date":[],
            "process_id": []
        }
        
        #Reading the csv file
        promotions_tra_table = pd.read_sql(f'SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_tra WHERE PROCESS_ID = {ID}', ses_db_stg)
        
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
                promotions_dict["process_id"].append(ID)
                
        if promotions_dict["promo_id"]:
            df_promotions_load = pd.DataFrame(promotions_dict)
            merge(table_name='promotions', natural_key_cols=['promo_id'], dataframe= df_promotions_load, db_context=ses_db_sor);

    except:
        traceback.print_exc()
    finally:
        pass