from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def extract_sales():
    try:
        #"PROD_ID","CUST_ID","TIME_ID","CHANNEL_ID","PROMO_ID","QUANTITY_SOLD","AMOUNT_SOLD"
                
        name_DB = getProperty("DBSTG")
        path_sales_csv = getProperty("PCSVSALES")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
            
        }
        
        #Reading the csv file
        sales_csv = pd.read_csv(path_sales_csv)
        
        if not sales_csv.empty:
            for prodId,custId,timeId,channelId,promoId,quantiSold,amountSold in zip(
                sales_csv["PROD_ID"],
                sales_csv["CUST_ID"],
                sales_csv["TIME_ID"],
                sales_csv["CHANNEL_ID"],
                sales_csv["PROMO_ID"],
                sales_csv["QUANTITY_SOLD"],
                sales_csv["AMOUNT_SOLD"],
                
                ):
                
                sales_dict["prod_id"].append(prodId)
                sales_dict["cust_id"].append(custId)
                sales_dict["time_id"].append(timeId)
                sales_dict["channel_id"].append(channelId)
                sales_dict["promo_id"].append(promoId)
                sales_dict["quantity_sold"].append(quantiSold)
                sales_dict["amount_sold"].append(amountSold)
                
                
        if sales_dict["prod_id"]:
            ses_db_stg.connect().execute('TRUNCATE TABLE sales_ext')
            df_sales_ext = pd.DataFrame(sales_dict)
            df_sales_ext.to_sql('sales_ext',ses_db_stg,if_exists='append',index=False)
        ses_db_stg.dispose()

    except:
        traceback.print_exc()
    finally:
        pass