from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def load_sales():
    try:
        #"PROD_ID","CUST_ID","TIME_ID","CHANNEL_ID","PROMO_ID","QUANTITY_SOLD","AMOUNT_SOLD"
                
        name_DB_stg = getProperty("DBSTG")
        name_DB_sor = getProperty("DBSOR")
        
        
        ses_db_stg = connect(name_DB_stg);
        ses_db_sor = connect(name_DB_sor);
        
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
        sales_tra_table = pd.read_sql('SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_tra', ses_db_stg)
        
        if not sales_tra_table.empty:
            for prodId,custId,timeId,channelId,promoId,quantiSold,amountSold in zip(
                sales_tra_table["PROD_ID"],
                sales_tra_table["CUST_ID"],
                sales_tra_table["TIME_ID"],
                sales_tra_table["CHANNEL_ID"],
                sales_tra_table["PROMO_ID"],
                sales_tra_table["QUANTITY_SOLD"],
                sales_tra_table["AMOUNT_SOLD"],
                
                ):
                
                sales_dict["prod_id"].append(prodId)
                sales_dict["cust_id"].append(custId)
                sales_dict["time_id"].append(timeId)
                sales_dict["channel_id"].append(channelId)
                sales_dict["promo_id"].append(promoId)
                sales_dict["quantity_sold"].append(quantiSold)
                sales_dict["amount_sold"].append(amountSold)
                
                
        if sales_dict["prod_id"]:
            df_sales_load = pd.DataFrame(sales_dict)
            df_sales_load.to_sql('sales',ses_db_sor,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass