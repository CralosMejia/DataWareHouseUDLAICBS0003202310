from util.db_connection import connect
from util.properties import getProperty
from transform.transformation import obt_date

import traceback
import pandas as pd


def transform_sales(ID):
    try:
        #"PROD_ID","CUST_ID","TIME_ID","CHANNEL_ID","PROMO_ID","QUANTITY_SOLD","AMOUNT_SOLD"
                
        name_DB = getProperty("DBSTG")
        
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
            "process_id":[]
            
        }
        
        #Reading the csv file
        sales_ext_table = pd.read_sql('SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_ext', ses_db_stg)
        
        if not sales_ext_table.empty:
            for prodId,custId,timeId,channelId,promoId,quantiSold,amountSold in zip(
                sales_ext_table["PROD_ID"],
                sales_ext_table["CUST_ID"],
                sales_ext_table["TIME_ID"],
                sales_ext_table["CHANNEL_ID"],
                sales_ext_table["PROMO_ID"],
                sales_ext_table["QUANTITY_SOLD"],
                sales_ext_table["AMOUNT_SOLD"],
                
                ):
                
                sales_dict["prod_id"].append(prodId)
                sales_dict["cust_id"].append(custId)
                sales_dict["time_id"].append(obt_date(timeId))
                sales_dict["channel_id"].append(channelId)
                sales_dict["promo_id"].append(promoId)
                sales_dict["quantity_sold"].append(quantiSold)
                sales_dict["amount_sold"].append(amountSold)
                sales_dict["process_id"].append(ID)
                
                
                
        if sales_dict["prod_id"]:
            df_sales_tra = pd.DataFrame(sales_dict)
            df_sales_tra.to_sql('sales_tra',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass