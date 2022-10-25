from util.db_connection import connect
from util.properties import getProperty
from util.sql import merge


import traceback
import pandas as pd


def load_sales(ID):
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
            "process_id": []
            
        }
        
        #Reading the csv file
        sales_tra_table = pd.read_sql(f'SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_tra WHERE PROCESS_ID = {ID}', ses_db_stg)

        #Subrrogate keys Products
        subrrogate_key_prod = pd.read_sql_query('SELECT ID, PROD_ID FROM products', ses_db_sor).set_index('PROD_ID').to_dict()['ID']
        sales_tra_table['PROD_ID']= sales_tra_table['PROD_ID'].apply(lambda key: subrrogate_key_prod[key])

        # Subrrogate keys Customers
        subrrogate_key_cust = pd.read_sql_query('SELECT ID, CUST_ID FROM customers', ses_db_sor).set_index('CUST_ID').to_dict()['ID']
        sales_tra_table['CUST_ID'] = sales_tra_table['CUST_ID'].apply(lambda key: subrrogate_key_cust[key])

        # Subrrogate keys Customers
        subrrogate_key_time = pd.read_sql_query('SELECT ID, TIME_ID FROM times', ses_db_sor).set_index('TIME_ID').to_dict()['ID']
        sales_tra_table['TIME_ID'] = sales_tra_table['TIME_ID'].apply(lambda key: subrrogate_key_time[key])

        # Subrrogate keys Channels
        subrrogate_key_channels = pd.read_sql_query('SELECT ID, CHANNEL_ID FROM channels', ses_db_sor).set_index('CHANNEL_ID').to_dict()['ID']
        sales_tra_table['CHANNEL_ID'] = sales_tra_table['CHANNEL_ID'].apply(lambda key: subrrogate_key_channels[key])

        # Subrrogate keys Promotions
        subrrogate_key_promotions = pd.read_sql_query('SELECT ID, PROMO_ID FROM promotions', ses_db_sor).set_index('PROMO_ID').to_dict()['ID']
        sales_tra_table['PROMO_ID'] = sales_tra_table['PROMO_ID'].apply(lambda key: subrrogate_key_promotions[key])
        
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
                sales_dict["process_id"].append(ID)
                
        if sales_dict["prod_id"]:
            df_sales_load = pd.DataFrame(sales_dict)
            merge(table_name='sales', natural_key_cols=['prod_id', 'cust_id', 'time_id', 'channel_id', 'promo_id'], dataframe= df_sales_load, db_context=ses_db_sor);
        ses_db_stg.dispose()
        ses_db_sor.dispose()

    except:
        traceback.print_exc()
    finally:
        pass