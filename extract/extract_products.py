from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def extract_products():
    try:
        #"PROD_ID","PROD_NAME","PROD_DESC","PROD_CATEGORY","PROD_CATEGORY_ID","PROD_CATEGORY_DESC","PROD_WEIGHT_CLASS","SUPPLIER_ID","PROD_STATUS","PROD_LIST_PRICE","PROD_MIN_PRICE"
                
        name_DB = getProperty("DBSTG")
        path_products_csv = getProperty("PCSVPRODUCTS")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        products_dict = {
            "prod_id":[],
            "prod_name":[],
            "prod_desc":[],
            "prod_category":[],
            "prod_category_id":[],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[]
        }
        
        #Reading the csv file
        products_csv = pd.read_csv(path_products_csv)
        
        if not products_csv.empty:
            for id,name,prodD,prodCate,prodCateId,prodCateD,prodWeiC,supliId,prodS,prodLiPri,prodMinPri in zip(
                products_csv["PROD_ID"],
                products_csv["PROD_NAME"],
                products_csv["PROD_DESC"],
                products_csv["PROD_CATEGORY"],
                products_csv["PROD_CATEGORY_ID"],
                products_csv["PROD_CATEGORY_DESC"],
                products_csv["PROD_WEIGHT_CLASS"],
                products_csv["SUPPLIER_ID"],
                products_csv["PROD_STATUS"],
                products_csv["PROD_LIST_PRICE"],
                products_csv["PROD_MIN_PRICE"]
                ):
                
                products_dict["prod_id"].append(id)
                products_dict["prod_name"].append(name)
                products_dict["prod_desc"].append(prodD)
                products_dict["prod_category"].append(prodCate)
                products_dict["prod_category_id"].append(prodCateId)
                products_dict["prod_category_desc"].append(prodCateD)
                products_dict["prod_weight_class"].append(prodWeiC)
                products_dict["supplier_id"].append(supliId)
                products_dict["prod_status"].append(prodS)
                products_dict["prod_list_price"].append(prodLiPri)
                products_dict["prod_min_price"].append(prodMinPri)
                
        if products_dict["prod_id"]:
            ses_db_stg.connect().execute('TRUNCATE TABLE products_ext')
            df_produtcs_ext = pd.DataFrame(products_dict)
            df_produtcs_ext.to_sql('products_ext',ses_db_stg,if_exists='append',index=False)
        ses_db_stg.dispose()

    except:
        traceback.print_exc()
    finally:
        pass