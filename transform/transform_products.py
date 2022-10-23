from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def transform_products(ID):
    try:
        #"PROD_ID","PROD_NAME","PROD_DESC","PROD_CATEGORY","PROD_CATEGORY_ID","PROD_CATEGORY_DESC","PROD_WEIGHT_CLASS","SUPPLIER_ID","PROD_STATUS","PROD_LIST_PRICE","PROD_MIN_PRICE"
                
        name_DB = getProperty("DBSTG")
        
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
            "prod_min_price":[],
            "process_id":[]
        }
        
        #Reading the csv file
        products_ext_table = pd.read_sql('SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_ext', ses_db_stg)
        
        if not products_ext_table.empty:
            for id,name,prodD,prodCate,prodCateId,prodCateD,prodWeiC,supliId,prodS,prodLiPri,prodMinPri in zip(
                products_ext_table["PROD_ID"],
                products_ext_table["PROD_NAME"],
                products_ext_table["PROD_DESC"],
                products_ext_table["PROD_CATEGORY"],
                products_ext_table["PROD_CATEGORY_ID"],
                products_ext_table["PROD_CATEGORY_DESC"],
                products_ext_table["PROD_WEIGHT_CLASS"],
                products_ext_table["SUPPLIER_ID"],
                products_ext_table["PROD_STATUS"],
                products_ext_table["PROD_LIST_PRICE"],
                products_ext_table["PROD_MIN_PRICE"]
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
                products_dict["process_id"].append(ID)
                
                
        if products_dict["prod_id"]:
            df_produtcs_tra = pd.DataFrame(products_dict)
            df_produtcs_tra.to_sql('products_tra',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass