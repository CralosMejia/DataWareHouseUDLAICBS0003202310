from transform.transformation import obt_gender
from transform.transformation import join_2_strings
from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def transform_customers(ID):
    try:
        #"CUST_ID","CUST_FIRST_NAME","CUST_LAST_NAME","CUST_GENDER","CUST_YEAR_OF_BIRTH","CUST_MARITAL_STATUS","CUST_STREET_ADDRESS","CUST_POSTAL_CODE","CUST_CITY","CUST_STATE_PROVINCE","COUNTRY_ID","CUST_MAIN_PHONE_NUMBER","CUST_INCOME_LEVEL","CUST_CREDIT_LIMIT","CUST_EMAIL"
                
        name_DB = getProperty("DBSTG")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        customers_dict = {
            "cust_id":[],
            "cust_full_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_number":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[],
            "process_id":[]
        }
        
        #Reading the csv file
        customers_ext_table = pd.read_sql('SELECT CUST_ID,CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_ext', ses_db_stg)
        
        if not customers_ext_table.empty:
            for id,name,lastname,gender,birth,maritalS,address,postalC,city,province,counrtyId,phone,incomeL,creditL,email in zip(
                customers_ext_table["CUST_ID"],
                customers_ext_table["CUST_FIRST_NAME"],
                customers_ext_table["CUST_LAST_NAME"],
                customers_ext_table["CUST_GENDER"],
                customers_ext_table["CUST_YEAR_OF_BIRTH"],
                customers_ext_table["CUST_MARITAL_STATUS"],
                customers_ext_table["CUST_STREET_ADDRESS"],
                customers_ext_table["CUST_POSTAL_CODE"],
                customers_ext_table["CUST_CITY"],
                customers_ext_table["CUST_STATE_PROVINCE"],
                customers_ext_table["COUNTRY_ID"],
                customers_ext_table["CUST_MAIN_PHONE_NUMBER"],
                customers_ext_table["CUST_INCOME_LEVEL"],
                customers_ext_table["CUST_CREDIT_LIMIT"],
                customers_ext_table["CUST_EMAIL"]
                ):
                
                customers_dict["cust_id"].append(id)
                customers_dict["cust_full_name"].append(join_2_strings(name,lastname))
                customers_dict["cust_gender"].append(obt_gender(gender))
                customers_dict["cust_year_of_birth"].append(birth)
                customers_dict["cust_marital_status"].append(maritalS)
                customers_dict["cust_street_address"].append(address)
                customers_dict["cust_postal_code"].append(postalC)
                customers_dict["cust_city"].append(city)
                customers_dict["cust_state_province"].append(province)
                customers_dict["country_id"].append(counrtyId)
                customers_dict["cust_main_phone_number"].append(phone)
                customers_dict["cust_income_level"].append(incomeL)
                customers_dict["cust_credit_limit"].append(creditL)
                customers_dict["cust_email"].append(email)
                customers_dict["process_id"].append(ID)
                
                
        if customers_dict["cust_id"]:
            df_customers_tra = pd.DataFrame(customers_dict)
            df_customers_tra.to_sql('customers_tra',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass