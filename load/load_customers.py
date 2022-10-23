from transform.transformation import obt_gender
from transform.transformation import join_2_strings
from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def load_customers():
    try:
        #"CUST_ID","CUST_FIRST_NAME","CUST_LAST_NAME","CUST_GENDER","CUST_YEAR_OF_BIRTH","CUST_MARITAL_STATUS","CUST_STREET_ADDRESS","CUST_POSTAL_CODE","CUST_CITY","CUST_STATE_PROVINCE","COUNTRY_ID","CUST_MAIN_PHONE_NUMBER","CUST_INCOME_LEVEL","CUST_CREDIT_LIMIT","CUST_EMAIL"
                
        name_DB_stg = getProperty("DBSTG")
        name_DB_sor = getProperty("DBSOR")
        
        
        ses_db_stg = connect(name_DB_stg);
        ses_db_sor = connect(name_DB_sor);
        
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
            "cust_email":[]
        }
        
        #Reading the csv file
        customers_tra_table = pd.read_sql('SELECT CUST_ID,CUST_FULL_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_tra', ses_db_stg)
        
        if not customers_tra_table.empty:
            for id,name,gender,birth,maritalS,address,postalC,city,province,counrtyId,phone,incomeL,creditL,email in zip(
                customers_tra_table["CUST_ID"],
                customers_tra_table["CUST_FULL_NAME"],
                customers_tra_table["CUST_GENDER"],
                customers_tra_table["CUST_YEAR_OF_BIRTH"],
                customers_tra_table["CUST_MARITAL_STATUS"],
                customers_tra_table["CUST_STREET_ADDRESS"],
                customers_tra_table["CUST_POSTAL_CODE"],
                customers_tra_table["CUST_CITY"],
                customers_tra_table["CUST_STATE_PROVINCE"],
                customers_tra_table["COUNTRY_ID"],
                customers_tra_table["CUST_MAIN_PHONE_NUMBER"],
                customers_tra_table["CUST_INCOME_LEVEL"],
                customers_tra_table["CUST_CREDIT_LIMIT"],
                customers_tra_table["CUST_EMAIL"]
                ):
                
                customers_dict["cust_id"].append(id)
                customers_dict["cust_full_name"].append(name)
                customers_dict["cust_gender"].append(gender)
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
                
        if customers_dict["cust_id"]:
            df_customers_load = pd.DataFrame(customers_dict)
            df_customers_load.to_sql('customers',ses_db_sor,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass