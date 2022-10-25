from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def extract_customers():
    try:
        #"CUST_ID","CUST_FIRST_NAME","CUST_LAST_NAME","CUST_GENDER","CUST_YEAR_OF_BIRTH","CUST_MARITAL_STATUS","CUST_STREET_ADDRESS","CUST_POSTAL_CODE","CUST_CITY","CUST_STATE_PROVINCE","COUNTRY_ID","CUST_MAIN_PHONE_NUMBER","CUST_INCOME_LEVEL","CUST_CREDIT_LIMIT","CUST_EMAIL"
                
        name_DB = getProperty("DBSTG")
        path_customers_csv = getProperty("PCSVCUSTOMERS")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        customers_dict = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
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
        customers_csv = pd.read_csv(path_customers_csv)
        
        if not customers_csv.empty:
            for id,name,lastname,gender,birth,maritalS,address,postalC,city,province,counrtyId,phone,incomeL,creditL,email in zip(
                customers_csv["CUST_ID"],
                customers_csv["CUST_FIRST_NAME"],
                customers_csv["CUST_LAST_NAME"],
                customers_csv["CUST_GENDER"],
                customers_csv["CUST_YEAR_OF_BIRTH"],
                customers_csv["CUST_MARITAL_STATUS"],
                customers_csv["CUST_STREET_ADDRESS"],
                customers_csv["CUST_POSTAL_CODE"],
                customers_csv["CUST_CITY"],
                customers_csv["CUST_STATE_PROVINCE"],
                customers_csv["COUNTRY_ID"],
                customers_csv["CUST_MAIN_PHONE_NUMBER"],
                customers_csv["CUST_INCOME_LEVEL"],
                customers_csv["CUST_CREDIT_LIMIT"],
                customers_csv["CUST_EMAIL"]
                ):
                
                customers_dict["cust_id"].append(id)
                customers_dict["cust_first_name"].append(name)
                customers_dict["cust_last_name"].append(lastname)
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
            ses_db_stg.connect().execute('TRUNCATE TABLE customers_ext')
            df_customers_ext = pd.DataFrame(customers_dict)
            df_customers_ext.to_sql('customers_ext',ses_db_stg,if_exists='append',index=False)
        ses_db_stg.dispose()

    except:
        traceback.print_exc()
    finally:
        pass