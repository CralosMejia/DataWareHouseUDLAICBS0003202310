from util.db_connection import connect
from util.properties import getProperty
from util.sql import merge


import traceback
import pandas as pd


def load_times(ID):
    try:
        #"TIME_ID","DAY_NAME","DAY_NUMBER_IN_WEEK","DAY_NUMBER_IN_MONTH","CALENDAR_WEEK_NUMBER","CALENDAR_MONTH_NUMBER","CALENDAR_MONTH_DESC","END_OF_CAL_MONTH","CALENDAR_QUARTER_DESC","CALENDAR_YEAR"
                
        name_DB_stg = getProperty("DBSTG")
        name_DB_sor = getProperty("DBSOR")
        
        
        ses_db_stg = connect(name_DB_stg);
        ses_db_sor = connect(name_DB_sor);
        
        #Dictionary for values of chanels
        times_dict = {
            "time_id":[],
            "day_name":[],
            "day_number_in_week":[],
            "day_number_in_month":[],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[],
            "process_id": []
        }
        
        #Reading the csv file
        times_tra_table = pd.read_sql(f'SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_MONTH_NAME,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_tra WHERE PROCESS_ID = {ID}', ses_db_stg)
        
        if not times_tra_table.empty:
            for timeId,dName,dnw,dnm,cwn,cmn,cmd,ecm,cmname,cqd,cy in zip(
                times_tra_table["TIME_ID"],
                times_tra_table["DAY_NAME"],
                times_tra_table["DAY_NUMBER_IN_WEEK"],
                times_tra_table["DAY_NUMBER_IN_MONTH"],
                times_tra_table["CALENDAR_WEEK_NUMBER"],
                times_tra_table["CALENDAR_MONTH_NUMBER"],
                times_tra_table["CALENDAR_MONTH_DESC"],
                times_tra_table["END_OF_CAL_MONTH"],
                times_tra_table["CALENDAR_MONTH_NAME"],
                times_tra_table["CALENDAR_QUARTER_DESC"],
                times_tra_table["CALENDAR_YEAR"]
                ):
                
                times_dict["time_id"].append(timeId)
                times_dict["day_name"].append(dName)
                times_dict["day_number_in_week"].append(dnw)
                times_dict["day_number_in_month"].append(dnm)
                times_dict["calendar_week_number"].append(cwn)
                times_dict["calendar_month_number"].append(cmn)
                times_dict["calendar_month_desc"].append(cmd)
                times_dict["end_of_cal_month"].append(ecm),
                times_dict["calendar_month_name"].append(cmname),
                times_dict["calendar_quarter_desc"].append(cqd)
                times_dict["calendar_year"].append(cy)
                times_dict["process_id"].append(ID)
                
        if times_dict["time_id"]:
            df_times_tra = pd.DataFrame(times_dict)
            merge(table_name='times', natural_key_cols=['time_id'], dataframe= df_times_tra, db_context=ses_db_sor);
        ses_db_stg.dispose()
        ses_db_sor.dispose()

    except:
        traceback.print_exc()
    finally:
        pass