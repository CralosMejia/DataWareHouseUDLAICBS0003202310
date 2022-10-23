from util.db_connection import connect
from util.properties import getProperty
from transform.transformation import obt_date
from transform.transformation import obt_month_number



import traceback
import pandas as pd


def transform_times(ID):
    try:
        #"TIME_ID","DAY_NAME","DAY_NUMBER_IN_WEEK","DAY_NUMBER_IN_MONTH","CALENDAR_WEEK_NUMBER","CALENDAR_MONTH_NUMBER","CALENDAR_MONTH_DESC","END_OF_CAL_MONTH","CALENDAR_QUARTER_DESC","CALENDAR_YEAR"
                
        name_DB = getProperty("DBSTG")
        
        ses_db_stg = connect(name_DB);
        
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
            "process_id":[]
        }
        
        #Reading the csv file
        times_ext_table = pd.read_sql('SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_ext', ses_db_stg)
        
        if not times_ext_table.empty:
            for timeId,dName,dnw,dnm,cwn,cmn,cmd,ecm,cqd,cy in zip(
                times_ext_table["TIME_ID"],
                times_ext_table["DAY_NAME"],
                times_ext_table["DAY_NUMBER_IN_WEEK"],
                times_ext_table["DAY_NUMBER_IN_MONTH"],
                times_ext_table["CALENDAR_WEEK_NUMBER"],
                times_ext_table["CALENDAR_MONTH_NUMBER"],
                times_ext_table["CALENDAR_MONTH_DESC"],
                times_ext_table["END_OF_CAL_MONTH"],
                times_ext_table["CALENDAR_QUARTER_DESC"],
                times_ext_table["CALENDAR_YEAR"]
                ):
                
                times_dict["time_id"].append(obt_date(timeId))
                times_dict["day_name"].append(dName)
                times_dict["day_number_in_week"].append(dnw)
                times_dict["day_number_in_month"].append(dnm)
                times_dict["calendar_week_number"].append(cwn)
                times_dict["calendar_month_number"].append(cmn)
                times_dict["calendar_month_desc"].append(cmd)
                times_dict["end_of_cal_month"].append(obt_date(ecm)),
                times_dict["calendar_month_name"].append(obt_month_number(cmn)),
                times_dict["calendar_quarter_desc"].append(cqd)
                times_dict["calendar_year"].append(cy)
                times_dict["process_id"].append(ID)
                
                
                
        if times_dict["time_id"]:
            df_times_tra = pd.DataFrame(times_dict)
            df_times_tra.to_sql('times_tra',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass