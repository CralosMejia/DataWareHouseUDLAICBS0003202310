from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def extract_times():
    try:
        #"TIME_ID","DAY_NAME","DAY_NUMBER_IN_WEEK","DAY_NUMBER_IN_MONTH","CALENDAR_WEEK_NUMBER","CALENDAR_MONTH_NUMBER","CALENDAR_MONTH_DESC","END_OF_CAL_MONTH","CALENDAR_QUARTER_DESC","CALENDAR_YEAR"
                
        name_DB = getProperty("DBSTG")
        path_times_csv = getProperty("PCSVTIMES")
        
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
            "calendar_quarter_desc":[],
            "calendar_year":[],
        }
        
        #Reading the csv file
        times_csv = pd.read_csv(path_times_csv)
        
        if not times_csv.empty:
            for timeId,dName,dnw,dnm,cwn,cmn,cmd,ecm,cqd,cy in zip(
                times_csv["TIME_ID"],
                times_csv["DAY_NAME"],
                times_csv["DAY_NUMBER_IN_WEEK"],
                times_csv["DAY_NUMBER_IN_MONTH"],
                times_csv["CALENDAR_WEEK_NUMBER"],
                times_csv["CALENDAR_MONTH_NUMBER"],
                times_csv["CALENDAR_MONTH_DESC"],
                times_csv["END_OF_CAL_MONTH"],
                times_csv["CALENDAR_QUARTER_DESC"],
                times_csv["CALENDAR_YEAR"]
                ):
                
                times_dict["time_id"].append(timeId)
                times_dict["day_name"].append(dName)
                times_dict["day_number_in_week"].append(dnw)
                times_dict["day_number_in_month"].append(dnm)
                times_dict["calendar_week_number"].append(cwn)
                times_dict["calendar_month_number"].append(cmn)
                times_dict["calendar_month_desc"].append(cmd)
                times_dict["end_of_cal_month"].append(ecm)
                times_dict["calendar_quarter_desc"].append(cqd)
                times_dict["calendar_year"].append(cy)
                
                
        if times_dict["time_id"]:
            ses_db_stg.connect().execute('TRUNCATE TABLE times')
            df_times_ext = pd.DataFrame(times_dict)
            df_times_ext.to_sql('times',ses_db_stg,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass