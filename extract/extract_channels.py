from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def extract_channels():
    try:
        

        
        name_DB = getProperty("DBSTG")
        path_channels_csv = getProperty("PCSVCHANNELS")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        cha_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        
        #Reading the csv file
        channel_csv = pd.read_csv(path_channels_csv)
        
        if not channel_csv.empty:
            for id,desc,cla,cla_id in zip(
                channel_csv["CHANNEL_ID"],
                channel_csv["CHANNEL_DESC"],
                channel_csv["CHANNEL_CLASS"],
                channel_csv["CHANNEL_CLASS_ID"]
                ):
                
                cha_dict["channel_id"].append(id)
                cha_dict["channel_desc"].append(desc)
                cha_dict["channel_class"].append(cla)
                cha_dict["channel_class_id"].append(cla_id)
                
        if cha_dict["channel_id"]:
            ses_db_stg.connect().execute('TRUNCATE TABLE channels_ext')
            df_channels_ext = pd.DataFrame(cha_dict)
            df_channels_ext.to_sql('channels_ext',ses_db_stg,if_exists='append',index=False)
        ses_db_stg.dispose()

    except:
        traceback.print_exc()
    finally:
        pass