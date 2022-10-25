from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def transform_channels(ID):
    try:
        

        
        name_DB = getProperty("DBSTG")
        
        ses_db_stg = connect(name_DB);
        
        #Dictionary for values of chanels
        cha_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[],
            "process_id":[]
        }
        
        #Reading the ext table
        channel_ext_table = pd.read_sql('SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID FROM channels_ext', ses_db_stg)
        
        if not channel_ext_table.empty:
            for id,desc,cla,cla_id in zip(
                channel_ext_table["CHANNEL_ID"],
                channel_ext_table["CHANNEL_DESC"],
                channel_ext_table["CHANNEL_CLASS"],
                channel_ext_table["CHANNEL_CLASS_ID"]
                ):
                
                cha_dict["channel_id"].append(id)
                cha_dict["channel_desc"].append(desc)
                cha_dict["channel_class"].append(cla)
                cha_dict["channel_class_id"].append(cla_id)
                cha_dict["process_id"].append(ID)
                
                
        if cha_dict["channel_id"]:
            df_channels_tra = pd.DataFrame(cha_dict)
            df_channels_tra.to_sql('channels_tra',ses_db_stg,if_exists='append',index=False)
        ses_db_stg.dispose()

                
    except:
        traceback.print_exc()
    finally:
        pass