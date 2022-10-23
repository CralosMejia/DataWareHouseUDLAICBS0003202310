from util.db_connection import connect
from util.properties import getProperty

import traceback
import pandas as pd


def load_channels():
    try:
        

        
        name_DB_stg = getProperty("DBSTG")
        name_DB_sor = getProperty("DBSOR")
        
        
        ses_db_stg = connect(name_DB_stg);
        ses_db_sor = connect(name_DB_sor);
        
        
        #Dictionary for values of chanels
        cha_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        
        #Reading the ext table
        channel_tra_table = pd.read_sql('SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID FROM channels_tra', ses_db_stg)
        
        if not channel_tra_table.empty:
            for id,desc,cla,cla_id in zip(
                channel_tra_table["CHANNEL_ID"],
                channel_tra_table["CHANNEL_DESC"],
                channel_tra_table["CHANNEL_CLASS"],
                channel_tra_table["CHANNEL_CLASS_ID"]
                ):
                
                cha_dict["channel_id"].append(id)
                cha_dict["channel_desc"].append(desc)
                cha_dict["channel_class"].append(cla)
                cha_dict["channel_class_id"].append(cla_id)
                
        if cha_dict["channel_id"]:
            df_channels_load = pd.DataFrame(cha_dict)
            df_channels_load.to_sql('channels',ses_db_sor,if_exists='append',index=False)
                
    except:
        traceback.print_exc()
    finally:
        pass