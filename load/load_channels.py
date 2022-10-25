from util.db_connection import connect
from util.properties import getProperty
from util.sql import merge

import traceback
import pandas as pd


def load_channels(ID):
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
            "channel_class_id":[],
            "process_id":[]
        }
        
        #Reading the ext table
        channel_tra_table = pd.read_sql(f"SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID FROM channels_tra WHERE PROCESS_ID = {ID}", ses_db_stg)
        
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
                cha_dict["process_id"].append(ID)
                
                
        if cha_dict["channel_id"]:
            table_tra = pd.DataFrame(cha_dict)
            #table_tra.to_sql('channels', ses_db_sor, if_exists='append', index=False)
            merge(table_name='channels', natural_key_cols=['channel_id'], dataframe= table_tra, db_context=ses_db_sor);
        ses_db_stg.dispose()
        ses_db_sor.dispose()

    except:
        traceback.print_exc()
    finally:
        pass