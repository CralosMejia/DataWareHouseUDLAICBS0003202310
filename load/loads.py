from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times








from util.db_connection import connect
from util.properties import getProperty


import traceback
import pandas as pd


try:
    
    #LOAD
    load_channels()
    load_countries()
    load_customers()
    load_products()
    load_promotions()
    load_times()
    load_sales()
    
    create_process_ID()
except:
    traceback.print_exc()
finally:
    pass