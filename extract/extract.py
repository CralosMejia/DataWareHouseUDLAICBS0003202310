from extract.extract_countries import extract_countries
from extract.extract_channels import extract_channels
from extract.extract_customers import extract_customers
from extract.extract_products import extract_products
from extract.extract_promotions import extract_promotions
from extract.extract_sales import extract_sales
from extract.extract_times import extract_times

import traceback

def extractions():
    try:
        #EXTRACT
        extract_channels()
        extract_countries()
        extract_customers()
        extract_products()
        extract_promotions()
        extract_sales()
        extract_times()
    except:
        traceback.print_exc()
    finally:
        pass