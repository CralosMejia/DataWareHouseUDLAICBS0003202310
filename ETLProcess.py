import traceback

from extract.extract import extractions
from transform.transforms import transformations

try:
   extractions();
   transformations();
except:
    traceback.print_exc()
finally:
    pass