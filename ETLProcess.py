import traceback

from extract.extract import extractions
from load.loads import loads
from transform.transforms import transformations

try:
    extractions()
    transformations()
    loads()
except:
    traceback.print_exc()
finally:
    pass
