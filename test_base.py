from data_ingestion import DataGetter
from datetime import datetime
from pprint import pprint
import pandas as pd

d = DataGetter('OmRkZWJjNmI4MGMxM2NmMmU1Yjg5NTY0ODMyMzFkY2Ey')
(ret, response) = d.getSecuritiesPriceVolume(
  'MMM',
  datetime(2019,1,1),
  datetime(2019,12,31),
  with_http_info=True
)

print(response)
df = pd.DataFrame.from_dict(ret.stock_prices_dict)
pprint(df)