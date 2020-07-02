from __future__ import print_function
import time
import intrinio_sdk
from intrinio_sdk.rest import ApiException
from pprint import pprint

intrinio_sdk.ApiClient().configuration.api_key['api_key'] = 'OmRkZWJjNmI4MGMxM2NmMmU1Yjg5NTY0ODMyMzFkY2Ey'

security_api = intrinio_sdk.SecurityApi()

identifier = 'MMM' # str | A Security identifier (Ticker, FIGI, ISIN, CUSIP, Intrinio ID)
source = '' # str | Return intraday prices from the specified data source (optional)
start_date = '' # date | Return intraday prices starting at the specified date (optional)
start_time = '' # str | Return intraday prices starting at the specified time on the start_date (timezone is UTC) (optional)
end_date = '' # date | Return intraday prices stopping at the specified date (optional)
end_time = '' # str | Return intraday prices stopping at the specified time on the end_date (timezone is UTC) (optional)
page_size = 100 # int | The number of results to return (optional) (default to 100)
next_page = '' # str | Gets the next page of data from a previous API call (optional)

try:
  api_response = security_api.get_security_intraday_prices(identifier, source=source, start_date=start_date, start_time=start_time, end_date=end_date, end_time=end_time, page_size=page_size, next_page=next_page)
  pprint(api_response)
except ApiException as e:
  print("Exception when calling SecurityApi->get_security_intraday_prices: %s\r\n" % e)