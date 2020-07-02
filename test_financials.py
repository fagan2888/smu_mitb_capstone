import yfinance as yf
from pprint import pprint

msft = yf.Ticker("C6L.SI")
pprint("SIA financials")
pprint(msft.get_cashflow())
