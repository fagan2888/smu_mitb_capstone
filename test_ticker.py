import yfinance as yf

msft = yf.Ticker("MSFT")
hist = msft.history(period="max")
print ("MSFT History")
print(hist)
"""
sti = yf.Ticker("^STI")
hist = sti.history(period="max")
print("^STI History")
print(hist)
"""