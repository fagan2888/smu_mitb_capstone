from __future__ import print_function
import time
import pandas as pd
from os import path
import intrinio_sdk as intrinio
from intrinio_sdk.rest import ApiException

DATA_FOLDER = "./data"
sp_file = "s&p500_historical_companies_1990.csv"

intrinio.ApiClient().configuration.api_key['api_key'] = 'OmE3ODc4NGMzNDY3NzgzYjRiYzczN2ZhMGY4OWQwNGE5'

df = pd.read_csv(path.join(DATA_FOLDER, sp_file))
print(df.head())

j = 1
count = df.shape[0]
results = []
for index, row in df.iterrows():
    entry = {}
    ticker = row["Ticker"]
    print(f"Ticker: {ticker}, {j}/{count}")
    try:
        response = intrinio.SecurityApi().get_security_by_id(row["ISIN Code"])
        entry["ISIN Code"] = row["ISIN Code"]
        entry["figi"] = response.figi
        entry["cik"] = response.cik
        results.append(entry)
        #print(response)
    except ApiException as e:
        print(e)
    j += 1
    time.sleep(1)

df_ret = pd.DataFrame(results)
print(df_ret.head())

df = pd.merge(df, df_ret, on="ISIN Code", how="left")
print(df.head())
df.to_csv(path.join(DATA_FOLDER, "s&p500_historical_companies_1990_expanded.csv"), index=False)
#df.to_csv(path.join(DATA_FOLDER, "s"))