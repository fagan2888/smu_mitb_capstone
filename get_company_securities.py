from data_ingestion import DataGetter
from data_ingestion import DataParser
from pprint import pprint
from datetime import datetime
from time import sleep
from os import path
import pandas as pd

BASE_DIR = './data'
PRICE_DIR = path.join(BASE_DIR, 'daily_price')
START_YEAR = 1968
END_YEAR = 2020
API_KEY = 'OmE3ODc4NGMzNDY3NzgzYjRiYzczN2ZhMGY4OWQwNGE5'
data_getter = DataGetter(API_KEY)
data_parser = DataParser()


def getAllCompanies():
    (obj, _) = data_getter.getCompaniesList()
    return obj.companies


def getCompanySecurities(id):
    (obj, _) = data_getter.getSecuritiesByCompany(id)
    return obj.securities


def getSecuritiesHistoricalPrices(id, start_year, end_year):
    daily_prices = []
    for y in range(start_year, end_year+1, 1):
        print(y)
        start_date = datetime(y, 1, 1)
        end_date = datetime(y, 12, 31)
        (obj, _) = data_getter.getSecuritiesPriceVolume(id, start_date, end_date)
        daily_prices.extend(obj.stock_prices_dict)
        sleep(30)
    return daily_prices


if __name__ == '__main__':
    securities_df = pd.DataFrame([])
    securities_csv = path.join(BASE_DIR, 'securities_list.csv')
    if not path.isfile(securities_csv):
        company_list = getAllCompanies()

        # Securities List
        securities_list = []
        for company in company_list:
            identifer = company.id
            securities_list.extend(getCompanySecurities(identifer))
        
        securities_list_dict = []
        for sec in securities_list:
            securities_list_dict.append(sec.to_dict())
        print(type(securities_list_dict[0]))
        
        securities_df = pd.DataFrame(securities_list_dict)
        pprint(securities_df.head())
        pprint(securities_df.shape)
        securities_df.to_csv(securities_csv, index=False)

        sleep(60)
    else:
        securities_df = pd.read_csv(securities_csv)

    for ticker in securities_df.composite_ticker:
        out_file = path.join(PRICE_DIR, f"{ticker.replace(':', '_')}_daily_price.csv")
        if not path.isfile(out_file):
            print(ticker)
            price = getSecuritiesHistoricalPrices(ticker, START_YEAR, END_YEAR)
            df = pd.DataFrame(price)
            print(df.head())
            print(df.shape)
            df.to_csv(out_file, index=False)
