from data_ingestion import DataGetter
from data_ingestion import DataParser
from pprint import pprint
from datetime import datetime
from os import path, makedirs
from time import sleep, time
import pandas as pd

BASE_DIR = './data'
PRD_KEY = 'OmE3ODc4NGMzNDY3NzgzYjRiYzczN2ZhMGY4OWQwNGE5'
SBX_KEY = 'OmRkZWJjNmI4MGMxM2NmMmU1Yjg5NTY0ODMyMzFkY2Ey'
data_getter = DataGetter(PRD_KEY)
data_parser = DataParser()

def getAllCompanies():
    (obj, _) = data_getter.getCompaniesList()
    return obj.companies


if __name__ == '__main__':
    company_list = getAllCompanies()
    sp500 = pd.read_csv('s&p500_list.csv')

    count = sp500.shape[0] #len(sp500.Symbol)
    j = 1
    companies_info = []
    for company in company_list:
        if company is None or company.ticker is None:
            continue

        if company.ticker not in set(sp500.Symbol):
            continue

        print(f"{j}/{count} - Company ID: {company.ticker}")

        (obj, _) = data_getter.getCompanyInfo(company.ticker)
        companies_info.append(obj)
        j += 1
        sleep(1)
    
    df = data_parser.parseCompanyInfo(companies_info, as_dataframe=True)
    outfile = path.join(BASE_DIR, 'company_info.csv')
    df.to_csv(outfile, index=False)
