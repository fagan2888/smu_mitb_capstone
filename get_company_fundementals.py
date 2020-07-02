from data_ingestion import DataGetter
from data_ingestion import DataParser
from pprint import pprint
from datetime import datetime
from os import path
import pandas as pd

BASE_DIR = './data/fundamentals'
START_DATE = datetime(2008,1,1)
END_DATE = datetime.today()
API_KEY = 'OmRkZWJjNmI4MGMxM2NmMmU1Yjg5NTY0ODMyMzFkY2Ey'
data_getter = DataGetter(API_KEY)
data_parser = DataParser()


def getAllCompanies():
    (obj, _) = data_getter.getCompaniesList()
    return obj.companies


def getCompanyfundamentals(id):
    income_statements = {}
    balance_sheets = {}
    cashflows = {}

    for y in range(START_DATE.year, END_DATE.year):
        for q in range(4):
            (income_statements[f"{y}_{q+1}"], _) = data_getter.getIncomeStatementByCompanyPeriod(id, y, q+1)
            (balance_sheets[f"{y}_{q+1}"], _) = data_getter.getBalanceSheetByCompanyPeriod(id, y, q+1)
            (cashflows[f"{y}_{q+1}"], _) = data_getter.getCashflowByCompanyPeriod(id, y, q+1)
    
    return income_statements, balance_sheets, cashflows


def processCompanyFundementals(income_statements):
    df = pd.DataFrame()
    for _, statement in income_statements.items():
        for _, data in statement.items():
            if data is None:
                continue

            f = data_parser.parseFundamentals(data, as_dataframe = True)
            f['join_col'] = 1
            d = data_parser.parseStandardizeFinanacials(data, as_dataframe=True)
            d['join_col'] = 1
            d = pd.merge(f, d, on='join_col', how='right')
            d.drop(columns='join_col', inplace=True)
            df = df.append(d)
    
    return df


if __name__ == '__main__':
    company_list = getAllCompanies()

    income_statements = {}
    balance_sheets = {}
    cashflows = {}
    for company in company_list:
        (i, b, c) = getCompanyfundamentals(company.ticker)
        income_statements[company.ticker] = i
        balance_sheets[company.ticker] = b
        cashflows[company.ticker] = c

    income_statements_df = processCompanyFundementals(income_statements)
    print(income_statements_df.head())
    print(income_statements_df.shape)
    out_file = path.join(BASE_DIR, 'income_statements.csv')
    income_statements_df.to_csv(out_file, index=False)

    balance_sheets_df = processCompanyFundementals(balance_sheets)
    print(balance_sheets_df.head())
    print(balance_sheets_df.shape)
    out_file = path.join(BASE_DIR, 'balance_sheets.csv')
    balance_sheets_df.to_csv(out_file, index=False)

    cashflows_df = processCompanyFundementals(cashflows)
    print(cashflows_df.head())
    print(cashflows_df.shape)
    out_file = path.join(BASE_DIR, 'cashflows.csv')
    cashflows_df.to_csv(out_file, index=False)
