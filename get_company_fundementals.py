from data_ingestion import DataGetter
from data_ingestion import DataParser
from pprint import pprint
from datetime import datetime
from os import path
from time import sleep
import pandas as pd

BASE_DIR = './data/fundamentals'
START_DATE = datetime(2007,1,1)
END_DATE = datetime.today()
API_KEY = 'OmE3ODc4NGMzNDY3NzgzYjRiYzczN2ZhMGY4OWQwNGE5'
data_getter = DataGetter(API_KEY)
data_parser = DataParser()


def getAllCompanies():
    (obj, _) = data_getter.getCompaniesList()
    return obj.companies


def getCompanyfundamentals(id, get_income_statements=True, get_balance_sheets=True, get_cashflows=True):
    income_statements = {}
    balance_sheets = {}
    cashflows = {}
    call = (get_income_statements or get_balance_sheets or get_cashflows)

    if call:
        for y in range(START_DATE.year, END_DATE.year):
            for q in range(4):
                if get_income_statements:
                    (income_statements[f"{y}_{q+1}"], _) = data_getter.getIncomeStatementByCompanyPeriod(id, y, q+1)
                if get_balance_sheets:
                    (balance_sheets[f"{y}_{q+1}"], _) = data_getter.getBalanceSheetByCompanyPeriod(id, y, q+1)
                if get_cashflows:
                    (cashflows[f"{y}_{q+1}"], _) = data_getter.getCashflowByCompanyPeriod(id, y, q+1)
                sleep(1.5)
    
    return income_statements, balance_sheets, cashflows


def processCompanyFundementals(statements):
    df = pd.DataFrame()
    for _, data in statements.items():
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

    sleep(60)

    income_statements = {}
    balance_sheets = {}
    cashflows = {}
    count = len(company_list)
    j = 1
    for company in company_list:
        if company is None or company.ticker is None:
            continue

        print(f"{j}/{count} - Company ID: {company.ticker}")
        i_outfile = path.join(BASE_DIR, f'{company.ticker}_income_statements.csv')
        b_outfile = path.join(BASE_DIR, f'{company.ticker}_balanace_sheets.csv')
        c_outfile = path.join(BASE_DIR, f'{company.ticker}_cashflows.csv')

        (i, b, c) = getCompanyfundamentals(
            company.ticker,
            not path.isfile(i_outfile),
            not path.isfile(b_outfile),
            not path.isfile(c_outfile)
        )
        income_statements_df = processCompanyFundementals(i)
        income_statements_df.to_csv(i_outfile, index=False)

        balance_sheets_df = processCompanyFundementals(b)
        balance_sheets_df.to_csv(b_outfile, index=False)

        cashflows_df = processCompanyFundementals(c)
        cashflows_df.to_csv(c_outfile, index=False)

        j+=1

    """
    for _, statement in income_statements.items():
        income_statements_df = processCompanyFundementals(statement)
        print(income_statements_df.head())
        print(income_statements_df.shape)
        out_file = path.join(BASE_DIR, 'income_statements.csv')
        income_statements_df.to_csv(out_file, index=False)
    """

    """
    balance_sheets_df = processCompanyFundementals(balance_sheets)
    print(balance_sheets_df.head())
    print(balance_sheets_df.shape)
    out_file = path.join(BASE_DIR, 'balance_sheets.csv')
    balance_sheets_df.to_csv(out_file, index=False)
    """

    """
    cashflows_df = processCompanyFundementals(cashflows)
    print(cashflows_df.head())
    print(cashflows_df.shape)
    out_file = path.join(BASE_DIR, 'cashflows.csv')
    cashflows_df.to_csv(out_file, index=False)
    """
