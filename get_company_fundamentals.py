from data_ingestion import DataGetter
from data_ingestion import DataParser
from pprint import pprint
from datetime import datetime
from os import path, makedirs
from time import sleep, time
import pandas as pd

BASE_DIR = './data/fundamentals'
START_DATE = datetime(2007,1,1)
END_DATE = datetime.today()
PRD_KEY = 'OmE3ODc4NGMzNDY3NzgzYjRiYzczN2ZhMGY4OWQwNGE5'
SBX_KEY = 'OmRkZWJjNmI4MGMxM2NmMmU1Yjg5NTY0ODMyMzFkY2Ey'
data_getter = DataGetter(PRD_KEY)
data_parser = DataParser()


def getAllCompanies():
    (obj, _) = data_getter.getCompaniesList()
    return obj.companies


def getCompanyfundamentals(id, outfolder, get_income_statements=True, get_balance_sheets=True, get_cashflows=True):
    income_statements = []
    balance_sheets = []
    cashflows = []
    call = (get_income_statements or get_balance_sheets or get_cashflows)

    if call:
        for y in range(START_DATE.year, END_DATE.year+1):
            start_time = time()
            if get_income_statements:
                (i, _) = data_getter.getIncomeStatementByCompanyFiscalYear(id, y)
                income_statements.extend(i)
                sleep(1)
            
            if get_balance_sheets:
                (b, _) = data_getter.getBalanceSheetByCompanyFiscalYear(id, y)
                balance_sheets.extend(b)
                sleep(1)
            
            if get_cashflows:
                (c, _) = data_getter.getCashflowByCompanyFiscalYear(id, y)
                cashflows.extend(c)
                sleep(1)
            end_time = time()
            print(f"{id}-{y}: {end_time-start_time}")
    
    return income_statements, balance_sheets, cashflows


def processCompanyFundementals(statements):
    df = pd.DataFrame()
    for data in statements:
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
    sp500 = pd.read_csv('./data/s&p500_historical_companies_1990.csv')
    tickers = []
    for t in list(sp500.Ticker):
        tickers.append(t.split(' ')[0])
    tickers = set(tickers)
    #print(sp500.head())

    sleep(60)

    count = sp500.shape[0] #len(sp500.Symbol)
    j = 1
    for company in company_list:
        if company is None or company.ticker is None:
            continue

        if company.ticker not in tickers:
            continue

        print(f"{j}/{count} - Company ID: {company.ticker}")
        outfolder = path.join(BASE_DIR, f'{company.ticker}')
        if not path.isdir(outfolder):
            makedirs(outfolder)

        i_outfile = path.join(outfolder, f'{company.ticker}_income_statements.csv')
        b_outfile = path.join(outfolder, f'{company.ticker}_balance_sheets.csv')
        c_outfile = path.join(outfolder, f'{company.ticker}_cashflows.csv')
        get_income = not path.isfile(i_outfile)
        get_balance = not path.isfile(b_outfile)
        get_cashflow = not path.isfile(c_outfile)

        (i, b, c) = getCompanyfundamentals(
            company.ticker,
            outfolder,
            get_income,
            get_balance,
            get_cashflow
        )

        if get_income:
            income_statements_df = processCompanyFundementals(i)
            income_statements_df.to_csv(i_outfile, index=False)

        if get_balance:
            balance_sheets_df = processCompanyFundementals(b)
            balance_sheets_df.to_csv(b_outfile, index=False)

        if get_cashflow:
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
