import pandas as pd
from os import path, listdir, rename

BASE_DIR = './data/fundamentals'
INCOME = '{}_income_statements.csv'
INCOME_WIDE = '{}_income_statements_wide.csv'
BALANCE = '{}_balance_sheets.csv'
BALANCE_WIDE = '{}_balance_sheets_wide.csv'
BALANCE_INCORRECT = '{}_balanace_sheets.csv'
CASHFLOW = '{}_cashflows.csv'
CASHFLOW_WIDE = '{}_cashflows_wide.csv'

def process_df(df):
    long_cols = ['company_ticker','year','quarter', 'tag', 'value']
    company_cols = ['company_name', 'company_ticker', 'company_lei', 'company_cik', 'year', 'quarter', 'start_date', 'end_date', 'filing_date']
    df_long = df[long_cols]
    df_company = df[company_cols].drop_duplicates()
    
    df_long = df_long.pivot_table(index=['company_ticker', 'year', 'quarter'], columns='tag').reset_index()
    df_long.columns = [' '.join(col).strip().replace('value ', '') for col in df_long.columns.values]
    df_wide = pd.merge(df_company, df_long, on=['company_ticker', 'year', 'quarter'], how='left')
    return df_wide


companies = listdir(BASE_DIR)
for c in companies:
    company_dir = path.join(BASE_DIR, c)

    if path.isfile(path.join(company_dir, BALANCE_INCORRECT.format(c))) and not path.isfile(path.join(company_dir, BALANCE.format(c))):
        rename(
            path.join(company_dir, BALANCE_INCORRECT.format(c)),
            path.join(company_dir, BALANCE.format(c))
        )

    try:
        df_income = pd.read_csv(path.join(company_dir, INCOME.format(c)))
        df_income_wide = process_df(df_income)
        df_income_wide.to_csv(path.join(company_dir, INCOME_WIDE.format(c)), index=False)
    except Exception as e:
        print(e)
        print("Exception:", path.join(company_dir, INCOME.format(c)))
        pass

    try:
        df_balance = pd.read_csv(path.join(company_dir, BALANCE.format(c)))
        df_balance_wide = process_df(df_balance)
        df_balance_wide.to_csv(path.join(company_dir, BALANCE_WIDE.format(c)), index=False)
    except Exception as e:
        print(e)
        print("Exception:", path.join(company_dir, BALANCE.format(c)))
        pass

    try:
        df_cashflow = pd.read_csv(path.join(company_dir, CASHFLOW.format(c)))
        df_cashflow_wide = process_df(df_cashflow)
        df_cashflow_wide.to_csv(path.join(company_dir, CASHFLOW_WIDE.format(c)), index=False)
    except Exception as e:
        print(e)
        print("Exception:", path.join(company_dir, CASHFLOW.format(c)))
        pass
