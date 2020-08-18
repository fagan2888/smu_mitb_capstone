import pandas as pd
from os import path, listdir

BASE_DIR = './data/fundamentals'
BALANCE_SHEET_CSV = '{0}_balance_sheets_wide.csv'
INCOME_STATEMENT_CSV = '{0}_income_statements_wide.csv'
CASHFLOW_CSV = '{0}_cashflows_wide.csv'

if __name__ == '__main__':
    overall_balance_sheets = pd.DataFrame()
    overall_income_statements = pd.DataFrame()
    overall_cashflows = pd.DataFrame()

    companies_list = listdir(BASE_DIR)
    for company in companies_list:
        company_dir = path.join(BASE_DIR, company)

        if path.isfile(path.join(company_dir, BALANCE_SHEET_CSV.format(company))):
            balance_sheet = pd.read_csv(path.join(company_dir, BALANCE_SHEET_CSV.format(company)))
            balance_sheet = balance_sheet[['company_name', 'company_ticker', 'year', 'quarter']]
            balance_sheet['year'] = balance_sheet['year'].astype(int).astype(str)
            balance_sheet['period'] = balance_sheet[['year', 'quarter']].agg('-'.join, axis=1)
            overall_balance_sheets = overall_balance_sheets.append(balance_sheet)
            #print(balance_sheet.head())

        if path.isfile(path.join(company_dir, INCOME_STATEMENT_CSV.format(company))):
            income_statement = pd.read_csv(path.join(company_dir, INCOME_STATEMENT_CSV.format(company)))
            income_statement = income_statement[['company_name', 'company_ticker', 'year', 'quarter']]
            income_statement['year'] = income_statement['year'].astype(int).astype(str)
            income_statement['period'] = income_statement[['year', 'quarter']].agg('-'.join, axis=1)
            overall_income_statements = overall_income_statements.append(income_statement)
            #print(income_statement.head())

        if path.isfile(path.join(company_dir, CASHFLOW_CSV.format(company))):
            cashflow = pd.read_csv(path.join(company_dir, CASHFLOW_CSV.format(company)))
            cashflow = cashflow[['company_name', 'company_ticker', 'year', 'quarter']]
            cashflow['year'] = cashflow['year'].astype(int).astype(str)
            cashflow['period'] = cashflow[['year', 'quarter']].agg('-'.join, axis=1)
            overall_cashflows = overall_cashflows.append(cashflow)
            #print(cashflow.head())
        #break

    overall_balance_sheets['exists'] = 1
    overall_balance_sheets = overall_balance_sheets.pivot_table(fill_value=0, values='exists', index=['company_name', 'company_ticker'], columns=['period'])
    overall_balance_sheets = overall_balance_sheets.reset_index()
    print(overall_balance_sheets)
    overall_balance_sheets.to_csv(path.join(BASE_DIR, 'balance_sheets_availability.csv'), index=False)

    overall_income_statements['exists'] = 1
    overall_income_statements = overall_income_statements.pivot_table(fill_value=0, values='exists', index=['company_name', 'company_ticker'], columns=['period'])
    overall_income_statements = overall_income_statements.reset_index()
    print(overall_income_statements)
    overall_income_statements.to_csv(path.join(BASE_DIR, 'income_statements_availability.csv'), index=False)

    overall_cashflows['exists'] = 1
    overall_cashflows = overall_cashflows.pivot_table(fill_value=0, values='exists', index=['company_name', 'company_ticker'], columns=['period'])
    overall_cashflows = overall_cashflows.reset_index()
    print(overall_cashflows)
    overall_cashflows.to_csv(path.join(BASE_DIR, 'cashflow_availability.csv'), index=False)
