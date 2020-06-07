import intrinio_sdk
from intrinio_sdk.rest import ApiException

class DataGetter:
    MSG_EXCEPTION = 'Exception occurred when calling %s: %s\r\n'

    TAG_INCOME_STATEMENT = 'income_statement'
    TAG_CASHFLOW_STATEMENT = 'cash_flow_statement'
    TAG_BALANCE_SHEET = 'balance_sheet_statement'
    TAG_INDUSTRIAL_TEMPLATE = 'industrial'
    TAG_FINANCIAL_TEMPLATE = 'financial'


    def __init__(self, api_key):
        intrinio_sdk.ApiClient().configuration.api_key['api_key'] = api_key
        self._company_api = intrinio_sdk.CompanyApi()
        self._security_api = intrinio_sdk.SecurityApi()
        self._fundementals_api = intrinio_sdk.FundamentalsApi()


    def getCompaniesList(self, with_http_info=False):
        try:
            if with_http_info:
                ret = self._company_api.get_all_companies_with_http_info(has_fundamentals=True, has_stock_prices=True)
            else:
                ret = self._company_api.get_all_companies(has_fundamentals=True, has_stock_prices=True)
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION % (__name__, e))

        return ret


    def getSecuritiesList(self, active=True, delisted=True, with_http_info=False):
        try:
            if with_http_info:
                ret = self._security_api.get_all_securities_with_http_info(active=active, delisted=delisted)
            else:
                ret = self._security_api.get_all_securities(active=active, delisted=delisted)
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION % (__name__, e))

        return ret


    def getSecuritiesByCompany(self, id, with_http_info=False):
        try:
            if with_http_info:
                ret = self._company_api.get_company_securities_with_http_info(id)
            else:
                ret = self._company_api.get_company_securities(id)
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION %(__name__, e))
        
        return ret


    def getIncomeStatementsByCompanyPeriod(self, company_id, year, quater, with_http_info=False):
        key = '-'.join([company_id, DataGetter.TAG_INCOME_STATEMENT, str(year), 'Q%d' %quater])
        print(key)
        try:
            if with_http_info:
                ret = self._fundementals_api.get_fundamental_standardized_financials_with_http_info(key)
            else:
                ret = self._fundementals_api.get_fundamental_standardized_financials(key)
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION % (__name__, e))

        return ret
