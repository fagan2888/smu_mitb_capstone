import intrinio_sdk
from intrinio_sdk.rest import ApiException

class DataGetter:
    MSG_EXCEPTION = 'Exception occurred when calling %s: %s\r\n'

    TAG_INCOME_STATEMENT = 'income_statement'
    TAG_CASHFLOW_STATEMENT = 'cash_flow_statement'
    TAG_BALANCE_SHEET = 'balance_sheet_statement'
    TAG_INDUSTRIAL_TEMPLATE = 'industrial'
    TAG_FINANCIAL_TEMPLATE = 'financial'

    DATA_OPEN_PRICE = 'open_price'
    DATA_CLOSE_PRICE = 'close_price'

    FREQUENCY_DAILY = 'daily'
    FREQUENCY_WEEKLY = 'hourly'


    def __init__(self, api_key):
        intrinio_sdk.ApiClient().configuration.api_key['api_key'] = api_key
        self._company_api = intrinio_sdk.CompanyApi()
        self._security_api = intrinio_sdk.SecurityApi()
        self._fundementals_api = intrinio_sdk.FundamentalsApi()


    def _callAPI(self, func, *args, **kwargs):
        kwargs['next_page'] = ''
        while kwargs['next_page'] is not None:
            print (", ".join(f"{param}: {value}" for param, value in kwargs.items()))
            try:
                ret = func(*args, **kwargs)
                if type(ret) == tuple:
                    (obj, response_code, _) = ret
                else:
                    obj = ret
                    response_code = None
                
                kwargs['next_page'] = obj.next_page
                yield (obj, response_code)
            except ApiException as e:
                print(DataGetter.MSG_EXCEPTION %(__name__, e))


    def getSecuritiesList(self, active=True, delisted=True, with_http_info=False):
        if with_http_info:
            it = self._callAPI(
                self._security_api.get_all_securities_with_http_info,
                active=active, delisted=delisted
            )
        else:
            it = self._callAPI(
                self._security_api.get_all_securities,
                active=active, delisted=delisted
            )
        
        response = []
        (ret, res) = next(it)
        response.append(res)
        for i in it:
            (obj, res) = i
            ret.securities += obj.securities
            response.append(res)

        return (ret, response)


    def getSecuritiesByCompany(self, id, with_http_info=False):
        if with_http_info:
            it = self._callAPI(
                self._company_api.get_company_securities_with_http_info,
                id
            )
        else:
            it = self._callAPI(
                self._company_api.get_company_securities,
                id
            )
        
        response = []
        (ret, res) = next(it)
        response.append(res)
        for i in it:
            (obj, res) = i
            ret.securities += obj.securities
            response.append(res)

        return (ret, response)
    

    def _getSecurityHistoricalData(self, id, data_type, start_date, end_date, frequency, with_http_info=False):
        try:
            if with_http_info:
                ret = self._security_api.get_security_historical_data_with_http_info(id, data_type, frequency=frequency, start_date=start_date, end_date=end_date)
            else:
                ret = self._security_api.get_security_historical_data(id, data_type, frequency=frequency, start_date=start_date, end_date=end_date)
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION %(__name__, e))
        
        return ret


    def getSecuritiesPriceVolume(self, id, start_date, end_date, frequency=None, with_http_info=False):
        frequency = frequency or DataGetter.FREQUENCY_DAILY
        ret = None
        if with_http_info:
            it = self._callAPI(
                self._security_api.get_security_stock_prices_with_http_info,
                id, start_date=start_date, end_date=end_date, frequency=frequency, page_size=1000
            )
        else:
            it = self._callAPI(
                self._security_api.get_security_stock_prices,
                id, start_date=start_date, end_date=end_date, frequency=frequency, page_size=500
            )

        response = []
        (ret, res) = next(it)
        response.append(res)
        for i in it:
            (obj, res) = i
            ret.stock_prices += obj.stock_prices
            response.append(res)

        return (ret, response)


    def getSecuritiesIntradayPriceVolume(self, id, start_date, end_date, with_http_info=False):
        try:
            if with_http_info:
                ret = self._security_api.get_security_intraday_prices_with_http_info(id, start_date=start_date, end_date=end_date, source='iex')
            else:
                ret = self._security_api.get_security_intraday_prices(id, start_date=start_date, end_date=end_date, source='iex')
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION %(__name__, e))
        
        return ret


    def getCompaniesList(self, with_http_info=False):
        if with_http_info:
            it = self._callAPI(
                self._company_api.get_all_companies_with_http_info,
                has_fundamentals=True, has_stock_prices=True
            )
        else:
            it = self._callAPI(
                self._company_api.get_all_companies,
                has_fundamentals=True, has_stock_prices=True
            )
        
        response = []
        (ret, res) = next(it)
        response.append(res)
        for i in it:
            (obj, res) = i
            ret.companies += obj.companies
            response.append(res)

        return (ret, response)

    
    @classmethod
    def _getFundementalsKey(cls, company_id, statement_type, year, quater):
        return '-'.join([company_id, statement_type, str(year), 'Q%d' %quater])


    def _getFundementalsByCompanyPeriod(self, key, with_http_info=False):
        print(key)
        obj = None
        response = None
        try:
            if with_http_info:
                (obj, response, _) = self._fundementals_api.get_fundamental_standardized_financials_with_http_info(key)
            else:
                obj = self._fundementals_api.get_fundamental_standardized_financials(key)
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION % (__name__, e))
        
        return (obj, response)


    def getIncomeStatementByCompanyPeriod(self, company_id, year, quater, with_http_info=False):
        key = DataGetter._getFundementalsKey(company_id, DataGetter.TAG_INCOME_STATEMENT, year, quater)
        return self._getFundementalsByCompanyPeriod(key, with_http_info)


    def getCashflowByCompanyPeriod(self, company_id, year, quater, with_http_info=False):
        key = DataGetter._getFundementalsKey(company_id, DataGetter.TAG_CASHFLOW_STATEMENT, year, quater)
        return self._getFundementalsByCompanyPeriod(key, with_http_info)


    def getBalanceSheetByCompanyPeriod(self, company_id, year, quater, with_http_info=False):
        key = DataGetter._getFundementalsKey(company_id, DataGetter.TAG_BALANCE_SHEET, year, quater)
        return self._getFundementalsByCompanyPeriod(key, with_http_info)


    def getFundementalsByCompany(self, company_id, year, with_http_info=False):
        if with_http_info:
            it = self._callAPI(
                self._company_api.get_company_fundamentals_with_http_info,
                company_id, fiscal_year=year
            )
        else:
            it = self._callAPI(
                self._company_api.get_company_fundamentals,
                company_id, fiscal_year=year
            )
        
        response = []
        (ret, res) = next(it)
        response.append(res)
        for i in it:
            (obj, res) = i
            ret.fundamentals += obj.fundamentals
            response.append(res)

        return (ret, response)