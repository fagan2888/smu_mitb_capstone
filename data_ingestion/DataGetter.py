import pandas as pd
from time import sleep
import intrinio_sdk
from intrinio_sdk.rest import ApiException

from .DataParser import DataParser


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
        self._data_tag_api = intrinio_sdk.DataTagApi()
        self._historical_data_api = intrinio_sdk.HistoricalDataApi()


    def _callAPI(self, func, *args, **kwargs):
        kwargs['next_page'] = ''
        while kwargs['next_page'] is not None:
            #print (", ".join(f"{param}: {value}" for param, value in kwargs.items()))
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
    def _getFundementalsKey(cls, company_id, statement_type, year, period):
        return '-'.join([company_id, statement_type, str(year), period])


    def _getFundamentalsByCompanyPeriod(self, key, with_http_info=False):
        obj = None
        response = None
        try:
            if with_http_info:
                (obj, response, _) = self._fundementals_api.get_fundamental_standardized_financials_with_http_info(key)
            else:
                obj = self._fundementals_api.get_fundamental_standardized_financials(key)
        except ApiException as e:
            print(key)
            print(DataGetter.MSG_EXCEPTION % (__name__, e))
        
        return (obj, response)


    def getIncomeStatementByCompanyFiscalYear(self, company_id, year, with_http_info=False):
        (obj, _) = self.getFundamentalsByCompany(company_id, year=year, statement_type=DataGetter.TAG_INCOME_STATEMENT)
        sleep(0.5)

        ret = []
        res = []
        for f in obj.fundamentals:
            (details, _) = self.getFundamentalsDetails(f.id)
            key = self._getFundementalsKey(obj.company.ticker, details.statement_code, str(int(details.fiscal_year)), details.fiscal_period)
            (info, code) = self._getFundamentalsByCompanyPeriod(key, with_http_info)
            ret.append(info)
            res.append(code)
            sleep(0.5)

        return (ret, res)
    

    def getCashflowByCompanyFiscalYear(self, company_id, year, with_http_info=False):
        (obj, _) = self.getFundamentalsByCompany(company_id, year=year, statement_type=DataGetter.TAG_CASHFLOW_STATEMENT)
        sleep(0.5)

        ret = []
        res = []

        for f in obj.fundamentals:
            (details, _) = self.getFundamentalsDetails(f.id)
            key = self._getFundementalsKey(obj.company.ticker, details.statement_code, str(int(details.fiscal_year)), details.fiscal_period)
            (info, code) = self._getFundamentalsByCompanyPeriod(key, with_http_info)
            ret.append(info)
            res.append(code)
            sleep(0.5)

        return (ret, res)


    def getBalanceSheetByCompanyFiscalYear(self, company_id, year, with_http_info=False):
        (obj, _) = self.getFundamentalsByCompany(company_id, year=year, statement_type=DataGetter.TAG_BALANCE_SHEET)
        sleep(0.5)

        ret = []
        res = []

        for f in obj.fundamentals:
            (details, _) = self.getFundamentalsDetails(f.id)
            key = self._getFundementalsKey(obj.company.ticker, details.statement_code, str(int(details.fiscal_year)), details.fiscal_period)
            (info, code) = self._getFundamentalsByCompanyPeriod(key, with_http_info)
            ret.append(info)
            res.append(code)
            sleep(0.5)

        return (ret, res)


    def _getDistinctDataTags(self, type='', statement_code='', fs_template='', with_http_info=False):
        obj = None
        response = None
        try:
            if with_http_info:
                (obj, response, _) = self._data_tag_api.get_all_data_tags_with_http_info(
                    type=type, statement_code=statement_code, fs_template=fs_template, page_size=500
                )
            else:
                obj = self._data_tag_api.get_all_data_tags(
                    type=type, statement_code=statement_code, fs_template=fs_template, page_size=500
                )
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION %(__name__, e))
        
        return (obj, response)


    def _getHistoricalDataByTag(self, identifier, tag, start_date, end_date, with_http_info=False):
        if with_http_info:
            it = self._callAPI(
                self._historical_data_api.get_historical_data_with_http_info,
                identifier=identifier, tag=tag, start_date=start_date, end_date=end_date, page_size=500
            )
        else:
            it = self._callAPI(
                self._historical_data_api.get_historical_data,
                identifier=identifier, tag=tag, start_date=start_date, end_date=end_date, page_size=500
            )
        
        response = []
        (ret, res) = next(it)
        response.append(res)
        for i in it:
            (obj, res) = i
            ret.historical_data += obj.historical_data
            response.append(res)

        return (ret, response)


    def getHistoricalIncomeStatementByCompany(self, company_id, start_date, end_date, with_http_info=False):
        tags, _ = self._getDistinctDataTags(statement_code='income_statement')
        tags = DataParser.parseDistinctDataTags(tags)
        sleep(2)

        response = []
        df = None
        for t in tags:
            obj, res = self._getHistoricalDataByTag(company_id, t, start_date, end_date, with_http_info)
            response.append(res)

            df_tag = DataParser.parseHistoricalTagData(company_id, t, obj, as_dataframe=True)
            if not df_tag.empty:
                if df is None:
                    df = df_tag
                else:
                    #print(df.head())
                    #print(df_tag.head())
                    df = pd.merge(df, df_tag, on=['identifier','date'], how='outer')
            sleep(1)
        
        return (df, response)


    def getFundamentalsByCompany(self, company_id, year='', statement_type='', with_http_info=False):
        if with_http_info:
            it = self._callAPI(
                self._company_api.get_company_fundamentals_with_http_info,
                company_id, fiscal_year=year, statement_code=statement_type, page_size=1000
            )
        else:
            it = self._callAPI(
                self._company_api.get_company_fundamentals,
                company_id, fiscal_year=year, statement_code=statement_type, page_size=1000
            )
        
        response = []
        (ret, res) = next(it)
        response.append(res)
        for i in it:
            (obj, res) = i
            ret.fundamentals += obj.fundamentals
            response.append(res)

        return (ret, response)


    def getFundamentalsDetails(self, fundamental_id, with_http_info=False):
        obj = None
        response = None
        try:
            if with_http_info:
                (obj, response, _) = self._fundementals_api.get_fundamental_by_id_with_http_info(fundamental_id)
            else:
                obj = self._fundementals_api.get_fundamental_by_id(fundamental_id)
        except ApiException as e:
            print(DataGetter.MSG_EXCEPTION % (__name__, e))
        
        return (obj, response)
