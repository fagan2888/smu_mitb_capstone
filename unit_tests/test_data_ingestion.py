from unittest import TestCase
from time import sleep
from pprint import pprint
from datetime import datetime, timedelta
from http import HTTPStatus
from data_ingestion import DataGetter

class TestDataIngestion(TestCase):
    TEST_COMPANY = 'AAPL' # Apple Company
    TEST_TICKER = 'AAPL' # Apple Ticker
    SANDBOX_API = 'OmE3ODc4NGMzNDY3NzgzYjRiYzczN2ZhMGY4OWQwNGE5'


    def setUp(self):
        self.data_getter = DataGetter(TestDataIngestion.SANDBOX_API)


    def test_DataGetter_getSecuritiesList(self):
        print("test_DataGetter_getSecuritiesList")
        obj, response = self.data_getter.getSecuritiesList(with_http_info=True)
        
        #pprint("Count of Securities: %d" % (len(obj.securities)))
        #pprint(obj)
        assert(all(_ == HTTPStatus.OK for _ in response))
        sleep(0.5)

    
    def test_DataGetter_getSecuritiesByCompany(self):
        print("test_DataGetter_getSecuritiesByCompany")
        (obj, response) = self.data_getter.getSecuritiesByCompany(TestDataIngestion.TEST_COMPANY, with_http_info=True)

        #pprint(obj.company)
        #pprint("Count of Securities by %s: %d" % (TestDataIngestion.TEST_COMPANY, len(obj.securities)))
        #pprint(obj.securities)

        assert(all(_ == HTTPStatus.OK for _ in response))
        sleep(0.5)


    def test_DataGetter_getSecuritiesPriceVolume(self):
        print("test_DataGetter_getSecuritiesPriceVolume")
        (obj, response) = self.data_getter.getSecuritiesPriceVolume(
            TestDataIngestion.TEST_COMPANY,
            datetime(2019, 1, 1),
            datetime(2020, 1, 1),
            with_http_info=True
        )

        #print(obj)
        assert(all(_ == HTTPStatus.OK for _ in response))
        sleep(0.5)


    def test_DataGetter_getSecuritiesIntradayPriceVolume(self):
        print("test_DataGetter_getSecuritiesIntradayPriceVolume")
        (obj, response) = self.data_getter.getSecuritiesIntradayPriceVolume(
            TestDataIngestion.TEST_COMPANY,
            datetime(2020, 1, 1),
            datetime(2020, 1, 31),
            with_http_info=True
        )

        print(obj)
        assert(response == HTTPStatus.OK)
        sleep(0.5)


    def test_DataGetter_getCompaniesList(self):
        print("test_DataGetter_getCompaniesList")
        (obj, response) = self.data_getter.getCompaniesList(with_http_info=True)

        #pprint("Count of Companies: %d" % (len(obj.companies)))
        #pprint(obj)
        
        assert(response == HTTPStatus.OK for _ in response)
        sleep(0.5)
    

    def test_DataGetter_getCompanyInfo(self):
        print("test_DataGetter_getCompanyInfo")
        (obj, response) = self.data_getter.getCompanyInfo(TestDataIngestion.TEST_COMPANY, with_http_info=True)

        print(obj)

        assert(response == HTTPStatus.OK)
        sleep(0.5)


    def test_DataGetter_getIncomeStatementByCompanyFiscalYear(self):
        print("test_DataGetter_getIncomeStatementByCompanyFiscalYear")
        (obj, response) = self.data_getter.getIncomeStatementByCompanyFiscalYear(TestDataIngestion.TEST_COMPANY, 2019, with_http_info=True)

        #pprint(obj)

        assert(all(_ == HTTPStatus.OK for _ in response))
        sleep(0.5)


    def test_DataGetter_getCashflowByCompanyFiscalYear(self):
        print("test_DataGetter_getCashflowByCompanyFiscalYear")
        (obj, response) = self.data_getter.getCashflowByCompanyFiscalYear(TestDataIngestion.TEST_COMPANY, 2019, with_http_info=True)

        #pprint(obj)

        assert(all(_ == HTTPStatus.OK for _ in response))
        sleep(0.5)


    def test_DataGetter_getBalanceSheetByCompanyFiscalYear(self):
        print("test_DataGetter_getBalanceSheetByCompanyFiscalYear")
        (obj, response) = self.data_getter.getBalanceSheetByCompanyFiscalYear(TestDataIngestion.TEST_COMPANY, 2019, with_http_info=True)

        #pprint(obj)

        assert(all(_ == HTTPStatus.OK for _ in response))
        sleep(0.5)


    def test_DataGetter_getHistoricalIncomeStatementByCompany(self):
        print("test_DataGetter_getHistoricalIncomeStatementByCompany")
        (obj, response) = self.data_getter.getHistoricalIncomeStatementByCompany(TestDataIngestion.TEST_COMPANY, '2019-01-01', '2020-01-01', with_http_info=True)

        print(len(obj.intraday_prices_dict))
        print(obj)
        

        assert(all(_ == HTTPStatus.OK for _ in response))
        sleep(0.5)


    def test_DataGetter_getFundamentalsByCompany(self):
        print("test_DataGetter_getFundamentalsByCompany")
        (obj, response) = self.data_getter.getFundamentalsByCompany(TestDataIngestion.TEST_COMPANY, 2019, with_http_info=True)

        #print(obj)
        assert(all(_ == HTTPStatus.OK for _ in response))
        sleep(0.5)


    def test_DataGetter_getFundamentalsDetails(self):
        print("test_DataGetter_getFundamentalsDetails")
        (obj, _) = self.data_getter.getFundamentalsByCompany(TestDataIngestion.TEST_COMPANY, 2019, DataGetter.TAG_BALANCE_SHEET)

        fun = obj.fundamentals[0]
        key = '-'.join([obj.company.ticker, fun.statement_code, str(int(fun.fiscal_year)), fun.fiscal_period])
        (obj, response) = self.data_getter._getFundamentalsByCompanyPeriod(key, with_http_info=True)

        #print(obj)
        assert(response == HTTPStatus.OK)
        sleep(0.5)
