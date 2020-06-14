from unittest import TestCase
from pprint import pprint
from datetime import datetime, timedelta
from http import HTTPStatus
from data_ingestion import DataGetter

class TestDataIngestion(TestCase):
    TEST_COMPANY = 'MMM' # 3M Company
    TEST_TICKER = 'MMM:GR' # 3M Ticker
    SANDBOX_API = 'OmRkZWJjNmI4MGMxM2NmMmU1Yjg5NTY0ODMyMzFkY2Ey'


    def setUp(self):
        self.data_getter = DataGetter(TestDataIngestion.SANDBOX_API)


    def test_DataGetter_getSecuritiesList(self):
        obj, response = self.data_getter.getSecuritiesList(with_http_info=True)
        
        pprint("Count of Securities: %d" % (len(obj.securities)))
        pprint(obj)
        assert(all(_ == HTTPStatus.OK for _ in response))

    
    def test_DataGetter_getSecuritiesByCompany(self):
        (obj, response) = self.data_getter.getSecuritiesByCompany(TestDataIngestion.TEST_COMPANY, with_http_info=True)

        pprint(obj.company)
        pprint("Count of Securities by %s: %d" % (TestDataIngestion.TEST_COMPANY, len(obj.securities)))
        pprint(obj.securities)

        assert(all(_ == HTTPStatus.OK for _ in response))


    def test_DataGetter_getSecuritiesPriceVolume(self):
        (obj, response) = self.data_getter.getSecuritiesPriceVolume(
            TestDataIngestion.TEST_COMPANY,
            datetime(2019, 1, 1),
            datetime(2020, 1, 1),
            with_http_info=True
        )

        print(obj)
        assert(all(_ == HTTPStatus.OK for _ in response))


    def test_DataGetter_getCompaniesList(self):
        (obj, response) = self.data_getter.getCompaniesList(with_http_info=True)

        pprint("Count of Companies: %d" % (len(obj.companies)))
        pprint(obj)
        
        assert(response == HTTPStatus.OK for _ in response)
    

    def test_DataGetter_getIncomeStatementByCompanyPeriod(self):
        (obj, response) = self.data_getter.getIncomeStatementByCompanyPeriod(TestDataIngestion.TEST_COMPANY, 2020, 1, with_http_info=True)

        pprint(obj)

        assert(response == HTTPStatus.OK)


    def test_DataGetter_getCashflowByCompanyPeriod(self):
        (obj, response) = self.data_getter.getCashflowByCompanyPeriod(TestDataIngestion.TEST_COMPANY, 2020, 1, with_http_info=True)

        pprint(obj)

        assert(response == HTTPStatus.OK)


    def test_DataGetter_getBalanceSheetByCompanyPeriod(self):
        (obj, response) = self.data_getter.getBalanceSheetByCompanyPeriod(TestDataIngestion.TEST_COMPANY, 2020, 1, with_http_info=True)

        pprint(obj)

        assert(response == HTTPStatus.OK)
