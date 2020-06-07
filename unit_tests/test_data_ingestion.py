from unittest import TestCase
from pprint import pprint
from http import HTTPStatus
from data_ingestion import DataGetter

class TestDataIngestion(TestCase):
    TEST_COMPANY_TICKER = 'MMM' # 3M Company
    SANDBOX_API = 'OmRkZWJjNmI4MGMxM2NmMmU1Yjg5NTY0ODMyMzFkY2Ey'


    def setUp(self):
        self.data_getter = DataGetter(TestDataIngestion.SANDBOX_API)


    def test_DataGetter_getSecuritiesList(self):
        ret = self.data_getter.getSecuritiesList(with_http_info=True)
        (obj, response_code, _) = ret
        
        pprint("Count of Securities: %d" % (len(obj.securities)))
        pprint(obj)
        assert(response_code == HTTPStatus.OK)

    
    def test_DataGetter_getSecuritiesByCompany(self):
        ret = self.data_getter.getSecuritiesByCompany(TestDataIngestion.TEST_COMPANY_TICKER, with_http_info=True)
        (obj, response_code, _) = ret

        pprint(obj.company)

        pprint("Count of Securities by %s: %d" % (TestDataIngestion.TEST_COMPANY_TICKER, len(obj.securities)))
        pprint(obj)

        assert(response_code == HTTPStatus.OK)


    def test_DataGetter_getCompaniesList(self):
        ret = self.data_getter.getCompaniesList(with_http_info=True)
        (obj, response_code, _) = ret

        pprint("Count of Companies: %d" % (len(obj.companies)))
        pprint(obj)
        
        assert(response_code == HTTPStatus.OK)
    

    def test_DataGetter_getIncomeStatementsByCompanyPeriod(self):
        ret = self.data_getter.getIncomeStatementsByCompanyPeriod(TestDataIngestion.TEST_COMPANY_TICKER, 2020, 1, with_http_info=True)
        (obj, response_code, _) = ret
        
        pprint(obj)

        assert(response_code == HTTPStatus.OK)
