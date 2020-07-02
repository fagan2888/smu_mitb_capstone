import pandas as pd
from . import DataGetter

class DataParser():

    def parseFundamentals(self, data, as_dataframe=False):
        df = []
        if hasattr(data, 'fundamental'):
            d = {}
            d['company_name'] = data.fundamental.company.name
            d['company_ticker'] = data.fundamental.company.ticker
            d['company_lei'] = data.fundamental.company.lei
            d['company_cik'] = data.fundamental.company.cik
            d['year'] = data.fundamental.fiscal_year
            d['quarter'] = data.fundamental.fiscal_period
            d['start_date'] = data.fundamental.start_date
            d['end_date'] = data.fundamental.end_date
            d['filing_date'] = data.fundamental.filing_date
            df.append(d)
        
        return pd.DataFrame(df) if as_dataframe else df


    def parseStandardizeFinanacials(self, data, as_dataframe=False):
        df = []
        if hasattr(data, 'standardized_financials'):
            for var in data.standardized_financials:
                d = {}
                d['parent'] = var.data_tag.parent
                d['tag'] = var.data_tag.tag
                d['financial_name'] = var.data_tag.name
                d['factor'] = var.data_tag.factor
                d['balance'] = var.data_tag.balance
                d['unit'] = var.data_tag.unit
                d['value'] = var.value
                df.append(d)
        
        return pd.DataFrame(df) if as_dataframe else df    
        

