# -*- coding: utf-8 -*-
"""
Created on Sat May  9 09:51:53 2020

@author: markg

Loads data into FinancialData database staging table
"""

import yahoo_scrape as scrape
from sqlalchemy import create_engine
from urllib import parse

# Establish connection to database
def sqlconn(db):
    params = parse.quote_plus('DRIVER={SQL Server};'
                              'SERVER=DESKTOP-9HGRDTD\SQLEXPRESS;'
                              'DATABASE='+db+';'
                              'Trusted_Connection=yes')

    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))
    return engine

# Pull data and load to database
def yahoo_hist_px2db(stock, day_end, day_start):
    # Connect to database
    engine = sqlconn('FinancialData')
    
    # Use yahoo_scrape to pull data
    hist_px = scrape.yahoo_hist_px(stock, day_end, day_start)
    
    # Load pulled data to staging
    hist_px.to_sql(name='EquityPxStage', con=engine, if_exists = 'replace', index=False)
    
    # Use SQL to merge staging to tbale
    engine.execute('''
                   INSERT INTO EquityPx
                       SELECT * FROM EquityPxStage
                       WHERE NOT EXISTS (SELECT 1 FROM EquityPx
                                         WHERE EquityPxStage.Stock = EquityPx.Stock
                                         AND EquityPxStage.Date = EquityPx.Date)
                   ''')

# Execution
if __name__ == '__main__':
    # Test
    yahoo_hist_px2db('MSFT','20200501','20200301')