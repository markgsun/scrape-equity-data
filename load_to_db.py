# -*- coding: utf-8 -*-
"""
Created on Sat May  9 09:51:53 2020

@author: markg

Loads data into FinancialData database staging table and merges to true table
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

# Pull yahoo data and load to database
def yahoo_hist_px2db(stock, day_end, day_start):
    # Connect to database
    engine = sqlconn('FinancialData')
    
    # Use yahoo_scrape to pull data
    hist_px = scrape.yahoo_hist_px(stock, day_end, day_start)
    
    # Equity/Index table
    if stock == '%5EGSPC':
        table = 'IndexPx'
    else:
        table = 'EquityPx'
    
    # Load pulled data to staging
    hist_px.to_sql(name=table+'Stage', con=engine, if_exists = 'replace', index=False)
    
    # Use SQL to merge staging to table
    engine.execute('''
                   INSERT INTO {0}
                       SELECT * FROM {0}Stage
                       WHERE NOT EXISTS (SELECT 1 FROM {0}
                                         WHERE {0}Stage.Stock = {0}.Stock
                                         AND {0}Stage.Date = {0}.Date)
                   '''.format(table))

# Pull wikipedia security table and load to database               
def sec_master2db(security_table):
    # Connect to database
    engine = sqlconn('FinancialData')
    
    # Load pulled data to staging
    security_table.to_sql(name='SecurityMasterStage', con=engine, if_exists = 'replace', index=False)
    
    # Use SQL to merge staging to table
    engine.execute('''
                   INSERT INTO SecurityMaster
                       SELECT * FROM SecurityMasterStage
                       WHERE NOT EXISTS (SELECT 1 FROM SecurityMaster
                                         WHERE SecurityMasterStage.Stock = SecurityMaster.Stock)
                   ''')

# Execution
if __name__ == '__main__':
    # index_hist_px2db('%5EGSPC','20200501','20170101')
    yahoo_hist_px2db('%5EGSPC','20200501','20170101')
    pass