# -*- coding: utf-8 -*-
'''
Created on Fri May  8 07:07:03 2020

@author: markg

Pulls historical price table from Yahoo finance for particular stock and date range
'''

# Packages
import datetime as dt
import lxml
import numpy as np
import pandas as pd
import requests
import scrape
import time
from lxml import html

# Dates
def dt2epoch(dt_datetime):
    dt_timetuple = dt_datetime.timetuple()
    dt_mktime = time.mktime(dt_timetuple)
    date_int = int(dt_mktime)
    date_str = str(date_int)
    return date_str

# Subdomain
def subdomain(symbol, start='', end='', filter='history'):
    if filter == 'history':
        subdoma='/quote/{0}/{3}?period1={1}&period2={2}&interval=1d&filter={3}&frequency=1d'.format(symbol, start, end, filter)
    else:
        subdoma = '/quote/{0}/{1}?'.format(symbol, filter)
    return subdoma

# Historical prices
def yahoo_hist_px(stock, day_end, day_start):
    # Setup
    dt_temp = dt.datetime.strptime(day_end,'%Y%m%d')
    dt_start = dt.datetime.strptime(day_start,'%Y%m%d')
    start = dt2epoch(dt_start)
    
    # Clean stock ticker
    stock = stock.replace('.','-')
    
    # Initialize price table
    px_table = None
    
    while(dt_temp>=dt_start):
        # Dates
        dt_end = dt_temp
        end = dt2epoch(dt_end)
        
        # Domain and header
        base_url = 'https://finance.yahoo.com'
        sub = subdomain(stock, start, end)
        url = base_url + sub
        header = scrape.header_function(base_url, sub)
        
        # Send request
        page = requests.get(url, headers=header)
        
        # Read webpage
        element_html = html.fromstring(page.content)
        table = element_html.xpath('//table')
        table_tree = lxml.etree.tostring(table[0], method = 'xml')
        
        # Convert table to dataframe
        table_pd = pd.read_html(table_tree)
        px_table_temp = table_pd[0].iloc[0:-1,:].set_index('Date')
        
        # If no new data, exit while loop
        if px_table_temp.empty:
            break
        # Else add append new data to price table if it exists, or create price table if not
        else:    
            dt_temp = dt.datetime.strptime(px_table_temp.index[-1],'%b %d, %Y')
            if px_table is not None:
                px_table = px_table.append(px_table_temp)
            else:
                px_table = px_table_temp
                
    # Convert to numeric
    px_table = px_table.apply(pd.to_numeric, errors = 'coerce').dropna()
    
    # Add stock name
    px_table['Stock'] = stock
    
    # Add new primary key
    px_table['Date'] = px_table.index
    px_table['Key'] = px_table['Stock']+'_'+px_table['Date']
    px_table = px_table.set_index('Key')
    
    # Remove stars from headers
    px_table.columns = px_table.columns.str.replace('*','')
    
    return px_table

# Book to Market Ratios
def yahoo_bk2mkt(stock):
    table = 'balance-sheet'
    
    # Domain and header
    base_url = 'https://finance.yahoo.com'
    sub = subdomain(stock,filter = table)
    url = base_url + sub
    header = scrape.header_function(base_url, sub)
    
    # Send request
    page = requests.get(url, headers=header)
    
    # Read webpage
    element_html = html.fromstring(page.content)
    table = element_html.xpath("//div[contains(@class, 'D(tbr)')]")
    
    # Parse elements
    table_list = []
    for row in table:
        row_el = row.xpath('./div')
        for el in row_el:
            text = str(el.xpath('.//span/text()[1]')).replace('[','').replace(']','').replace('\'','')
            table_list.append(text)
    
    # Set index names
    fin_table = pd.DataFrame(np.reshape(np.array(table_list),[-1,5])).set_index(0)
    # Set column names
    fin_table.columns = fin_table.iloc[0,]
    fin_table.drop(fin_table.index[0])
    # Drop apostrophes
    fin_table = fin_table.replace({'\'':'',',':''}, regex = True)
    
    # Calculate book/market
    mkt = pd.to_numeric(fin_table.loc['Total Capitalization',])
    bk = pd.to_numeric(fin_table.loc['Common Stock Equity',])
    bk2mkt = pd.DataFrame(data = {'bk2mkt':bk/mkt})
    
    # Add stock name
    bk2mkt['Stock'] = stock
    
    bk2mkt['Date'] = bk2mkt.index
    
    return bk2mkt


# Execution
if __name__ == '__main__':
    
    pass