# -*- coding: utf-8 -*-
"""
Created on Fri May  8 07:07:03 2020

@author: markg

Pulls historical price table from Yahoo finance for particular stock and date range
"""

# Packages
import datetime as dt
import lxml
import pandas as pd
import requests
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
def subdomain(symbol, start, end, filter='history'):
     subdoma="/quote/{0}/history?period1={1}&period2={2}&interval=1d&filter={3}&frequency=1d"
     subdomain = subdoma.format(symbol, start, end, filter)
     return subdomain

# Header
def header_function(subdomain):
    hdrs =  {"authority": "finance.yahoo.com",
             "method": "GET",
             "path": subdomain,
             "scheme": "https",
             "accept": "text/html",
             "accept-encoding": "gzip, deflate, br",
             "accept-language": "en-US,en;q=0.9",
             "cache-control": "no-cache",
             "cookie": "Cookie:identifier",
             "dnt": "1",
             "pragma": "no-cache",
             "sec-fetch-mode": "navigate",
             "sec-fetch-site": "same-origin",
             "sec-fetch-user": "?1",
             "upgrade-insecure-requests": "1",
             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36"}
    return hdrs

# Main function
def yahoo_hist_px(stock, ndays):

    # Setup
    dt_end = dt.datetime.today()
    dt_start = dt_end - dt.timedelta(days = ndays)
    end = dt2epoch(dt_end)
    start = dt2epoch(dt_start)
    
    sub = subdomain(stock, start, end)
    header = header_function(sub)
    
    # Send request
    base_url = 'https://finance.yahoo.com'
    url = base_url + sub
    page = requests.get(url, headers=header)
    
    # Read webpage
    element_html = html.fromstring(page.content)
    table = element_html.xpath('//table')
    table_tree = lxml.etree.tostring(table[0], method = 'xml')
    
    # Conver table to dataframe
    table_pd = pd.read_html(table_tree)
    px_table = table_pd[0].iloc[0:-1,:]
    
    return px_table
