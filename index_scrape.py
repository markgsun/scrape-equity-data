# -*- coding: utf-8 -*-
'''
Created on Sun May 10 10:12:05 2020

@author: markg

Pulls index consituents from Wikipedia and uses yahoo_scrape to pull all 
constituent data
'''

# Packages
import lxml
import pandas as pd
import requests
import time
from lxml import html

# Personal modules
import scrape
import load_to_db

# Scrape constituent table from wikipedia
def wiki_index_cons(idx):
    # URL
    base_url = 'https://en.wikipedia.org'
    sub = '/wiki/List_of_'+idx.replace('&','%26').replace(' ','_')+'_companies'
    url = base_url + sub
    
    # Header
    header = scrape.header_function(base_url, sub)
    
    # Send request
    page = requests.get(url, headers=header)
    
    # Read webpage
    element_html = html.fromstring(page.content)
    table = element_html.xpath("//table[@id='constituents']")
    table_tree = lxml.etree.tostring(table[0], method = 'xml')
    
    # Convert table to dataframe
    table_pd = pd.read_html(table_tree)[0]
    
    return table_pd

# Scrape historical prices from yahoo for index constituents
def index_hist_price(idx, day_end, day_start): 
    # Start timer
    start = time.perf_counter()
    
    # Get list of constituents
    constituents = wiki_index_cons(idx)
    # Get ticker column
    tic_index = [i for i, elem in enumerate(constituents.columns) if 'symbol' in elem.lower()][0]
    
    # Run yahoo_hist_px2db for each constituent
    for i in range(0,len(constituents)):
        # Load ticker
        tic = constituents.iloc[i,tic_index]
        try:
            print(tic)
            load_to_db.yahoo_hist_px2db(tic, day_end, day_start)
        except:
            print('Ticker unavailable')
    
    # End timer
    end = time.perf_counter()
    print('Elapsed time: '+str(end-start)+' seconds')

# Scrape constituent information from wikipedia
def sec_master(idx):
    # Get list of constituents
    constituents = wiki_index_cons(idx)
    
    # Get industry column
    tic_index = [i for i, elem in enumerate(constituents.columns) if 'symbol' in elem.lower()][0]
    sec_index = [i for i, elem in enumerate(constituents.columns) if 'sector' in elem.lower()][0]
    
    # Tick table
    security_table = constituents.iloc[:,[tic_index, sec_index]].copy()
    security_table['Index'] = idx
    security_table.columns = ['Stock','GICS Sector','Index']
    
    # Load to database
    load_to_db.sec_master2db(security_table)
    
# Scrape book to market ratios from yahoo for index constituents
def index_bk2mkt(idx):
    # Start timer
    start = time.perf_counter()
    
    # Get list of constituents
    constituents = wiki_index_cons(idx)
    # Get ticker column
    tic_index = [i for i, elem in enumerate(constituents.columns) if 'symbol' in elem.lower()][0]
    
    # Run yahoo_hist_px2db for each constituent
    err = 0
    i = 0
    while i <= len(constituents):
        # Load ticker
        tic = constituents.iloc[i,tic_index]
        try:
            print('{},\t{}'.format(tic,i))
            load_to_db.yahoo_bk2mkt2db(tic)
            err = 0
        except Exception as e:
            print(e)
            print('Ticker unavailable')
            err = err + 1
            if err > 3:
                print('Pausing for 1 min...')
                time.sleep(60)
                i = i - 4
                err = 0
        i = i + 1
            
    # End timer
    end = time.perf_counter()
    print('Elapsed time: '+str(end-start)+' seconds')

# Execution
if __name__ == '__main__':    
    # index_hist_price('S&P 500','20200501','20170101')
    # index_hist_price('S&P 1000','20200501','20170101')
    
    # # Scrape constituent information
    # idx = 'S&P 500'
    # sec_master('S&P 1000')
    
    # index_bk2mkt('S&P 1000')
    
    idx = 'S&P 1000'
    constituents = wiki_index_cons(idx)
    index_bk2mkt(idx)
    
    pass
    
    
    
