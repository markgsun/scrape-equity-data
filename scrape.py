# -*- coding: utf-8 -*-
"""
Created on Sun May 10 10:27:50 2020

@author: markg

Contains shared webscraping functions
"""

# Header
def header_function(base_url, subdomain):
    hdrs =  {'authority': base_url,
             'method': 'GET',
             'path': subdomain,
             'scheme': 'https',
             'accept': 'text/html',
             'accept-encoding': 'gzip, deflate, br',
             'accept-language': 'en-US,en;q=0.9',
             'cache-control': 'no-cache',
             'cookie': 'Cookie:identifier',
             'dnt': '1',
             'pragma': 'no-cache',
             'sec-fetch-mode': 'navigate',
             'sec-fetch-site': 'same-origin',
             'sec-fetch-user': '?1',
             'upgrade-insecure-requests': '1',
             'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36'}
    return hdrs