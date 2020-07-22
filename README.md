# scrape-equity-data
Scrape equity data from online sources

### scrape.py
Shared web-scraping functions

### yahoo_scrape.py
Pulls historical price table from yahoo finance for specified ticker and price range

*Uses scrape.py*

### load_to_db.py
Loads historical price table from yahoo finance and loads into local database table

*Uses yahoo_scrape.py*

### index_scrape.py
Pulls index constituents from Wikipedia, then pulls historical price table and book-to-market ratios from yahoo finance for all constituents

*Uses scrape.py, load_to_db.py*
