#!/usr/bin/env python
# coding: utf-8

# In[23]:


import requests
from parsel import Selector
import pandas as pd
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials


# ## Extract

# In[24]:


def parse_book(book):
    '''
    Parse the book information
    
    '''
    
    title = book.xpath('div/a/img/@alt').get()
    price = book.xpath('div/p[@class="price_color"]/text()').get()
    instock_status = "".join(book.xpath('div/p[@class="instock availability"]/text()').getall())
    instock_status = instock_status.strip('\n').strip()
    rating = book.xpath('p[contains(@class, "star-rating")]/@class').get().replace("star-rating ", "")
    url = book.xpath('div[@class="image_container"]/a/@href').get()
    
    return {
        'title': title,
        'price': price,
        'in_stock': instock_status,
        'rating': rating,
        'url': url
    }


def scrape_books():
    '''
    perform scraping
    
    '''
    start_urls = "http://books.toscrape.com/"
    response = requests.get(start_urls)
    selector = Selector(response.text)
    books = selector.xpath('//article[@class="product_pod"]')
    items = [parse_book(book) for book in books]
    
    return items


# ## Transform

# In[25]:


def convert_to_df(books_data):
    
    '''
    Converts json format data to dataframe
    
    '''
    
    books_data = pd.DataFrame(books_data)
    
    return books_data


def split_price_currency(df):
    '''
    Split the price column into currency and price (as float).
    
    '''
    df['currency'] = df['price'].str.extract(r'([^\d]+)')
    df['price'] = df['price'].str.extract(r'(\d+\.\d+)').astype(float)
    
    return df


def convert_ratings_to_int(df):
    '''
    Converts the ratings from text to integer.
    
    '''
    ratings_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
    df['rating'] = df['rating'].map(ratings_map).astype(int)
    
    return df


# ## Load

# In[26]:


def load_to_bigquery(df, bq_project_id, bq_dataset_id, bq_table_id, google_service_account_file):
    
    '''
    Loads the data to a BQ table
    
    '''
    
    destination_table = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    df.to_gbq(destination_table=destination_table,
              project_id=bq_project_id,
              credentials=google_service_account_file,
              chunksize=10000, 
              progress_bar=False,  
              if_exists='replace') 


# ## Orchastrate the pipeline

# In[27]:


bq_project_id = 'bright-arc-328707'
bq_dataset_id = 'test'
bq_table_id = 'books_data'
google_service_account_file = Credentials.from_service_account_file('C:/Users/leemn/OneDrive/Documents/personal google service acount/bright-arc-328707-b5e2d782b48b.json')

if __name__ == "__main__":
    books_data = scrape_books()
    books_df = convert_to_df(books_data)
    books_df = split_price_currency(books_df)
    books_df = convert_ratings_to_int(books_df)
    load_to_bigquery(books_df, bq_project_id, bq_dataset_id, bq_table_id, google_service_account_file)


# In[ ]:




