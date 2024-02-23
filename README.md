#  Web Scraping


This repository contains an ETL pipeline for scraping a books website. It includes scripts to extract data about books, such as the title, price, rating, availability, and web address, from a book retail website. The source for scraping is "http://books.toscrape.com/".
Following the extraction, the pipeline processes the data, perform some data transformation and loads it into a BigQuery database. 


Data Extraction:

Use HTTP requests to retrieve the HTML content from "http://books.toscrape.com/".
The Parsel library is then used to parse the HTML and extract the book data.
Data Transformation:

The extracted data is converted into a structured format using Pandas, forming a DataFrame.
Price and currency are split into separate columns, and ratings are converted from text to integers.
Data Loading:

Finally, the data is loaded into a BigQuery table using the to_gbq method from Pandas, which interfaces with the Google BigQuery API.
Tools and Languages:

The programming language used is Python.
Libraries involved include requests, Parsel, Pandas, and Google Cloud BigQuery client libraries.
The Google OAuth2 library is used for authenticating with Google Cloud services.
Flow of Data:

Data flows from the source website through the extraction process, into a DataFrame where it is transformed, and finally into BigQuery.
