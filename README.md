#  Web Scraping and ETL


This repository contains an ETL pipeline for scraping a books website. It includes scripts to extract data about books, such as the title, price, rating, availability, and web address, from a book retail website. The source for scraping is "http://books.toscrape.com/".
Following the extraction, the pipeline processes the data, perform some data transformation and loads it into a BigQuery database. 

### 1. Tech Stack
- Python.
- BigQuery.
  
### 2. Installation and How to Run
- Clone the repository: ``git clone https://github.com/MNCEDISIMNCWABE/Web-Scraping-and-ETL.git``
- Install libraries: ``pip install requests parsel pandas pandas-gbq google-auth``
- Run the script: ``python app.py``

### 3. Data Extraction

- Uses HTTP requests to retrieve the HTML content from "http://books.toscrape.com/".
The Parsel library is then used to parse the HTML and extract the book data.

### 2. Data Transformation

- The extracted data is converted into a structured format using Pandas, forming a DataFrame.
Price and currency are split into separate columns, and ratings are converted from text to integers.

### 3. Data Loading

- Finally, the data is loaded into a BigQuery table using the ``to_gbq`` method from Pandas, which interfaces with the Google BigQuery API.


### 5. Flow of Data
Data flows from the source website through the extraction and transformation process, and finally loaded into a BigQuery table.
