Implementation of an ETL Pipeline using airflow to fetch data from an API. This app's cod is written for customer 22001 Hittcher. For other customers it needs to be configured

**set up and execution.**

1. clone the repo
2. Install Docker porogram. For more information visit the official website https://www.docker.com
3. Install Astro CLI based on your op on your local machine. For more information visit the official website https://www.astronomer.io/docs/astro/cli/overview/

**Notice:**

if you would like to run this app on your machine you need to configure three credentials  
1. The globalstock database credential in the both files "gstock.py" and "stock_api.py"  under the "uri" like "mysql://user:password@ip:port/mydatabase"
2. An api uri in the "stock_api.py" under the variable "api_uri" like "./*/*/*/*" 
4. A tocken in the "stock_api.py" under the variable "tocken"

**Tasks flow**

1. task 1(get_stockes_from_API):
    1. Fetch all custArIds from the gStock database
    2. Fetch stock of each custArtId from the API
    3. Store all fetched data (the custArtId and stocks) as parquet file on the locall machine under the path "./include/data/customer/stocks/year/month/day/" and "./include/data/customer/articles/year/month/day/"
       
2. task 2(get_yesterday_transaction_sales):
   1. fetch transation sales of all stores for the given customer(in this case 22001)
   2. save these information as parquet file under path "./include/data/customer/sales/year/month/day/"

3. task 3(pivot_article_stock_all_stores):
    1. Create a pivot table where rows represent custArtId and size, columns are stroe numbers and values indicate the stocke of each custArt/size at all stores at the the time the information was retrieved data from API.
    2. save the created dataframe locally under the path "./include/data/customer/stocks/year/month/day/"
4. task 4(two_days_stocke_diff):
     1. find custArtIds whose stock have changed compard to yesterday and the day before
     2. save the changed stockes locally under the path "./include/data/customer/stocks/year/month/day/
        
<p align="center">
          <img width="611" height="248" alt="graph" src="https://github.com/user-attachments/assets/de395bbb-3900-469f-8244-a321490faf48" />
</p>
 
