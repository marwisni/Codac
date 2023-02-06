# PySpark application to work with csv files

## Description
The goal of this application is to help very small company called **KommatiPara** that deals with bitcoin trading. Company has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

## Input data
User needs to provide two csv files:
1) First with personal data which contains columns:  
       a)  id  
       b)  first_name  
       c)  last_name  
       d)  email  
       e)  country  

2) Second with financial details:  
       a)  id  
       b)  btc_a (bitcoin address)  
       c)  cc_t (credit card type)
       d)  cc_n (credit card number)

## Output (result)
As the result of application work user will get one csv file with joined data from both inputed files. Output data will contains data only for chosen countries included in list which user will provide as parameter. Some columns' names will be also changed for better readability. Final file will contains columns:
<ol type="a">
        <li> client_identifier (previous 'id')  </li>
        <li> email  </li>
        <li> country  </li>
        <li>  bitcoin_address  (previous 'btc_a')  </li>
        <li> credit_card_type  (previous 'cc_t') </li>
</ol>

## Run
To run application just type in terminal:  
python3 codac.py *personal_data_file_name.csv* *financial_data_file_name.csv* country_1, country_2, ..., country_n