# Codac PySpark assignment

## Description
The goal of this application is to help very small company called **KommatiPara** that deals with bitcoin trading. Company has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

## Input data
User needs to provide two csv files:
1) First with personal data which contains columns:

|id|first_name|last_name|email|country|
|---|---------|---------|-----|-------|

2) Second with financial details:
   
|id|btc_a|cc_t|cc_n|
|---|---------|---------|-----|

where:  
-  btc_a = bitcoin address  
-  cc_t = credit card type  
-  cc_n = credit card number

## Output (result)
As the result of application work user will get one csv file with joined data from both inputed files. Output data will contains data only for chosen countries included in list which user will provide as parameter. Some columns' names will be also changed for better readability. Final file will contains columns:
|client_identifier|email|country|bitcoin_address|credit_card_type|
|---|---------|---------|-----|-------|

## Run
After installation of package to run application use this command:
```python  
Codac -p 'personal_data_file_name.csv' -f 'financial_data_file_name.csv' -c 'country_1, country_2, ..., country_n'
```
where:  
- Codac is named entry point of application
- *'personal_data_file_name.csv'* is path to csv file contains clients personal data
- *'financial_data_file_name.csv'* is path to csv file contains clients financial data
- 'country_1, country_2, ..., country_n' is string with list of countries' names (comma separated) which user prefer to filter

All arguments are optional and if any of them is not provided then default parameters from config.py file will be used. Instead of *-p*, *-f*, *-c*  option you can use long versions: *--personal*, *--financial, *--country* appropriately. So if:    
a) config.py was not modified   
b) you run application from directory where *source_data* with sample .csv files are stored  

then these two run commands are equivalent:

1.
```python  
Codac -p './source_data/dataset_one.csv' -f './source_data/dataset_two.csv' -c 'United Kingdom, Netherlands'
```
2.
```python  
Codac
```

because parameters used in option (1) are default and stored in config.py file.