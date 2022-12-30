# Etherscan scrapper
This project scraps the top 1000 users of one crypto-currency and their transactions even if it is with other crypto-currencies. Then saves the scrapped data into a MySQL database

## Requirements 
* Python 2.7 or 3
* BeautifulSoup
* pymysql   

## Getting started
* Get the contract_token that you want to scrap from etherscan.io
* open the etherscan.py and edit the inputs dictionary to fit your needs.
```   
inputs= {
    "contract_token" :"0x8d5682941ce456900b12d47ac06a88b47c764ce1",
    "new_DB":True, 		# If you want to create a new database
    "host":"localhost",
    "user":"root",
    "password":"",
    "db_name":"etherscan2"  
}
```
