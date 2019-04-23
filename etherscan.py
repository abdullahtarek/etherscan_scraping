from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime , timedelta
import numpy as np
from tqdm import tqdm
import pymysql

#contract_token is the token you want to scrape the information of
#new_DB is a FLAG set it True if you want to make a new database and false if you want to add information to an existing database
#The host link that has the database
#Database User that can access and edit the database
#password of the user 
#db_name database name
inputs= {
    "contract_token" :"0x8d5682941ce456900b12d47ac06a88b47c764ce1",
    "new_DB":True,
    "host":"localhost",
    "user":"root",
    "password":"",
    "db_name":"etherscan2"  
}

def create_database():
    global inputs
    conn = pymysql.connect(host=inputs["host"], user=inputs["user"],passwd=inputs["password"])
    cursor = conn.cursor()
    cursor.execute(""" 
           CREATE DATABASE {}
        """.format(inputs["db_name"])
        )
    
    conn.commit()
    conn.close()
    

def create_tabels():
    global inputs
    conn = pymysql.connect(host=inputs["host"], user=inputs["user"],passwd=inputs["password"], db=inputs["db_name"] )
    cursor = conn.cursor()
    cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS `holders` (
              `address` varchar(100) NOT NULL,
              `quantity` decimal(40,30) NOT NULL,
              `percentage` decimal(40,30) NOT NULL
            )
        """
         )
    
    cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS `transactions` (
              `address` varchar(100) NOT NULL,
              `transaction_date` datetime NOT NULL,
              `direction` varchar(10) NOT NULL,
              `value` decimal(40,30) NOT NULL,
              `token` varchar(255) NOT NULL
            )
        """
         )
    
    
    conn.commit()
    conn.close()

def inster_holders(holders):
    global inputs
    conn = pymysql.connect(host=inputs["host"], user=inputs["user"],passwd=inputs["password"], db=inputs["db_name"] )
    cursor = conn.cursor()
    
    for holder in holders:
        cursor.execute(""" 
        INSERT INTO holders values(%s,%s,%s)

        """
         , holder)
    
    conn.commit()
    conn.close()

    
def inster_transaction(transactions):
    global input
    conn = pymysql.connect(host=inputs["host"], user=inputs["user"],passwd=inputs["password"], db=inputs["db_name"] )
    cursor = conn.cursor()
    
    for transaction in transactions:
        cursor.execute(""" 
        INSERT INTO transactions values(%s,%s ,%s,%s,%s)

        """
         , transaction)
    conn.commit()
    conn.close()
    
    
#retruns top holders of given a token
def get_top_holder(token):
    website = "https://etherscan.io/token/generic-tokenholders2?a="+token
    rows = []
    next_page = website
    while(next_page!="#"):
        raw_html = requests.get(next_page).content
        html=BeautifulSoup(raw_html, 'html.parser')
        for row in html.table.find_all("tr")[1:]:
            one_row=[]
            #one_row.append(token)
            one_row.append(row.find_all("td")[1].text.strip())
            one_row.append(row.find_all("td")[2].text.strip())
            one_row.append(row.find_all("td")[3].text.strip()[:-1])
            rows.append(one_row)

        next_page = html.find(id="PagingPanel2").find("a",text="Next")['href']
        if next_page !="#":
            next_page=website+ next_page.split(token)[1][:-2]
            print(next_page)
        time.sleep(0.25)
    return rows
    
    
#convert x days y hours to timestamp.
def convert_to_date(ss):
    percision = {'days': 0 , 'hrs':0 , 'min':0}
    for p in percision:
        if p in ss.split(" "):
            date_list = ss.split(" ")
            percision[p]=float(date_list[date_list.index(p)-1])
    date_N_days_ago = datetime.now() - timedelta(days=percision['days'] , hours= percision['hrs'],minutes =percision['min']  ) 
    return str(date_N_days_ago)
#convert a string number to a decimal
def convert_to_num(num):
    return str(num.replace(',',''))

#this function takes in the webpage of a user and returns back all his transactions
def get_transactions(token , contract):
    rows = []
    website ="https://etherscan.io/tokentxns?a="+token
    next_page = website
    #loop over each page of transactions
    while(next_page!="#"):
        #get the contents of the webpage
        raw_html = requests.get(next_page).content
        html=BeautifulSoup(raw_html, 'html.parser')
        
        if (len(html.table.find_all("tr")[1]))==7:
            #loop over rows of of the transactions table to extract information
            for row in html.table.find_all("tr")[1:]:
                one_row=[]
                one_row.append(token)
                one_row.append(convert_to_date(row.find_all("td")[1].text.strip()))
                #one_row.append(row.find_all("td")[2].text.strip())
                #one_row.append(row.find_all("td")[4].text.strip())
                one_row.append(row.find_all("td")[3].text.strip())
                one_row.append(convert_to_num(row.find_all("td")[5].text.strip()))
                one_row.append(row.find_all("td")[6].text.strip())
                rows.append(one_row)
            
        next_page = html.find("a",text="Next")['href']
        if next_page !="#":
            next_page=website+ next_page.split(token)[1]
        time.sleep(0.25)
    return rows


def all_transactions(token):
    global inputs
    if inputs["new_DB"] ==True:
        print("creating New database")
        create_database()
        create_tabels()
        
    token = token.strip()
    #get top holders of this token
    print("getting Top Holders")
    top_holders =get_top_holder(token)
    #save top holders in database
    print("saving holders in database")
    inster_holders(top_holders)
    
    print("getting transactions for each ")
    #get transactions for each holder
    print("getting transactions for each holder")
    top_holders_np= np.array(top_holders)[:,0]
    transactions= []
    for holder in tqdm(top_holders_np):
        transactions += get_transactions(holder , token)
        time.sleep(3)
    #remove duplicate transactions
    transactions = [list(item) for item in set(tuple(row) for row in transactions)]
    #save transactions in database  
    print("saving transactions in database")
    inster_transaction(transactions)
    
    return top_holders , transactions
    
    
#fetch all data of a certain token
token = inputs["contract_token"]
top_holders , transactions = all_transactions(token)