#https://www.reuters.com/topics/archive/fundsfundsNews
import requests
from bs4 import BeautifulSoup
import pymongo

table_name="site_datavalues"
#Connection to Mongo DB
client = pymongo.MongoClient("mongodb://yurkin:Zz123456@localhost")
db = client.particleone
print("Connection to Mongo Successful")
news_table = db[table_name]
news_table.drop()

url = 'https://www.reuters.com/topics/archive/fundsfundsNews'
r = requests.get(url)

soup = BeautifulSoup(r.text, "html.parser")
for link in soup.find_all('article'):
    lnk=link.find("a")["href"]
    title=link.find("h3").text
    text=link.find("p").text
    t_stamp=link.select('span[class="timestamp"]')[0].text
    #Insert into MongoDB
    insert_string = {"link": lnk, "title": title, "text": text, "t_stamp": t_stamp}
    query_mongo = {"link": lnk}
    if mycol.find(query_mongo).count() == 0:
        x = news_table.insert_one(insert_string)

print("Finished")