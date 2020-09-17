import requests
from bs4 import BeautifulSoup
import pymongo
import subprocess
from datetime import date
import sys
import csv

table_name="site_datavalues"
#db_adress="mongodb://yurkin:Zz123456@localhost"
db_adress="mongodb+srv://yurkin:Zz123456@cluster0.b3evf.mongodb.net/test?retryWrites=true&w=majority"

def scrath():
    client = pymongo.MongoClient(db_adress)
    db = client.particleone
    print("Connection to Mongo Successful")
    news_table = db[table_name]
    url = 'https://www.reuters.com/topics/archive/fundsfundsNews'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    i = 0
    for link in soup.find_all('article'):
        lnk = link.find("a")["href"]
        title = link.find("h3").text
        text = link.find("p").text
        t_stamp = link.select('span[class="timestamp"]')[0].text
        # Insert into MongoDB
        insert_string = {"link": lnk, "title": title, "text": text, "t_stamp": t_stamp, "date": date.today().strftime("%d/%m/%Y")}
        query_mongo = {"link": lnk}
        if news_table.find(query_mongo).count() == 0:
            x = news_table.insert_one(insert_string)
            i += 1
    print("Finished. "+str(i)+" records added")


def output_file(date_value):
    client = pymongo.MongoClient(db_adress)
    db = client.particleone
    print("Connection to Mongo Successful")
    news_table = db[table_name]
    query_mongo = {"date": date_value}
    with open("unload.csv", 'w', newline='\n') as csvfile:
        #writer = csv.writer(csvfile, delimiter=';', quotechar='|')
        for x in news_table.find(query_mongo):
            for x_values in x.values():
                csvfile.write(str(x_values).replace("\n","\t"))
                csvfile.write('|')
            csvfile.write('\n')


command = sys.argv[1]
if len(sys.argv)>2:
    date_value_input=sys.argv[2]

print(command)

if command == "run":
    scrath()
elif command == "startdb":
    subprocess.call("sudo","/usr/local/bin/docker-compose","up","-d")
elif command == "unload":
    output_file(date_value_input)
else:
    print("use run, startdb or unload")

