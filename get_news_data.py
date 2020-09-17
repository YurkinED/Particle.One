#https://www.reuters.com/topics/archive/fundsfundsNews
import requests
from bs4 import BeautifulSoup
import psycopg2
import pymongo

table_name="site_datavalues"
#Connection to Mongo DB
client = pymongo.MongoClient("mongodb+srv://yurkin:Zz123456@cluster0.b3evf.mongodb.net/test?retryWrites=true&w=majority")
db = client.particleone
print("Connection to Mongo Successful")
mycol = db[table_name]
mycol.drop()

#Connection to Postgre
conn = psycopg2.connect("""
   host=rc1c-iwte0zp4v7af7nr2.mdb.yandexcloud.net
    port=6432
    dbname=db1
    user=user1
    password=db1user12
    target_session_attrs=read-write
    sslmode=verify-full
""")
print("Successfully Postgree connected!")
cursor = conn.cursor()

cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
if not cursor.fetchone()[0]:
    commands =  """
    CREATE TABLE {} (
        link  VARCHAR(255) NOT NULL,
        title VARCHAR(255) NOT NULL,
        text  VARCHAR(2000) NOT NULL,
        t_stamp  VARCHAR(255) NOT NULL
    )
   """.format(table_name)
    cursor.execute(commands)
    conn.commit()
cursor.execute("delete from public."+table_name)


url = 'https://www.reuters.com/topics/archive/fundsfundsNews'
r = requests.get(url)
#with open('test.html', 'w', encoding='utf-8') as output_file:
#  output_file.write(r.text)

#news_list_lxml = xpath('//*[@id="moreSectionNews"]/section/div/article[1]/div[2]/a/h3"]')
soup = BeautifulSoup(r.text, "html.parser")
for link in soup.find_all('article'):
    lnk=link.find("a")["href"]
    title=link.find("h3").text
    text=link.find("p").text
    t_stamp=link.select('span[class="timestamp"]')[0].text
    #Insert into Postgre
    sql_pg = """INSERT INTO  public.{}(link, title, text, t_stamp)
                 VALUES(%s, %s, %s , %s);""".format(table_name)
    cursor.execute(sql_pg, (lnk,title,text,t_stamp,))
    #Insert into MongoDB
    mydict = {"link": lnk, "title": title, "text": text, "t_stamp": t_stamp}
    x = mycol.insert_one(mydict)

#End Postgre connection
cursor.close()
conn.commit()
conn.close()

print("Finished")