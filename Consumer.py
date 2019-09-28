import pika
import sqlite3
import json
import csv
import os
import xml.etree.ElementTree as ET

from sqlite3 import Error

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')



def sql_fetch_TotalOrders(con, cursorObj):
    try:
        cursorObj.execute('select billingcountry, count(*) as TotalOrders from invoices group by billingcountry order by TotalOrders desc')
    except Error as e:
        print(e)
    

    OrdersRows = cursorObj.fetchall()
    
    # create a new csv file with the results
    csvWriter = csv.writer(open("D:\Project\TotalOrders.csv", "w", encoding="utf8"))
    csvWriter.writerow(['Country', 'TotalOrders'])
    csvWriter.writerows(OrdersRows)

    return OrdersRows

def sql_fetch_TotalItems(con, cursorObj):
    try:
        cursorObj.execute('select i.billingcountry, sum(ii.quantity) as TotalItems from invoices i inner join invoice_items ii on ii.invoiceid = i.invoiceid group by i.billingcountry order by TotalItems desc ')
    except Error as e:
        print(e)
    
    ItemsRows = cursorObj.fetchall()

    # create a new csv file with the results
    csvWriter = csv.writer(open("D:\Project\TotalItems.csv", "w", encoding="utf8"))
    csvWriter.writerow(['Country', 'TotalItems'])
    csvWriter.writerows(ItemsRows)

    return ItemsRows

def sql_fetch_ListOfAlbumsForCountry(con, cursorObj, Country):
    try:
        cursorObj.execute('SELECT  a.title FROM invoices i inner join invoice_items ii on ii.invoiceid=i.invoiceid inner join tracks t on t.trackid = ii.trackid inner join albums a on a.albumid = t.albumid where i.BillingCountry="'+Country+'"  group by i.BillingCountry, a.title ')
    except Error as e:
        print(e)
    
    AlbumsForCountryRows = cursorObj.fetchall()
    
    # create the file structure
    jsonText = {"Country":Country,
            "Albums":AlbumsForCountryRows
            }
    json_string = json.dumps(jsonText)
    filename = 'D:\\Project\\JsonFile.json'
   
    # create a new json file with the results
    with open(filename, 'w') as f:
        f.write(json_string)
    

def sql_fetch_BestAlbum(con, cursorObj, Country, Year):
    try:
        cursorObj.execute('with c as (select a.title, i.billingcountry, sum(quantity) as total,strftime("%Y", i.invoicedate) as Year  from invoices i inner join invoice_items ii on ii.invoiceid = i.invoiceid inner join tracks t on t.trackid = ii.trackid inner join albums a on a.albumid = t.albumid inner join genres g on g.genreid = t.genreid  where  g.name = "Rock" and billingcountry = "'+Country+'"   group by a.title, i.billingcountry,strftime("%Y", i.invoicedate) order by total desc)select "'+Country+'" as Country, *, "'+Year+'" as Year  from( select  title, sum(Total) as Total from c where Year>="'+Year+'" group by title order by Total desc) limit 1')
    except Error as e:
        print(e)

    BestAlbumRows = cursorObj.fetchall()
    CountryName = Country
    AlbumName = BestAlbumRows[0][1]
    NumTotalOrder = BestAlbumRows[0][2]
    
    
    # create the file structure
    data = ET.Element('Data')
    Country = ET.SubElement(data, 'Country')
    Country.text = CountryName
    Album = ET.SubElement(Country, 'Album')
    Album.set('name','NameOfBestAlbum')
    Album.text = AlbumName
    TotalOrder = ET.SubElement(Country, 'TotalOrder')
    TotalOrder.set('name','TotalOrder')
    TotalOrder.text = str(NumTotalOrder)
    FromYear = ET.SubElement(Country, 'FromYear')
    FromYear.set('name','FromYear')
    FromYear.text = Year
    
    # create a new XML file with the results
    mydata = ET.tostring(data)
    myfile = open("D:\Project\items2.xml", "wb")
    myfile.write(mydata)

    return BestAlbumRows


def create_table(con, cursorObj, create_table_sql, Data, TableName):
    try:
        cursorObj.execute(create_table_sql)
    except Error as e:
        print(e)
  
    if TableName == "Orders":
        for row in Data:
            try:
                InsertRows = ''' INSERT INTO TotalOrders(Country,TotalOrders)
                        VALUES("'''+row[0]+'''","'''+str(row[1])+'''") '''
                cursorObj = con.cursor()
                cursorObj.execute(InsertRows)
                print ('Inserted')
            except Error as e:
                print(e)
            
    elif TableName == "Items":
        for row in Data:
            try:
                InsertRows = ''' INSERT INTO TotalItems(Country,TotalItems)
                        VALUES("'''+row[0]+'''","'''+str(row[1])+'''") '''
                cursorObj = con.cursor()
                cursorObj.execute(InsertRows)
                print ('Inserted')
            except Error as e:
                print(e)
    elif TableName == "BestAlbum":
        for row in Data:
            try:
                InsertRows = ''' INSERT INTO BestAlbum(Country,NameOfBestAlbum,TotalOrder,FromYear)
                        VALUES("'''+row[0]+'''","'''+row[1]+'''","'''+str(row[2])+'''","'''+row[3]+'''") '''
                cursorObj = con.cursor()
                cursorObj.execute(InsertRows)
                print ('Inserted')
            except Error as e:
                print(e)
    

sql_create_TotalOrders_table = """ CREATE TABLE IF NOT EXISTS TotalOrders (
                                        Country text NOT NULL,
                                        TotalOrders text
                                    ); """
 
sql_create_TotalItems_table = """ CREATE TABLE IF NOT EXISTS TotalItems (
                                        Country text NOT NULL,
                                        TotalItems text
                                    ); """

sql_create_BestAlbum_table = """ CREATE TABLE IF NOT EXISTS BestAlbum (
                                        Country text NOT NULL,
                                        NameOfBestAlbum text,
                                        TotalOrder text,
                                        FromYear text
                                    ); """


def create_connection(db_file):
    con = None
    try:
        con = sqlite3.connect(db_file)
    except Error as e:
        print(e)
 
    return con

def callback(ch, method, properties, body):
    T = json.loads(body)
    conPath = T['Path']
    Country = T['CountryName']
    Year = T['Year']

    create_connection(conPath)
    con = create_connection(conPath)
    cursorObj = con.cursor()
    
    Orders_Rows = sql_fetch_TotalOrders(con, cursorObj)
    Items_Rows = sql_fetch_TotalItems(con, cursorObj)
    BestAlbum_Rows = sql_fetch_BestAlbum(con, cursorObj, Country, Year)

    sql_fetch_TotalOrders(con, cursorObj)
    sql_fetch_TotalItems(con, cursorObj)
    sql_fetch_ListOfAlbumsForCountry(con, cursorObj, Country)
    sql_fetch_BestAlbum(con, cursorObj, Country, Year)
    create_table(con, cursorObj, sql_create_TotalOrders_table, Orders_Rows, "Orders")
    create_table(con, cursorObj, sql_create_TotalItems_table, Items_Rows, "Items")
    create_table(con, cursorObj, sql_create_BestAlbum_table, BestAlbum_Rows, "BestAlbum")
    
    
    con.commit()
    cursorObj.close()
    if (con):
        con.close()
        print("The SQLite connection is closed")

channel.basic_consume(
    queue='hello', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()






