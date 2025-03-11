from matplotlib.pyplot import close
import psycopg2
import pandas as pd


#establishing the connection
conn = psycopg2.connect(
   database="postgres", user='postgres', password='015775570018', host='127.0.0.1', port= '5432')
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Creating a database
try:
   cursor.execute('''CREATE database FahrraddiebstahlinBerlin''')
   print("Database created successfully........")
except:
   print("Database already exists")


# Statement for creating Table
conn = psycopg2.connect(database="fahrraddiebstahlinberlin", user='postgres', password='015775570018', host='127.0.0.1', port= '5432')
conn.autocommit = True

print("connection to FahrraddiebstahlinBerlin established")

cursor = conn.cursor()

# Statement for creating Table
create_table_bezirksgrenz = "CREATE TABLE bezirksgrenz(Gemeinde_name char(35), Gemeinde_schluessel int NOT NULL)"
create_table_lor_planung = "CREATE TABLE lor_planung(PLR_ID int NOT NULL, PLR_NAME varchar(40), BEZ int NOT NULL, GROESSE_M2 DECIMAL(10, 2))"
create_table_fahrraddieb = '''CREATE TABLE fahrraddieb( TATZEIT_ANFANG_DATUM varchar(20),
                              TATZEIT_ANFANG_STUNDE int,
                              TATZEIT_ENDE_DATUM varchar(20),
                              TATZEIT_ENDE_STUNDE int,
                              LOR int NOT NULL,
                              SCHADENSHOEHE int NOT NULL,
                              ART_DES_FAHRRADS varchar(35))'''

try:
    cursor.execute(create_table_bezirksgrenz)
    print("relation bezirksgrenz created successfully")
except psycopg2.Error as e:
    print("Error creating table: ", e)
        
try:
    cursor.execute(create_table_lor_planung)
    print("relation lor_planung created successfully")
except psycopg2.Error as e:
    print("Error creating table: ", e)

try:
    cursor.execute(create_table_fahrraddieb)
    print("relation fahrraddieb created successfully")
except psycopg2.Error as e:
    print("Error creating table: ", e)

#Tabellen kopieren
b = open('bezirk.csv')
next(b)
cursor.copy_from(b, 'bezirksgrenz', columns=('gemeinde_name', 'gemeinde_schluessel'), sep=',')

l = open('lor.csv')
next(l)
cursor.copy_from(l, 'lor_planung', columns=('plr_id', 'plr_name', 'bez', 'groesse_m2'), sep=',')

f = open('fahrrad.csv')
next(f)
cursor.copy_from(f, 'fahrraddieb', columns=('tatzeit_anfang_datum', 'tatzeit_anfang_stunde',
                                            'tatzeit_ende_datum','tatzeit_ende_stunde','lor',
                                            'schadenshoehe','art_des_fahrrads'), sep=',')


#some more data cleansing
cursor.execute("DELETE FROM fahrraddieb WHERE schadenshoehe IS NULL;")

#Closing the connection
cursor.close()
conn.close()
