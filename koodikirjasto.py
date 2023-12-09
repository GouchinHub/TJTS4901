#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import numpy as np
import psycopg2


# Data koostettu: 
# Suurin osa täältä
# https://www.avoindata.fi/data/fi/dataset/ajoneuvojen-avoin-data/resource/00041d67-966e-48cf-a720-255faf09d55c?
# rekisteroinnitmalleittan taulu koostettu näistä datoista
# https://www.aut.fi/tilastot/ensirekisteroinnit/henkiloautojen_vuosittaiset_merkki-_ja_mallitilastot
# 
# Data koostettiin useammasta lähteestä, koska pääasiallisen datalähteen data oli puutteellista vuodesta 2017 lähtien. 

# In[6]:


def luo_yhteys():
    try:
        connection = psycopg2.connect(
            host="192.168.0.15",
            database="VehicleRegisterDatabase",
            user="postgres",
            password="1337"
        )
        #print("Connection to PostgreSQL database successful")
        print(connection)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL database:", error)

    try:
        cursor = connection.cursor()
        print("Cursor created successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating a cursor:", error)

    return cursor


# In[7]:


cursor = luo_yhteys()
query = f'''
    SELECT kunta, ajoneuvoluokka, ensirekisterointipvm
    FROM ajoneuvorekisteroinnit
    WHERE kunta = '179' AND ajoneuvoluokka = 'M1' AND ensirekisterointipvm >= '2010-01-01' AND ensirekisterointipvm <= '2022-01-01'
    ORDER BY ensirekisterointipvm
'''
try:
    cursor.execute(query)
    # Fetch and print the results
    records = cursor.fetchall()
    df = pd.DataFrame(records, columns=["kunta", "ajoneuvoluokka", "ensirekisterointopvm"])
    print(df)

    # Commit the transaction
    #connection.commit()
except (Exception, psycopg2.Error) as error:
    print("Error while executing SQL query:", error)


# In[8]:


def ensirekisteroinnit_kunnittain_ajoneuvoluokassa(pvm_alku, pvm_loppu, ajoneuvoluokka, kunta, hae_malleittain):
    cursor = luo_yhteys()
    query = f''' SELECT ar.merkkiselvakielinen, '''
    if(hae_malleittain):
        query += f'''ar.mallimerkinta,'''
    query += f'''
        COUNT(ar.id) AS maara
        FROM ajoneuvorekisteroinnit ar
        LEFT JOIN kunta ku ON ku.id = ar.kunta
        WHERE ku.selite_fi = '{kunta.title()}' AND ar.ajoneuvoluokka = '{ajoneuvoluokka}' AND ensirekisterointipvm >= '{pvm_alku}' AND ensirekisterointipvm <= '{pvm_loppu}'
		GROUP BY ar.merkkiselvakielinen 
    '''
    if(hae_malleittain):
        query += f''', ar.mallimerkinta'''

    query += f'''
        ORDER BY maara DESC;
    '''
    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        columns = ["Merkki", "Määrä"]
        if (hae_malleittain):
            columns.insert(1, "Malli")
        
        df = pd.DataFrame(records, columns=columns)

        return df
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)


# In[9]:


ensirekisteroinnit_kunnittain_ajoneuvoluokassa('2016-01-01', '2017-01-01', 'M1', 'Helsinki', True)


# In[10]:


def hybridiautot_luokittain(luokka, vuosi_alku, vuosi_loppu,):
    cursor = luo_yhteys()
    query = f'''
        SELECT EXTRACT(YEAR FROM ar.ensirekisterointipvm), EXTRACT(MONTH FROM ar.ensirekisterointipvm), hl.lyhytselite_fi, COUNT(ar.*)
        FROM ajoneuvorekisteroinnit ar
    	LEFT JOIN hybridiluokka hl ON hl.id = ar.sahkohybridinluokka
        WHERE ar.sahkohybridi = true
        AND ar.ajoneuvoluokka = '{luokka}' 
        AND ar.ensirekisterointipvm >= '{str(vuosi_alku)}-1-1' 
        AND ar.ensirekisterointipvm <= '{str(vuosi_loppu)}-12-31'
        GROUP BY EXTRACT(YEAR FROM ar.ensirekisterointipvm), EXTRACT(MONTH FROM ar.ensirekisterointipvm), hl.lyhytselite_fi
        ORDER BY EXTRACT(YEAR FROM ar.ensirekisterointipvm) ASC, EXTRACT(MONTH FROM ar.ensirekisterointipvm) ASC, hl.lyhytselite_fi
    '''
    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=["vuosi", "kuukausi", "hybridityyppi", "maara"])
        df.fillna("Ei määritelty", inplace=True)

        return df
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)


# In[22]:


import matplotlib.pyplot as plt

df = hybridiautot_luokittain('M1', 2010, 2017)
df['date'] = df["kuukausi"].astype('str') + '.' + df["vuosi"].astype('str')

pd.set_option('display.max_rows', None)
print(df)


df1 = df[df['hybridityyppi'] == 'Sähköverkosta ladattava']
plt.plot(df1['date'], df1['maara'], label='Type 1')

df2 = df[df['hybridityyppi'] == 'Pelkästään polttomoottorilla ladattava']
plt.plot(df2['date'], df2['maara'], label='Type 2')
print(df1)
print(df2)
plt.show()

# Create a figure and axis
fig, ax = plt.subplots()

# Plot data for Type 1
ax.plot(df1['date'], df1['maara'], label='Type 1')

# Plot data for Type 2
ax.plot(df2['date'], df2['maara'], label='Type 2')

# Add labels and legend
ax.set_xlabel('Date')
ax.set_ylabel('Amount')
ax.set_title('Hybridity Type Comparison')
ax.legend()

# Show the plot
plt.show()




# In[25]:


df['date'] = pd.to_datetime(df['vuosi'].astype(str) + '-' + df['kuukausi'].astype(str), format='%Y-%m')
condition = df['hybridityyppi'] == 'Ei määritelty'
df = df[~condition]


df_pivot = df.pivot(index='date', columns='hybridityyppi', values='maara')

fig, ax = plt.subplots(figsize=(12, 6))
df_pivot.plot(ax=ax, marker='o')

plt.title('Hybridiajoneuvojen kehitys')
plt.xlabel('Aika')
plt.ylabel('Määrä')
plt.legend(title='Hybridiajoneuvon tyyppi')
plt.grid(True)

plt.show()


# In[13]:


def ensirekisteroinnit_vuosittain_ominaisuuden_mukaan(ominaisuus, vuosi, luokka):
    cursor = luo_yhteys()
    query = f'''
        SELECT o.selite_fi, COUNT(ar.id) AS maara
        FROM ajoneuvorekisteroinnit ar
    	LEFT JOIN {ominaisuus} o ON o.id = ar.{ominaisuus}
        WHERE ensirekisterointipvm >= '{vuosi}-01-01' AND ensirekisterointipvm <= '{vuosi+1}-01-01' AND ar.ajoneuvoluokka = '{luokka}'
    	GROUP BY o.selite_fi
        ORDER BY maara DESC;
    '''
    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=[ominaisuus, "maara"])
        df.fillna("Ei määritelty", inplace=True)
        print(df)
        return df
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)


# In[14]:


ensirekisteroinnit_vuosittain_ominaisuuden_mukaan('vari', 2016, 'M1')
ensirekisteroinnit_vuosittain_ominaisuuden_mukaan('kunta', 2016, 'M1')
ensirekisteroinnit_vuosittain_ominaisuuden_mukaan('ohjaamotyyppi', 2016, 'M1')
ensirekisteroinnit_vuosittain_ominaisuuden_mukaan('vaihteistotyyppi', 2016, 'M1')


# In[15]:


def ensirekisterointien_maarat_ajoneuvoluokittain_aikavalilla(pvm_alku, pvm_loppu):
    cursor = luo_yhteys()
    query = f'''
        SELECT al.lyhytselite_fi, COUNT(ar.id) AS maara
        FROM ajoneuvorekisteroinnit ar
    	JOIN ajoneuvoluokka al ON al.id = ar.ajoneuvoluokka
        WHERE ensirekisterointipvm >= '{pvm_alku}' AND ensirekisterointipvm <= '{pvm_loppu}'
        GROUP BY al.lyhytselite_fi
    	ORDER BY maara DESC;
    '''
    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=["ajoneuvoluokka", "maara"])
        df.fillna("Ei määritelty", inplace=True)
        print( df)
        return df
        
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)


# In[28]:


df = ensirekisterointien_maarat_ajoneuvoluokittain_aikavalilla('2015-01-01','2020-01-01')
#ensirekisterointien_maarat_ajoneuvoluokittain_aikavalilla('2009-01-01','2010-01-01')
# Plot a pie chart
fig, ax = plt.subplots()
ax.pie(df["maara"], labels=df["ajoneuvoluokka"].values, autopct='%1.1f%%', startangle=90)

# Customize the plot
ax.set_title('Ajoneuvoluokkien osuudet')

# Show the plot
plt.show()


# In[17]:


def mallille_ja_merkille_korreloituvat_muuttujat(luokka):
    cursor = luo_yhteys()
    query = f'''
        SELECT al.lyhytselite_fi, COUNT(ar.id) AS maara
        FROM ajoneuvorekisteroinnit ar
    	JOIN ajoneuvoluokka al ON al.id = ar.ajoneuvoluokka
        WHERE ensirekisterointipvm >= '{pvm_alku}' AND ensirekisterointipvm <= '{pvm_loppu}'
        GROUP BY al.lyhytselite_fi
    	ORDER BY maara DESC;
    '''
    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=["ajoneuvoluokka", "maara"])
        df.fillna("Ei määritelty", inplace=True)
        print( df)
        return df
        
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)


# In[18]:


def merkin_ensirekisteroinnit_vuosittain(pvm_alku, pvm_loppu, ajoneuvoluokka, merkki):
    cursor = luo_yhteys()
    query = f'''
        SELECT *
        FROM rekisteroinnit malleittain
        AND malli = '{merkki}';
    '''
    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        df = pd.DataFrame(records)
        df.fillna("Ei määritelty", inplace=True)
        print( df)
        return df
        
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)


# In[19]:


df = merkin_ensirekisteroinnit_vuosittain('2015-01-01','2020-01-01', 'M1','Toyota')


# In[ ]:




