import pandas as pd
import numpy as np
import psycopg2

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

def ensirekisteroinnit_kunnittain_ajoneuvoluokassa(pvm_alku, pvm_loppu, ajoneuvoluokka, kunta, hae_malleittain):
    cursor = luo_yhteys()
    query = f''' SELECT ar.merkkiselvakielinen, '''
    if(hae_malleittain):
        query += f'''ar.mallimerkinta,'''
    query += f'''
        COUNT(ar.id) AS maara
        FROM ajoneuvorekisteroinnit ar
        LEFT JOIN kunta ku ON ku.id = ar.kunta
        WHERE ku.selite_fi = '{kunta.title()}' 
        AND ar.ajoneuvoluokka = '{ajoneuvoluokka}' 
        AND ensirekisterointipvm >= '{pvm_alku}' 
        AND ensirekisterointipvm <= '{pvm_loppu}'
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

def ensirekisteroinnit_vuosittain_ominaisuuden_mukaan(ominaisuus, vuosi, luokka):
    cursor = luo_yhteys()
    query = f'''
        SELECT o.selite_fi, COUNT(ar.id) AS maara
        FROM ajoneuvorekisteroinnit ar
    	LEFT JOIN {ominaisuus} o ON o.id = ar.{ominaisuus}
        WHERE ensirekisterointipvm >= '{vuosi}-01-01' AND ensirekisterointipvm < '{vuosi+1}-01-01' AND ar.ajoneuvoluokka = '{luokka}'
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

def ensirekisteroinnit_vuosittain(vuosi_alaraja, vuosi_ylaraja, filteri="merkkijamalli"):
    if not filteri in ['merkkijamalli','merkki','malli']:
        print("Virheellinen filteri")
        return 
    
    cursor = luo_yhteys()
    query = f''' 
        SELECT {filteri}
        '''
    kolumnit = [filteri]
    for vuosi in range(vuosi_alaraja, vuosi_ylaraja + 1 , 1):
        if filteri != "merkkijamalli":
            query += f''', SUM(vuosi{vuosi}) AS vuosi{vuosi}'''
        else:
            query += f''', vuosi{vuosi}'''
        kolumnit.append(str(vuosi))
    
    query += f''' FROM rekisteroinnitmalleittain '''
    if filteri != "merkkijamalli":
        query += f''' GROUP BY {filteri} '''
        
    query += f''' ORDER BY {filteri} '''
    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=kolumnit)
        df.fillna("Ei määritelty", inplace=True)
        return df
        
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)
  
def ensirekisteroidyt_merkit_vuosittain_aikavalilla(vuosi_alaraja, vuosi_ylaraja):
    cursor = luo_yhteys()
    query = f''' 
        SELECT am.selite AS merkki'''

    kolumnit = ['merkki']
    for vuosi in range(vuosi_alaraja, vuosi_ylaraja + 1, 1):
        query += f''', 
            SUM(u.vuosi{vuosi}) AS vuosi{vuosi}'''
        kolumnit.append('vuosi' + str(vuosi))
    
    query += f''' 
        FROM (
          SELECT
            ar.merkki_id '''
    
    for vuosi in range(vuosi_alaraja, vuosi_ylaraja + 1, 1):
        if vuosi <= 2016: #viimeinen kokonainen vuosi ajoneuvorekisteroinnit taulussa
            query += f''', 
                COUNT(CASE WHEN EXTRACT(YEAR FROM ar.ensirekisterointipvm) = {vuosi} THEN 1 END) AS vuosi{vuosi}'''
        else:
            query += f''', 
                0 AS vuosi{vuosi}'''

    query += f''' 
          FROM
            ajoneuvorekisteroinnit ar
          WHERE
            ar.ajoneuvoluokka = 'M1' AND ar.merkki_id IS NOT NULL
          GROUP BY
            ar.merkki_id

          UNION
  
          SELECT
        	rek.merkki_id'''

    for vuosi in range(vuosi_alaraja, vuosi_ylaraja + 1, 1):
        if vuosi <= 2016: #viimeinen kokonainen vuosi ajoneuvorekisteroinnit taulussa
            query += f''', 
                0 AS vuosi{vuosi}'''
        else:
            query += f''', 
                SUM(rek.vuosi{vuosi}) AS vuosi{vuosi}'''

    query += f'''
        FROM
          rekisteroinnitmalleittain rek
        WHERE
          rek.merkki_id IS NOT NULL
        GROUP BY
          rek.merkki_id
      ) AS u
      JOIN ajoneuvomerkki am ON am.id = u.merkki_id
      GROUP BY
        am.selite
      ORDER BY
        vuosi{vuosi_ylaraja} DESC;;'''

    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=kolumnit)
        df.fillna("Ei määritelty", inplace=True)
        return df
        
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)

def rekisteroityjen_autojen_tiedot_aikavalilla(vuosi_alaraja, vuosi_ylaraja):
    cursor = luo_yhteys()
    query = f'''
        SELECT ar.merkkiselvakielinen AS merkki, v.selite_fi AS vari, kt.selite_fi AS korityyppi, 
            ot.selite_fi AS ohjaamotyyppi, ar.istumapaikkojenlkm, ar.omamassa, kv.selite_fi AS kayttovoima, 
            ar.iskutilavuus, ar.suurinnettoteho, ar.sylintereidenlkm, ar.ahdin, ar.sahkohybridi,
        	vt.selite_fi AS vaihteisto, ar.vaihteidenlkm, ku.selite_fi AS kunta, ar.matkamittarilukema 
        FROM ajoneuvorekisteroinnit ar
        LEFT JOIN vari v ON v.id =  ar.vari
        LEFT JOIN kunta ku ON ku.id = ar.kunta
        LEFT JOIN korityyppi kt ON kt.id = ar.korityyppi
        LEFT JOIN ohjaamotyyppi ot ON ot.id = ar.ohjaamotyyppi
        LEFT JOIN kayttovoima kv ON kv.id = ar.kayttovoima
        LEFT JOIN vaihteistotyyppi vt ON vt.id = ar.vaihteistotyyppi
        WHERE ajoneuvoluokka = 'M1'
        AND ensirekisterointipvm >= '{vuosi_alaraja}-01-01' 
        AND ensirekisterointipvm < '{vuosi_ylaraja+1}-01-01'
    '''
    try:
        cursor.execute(query)
        # Fetch and print the results
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=["merkkiselvakielinen", "vari", "korityyppi", "ohjaamotyyppi", 
            "istumapaikkojenlkm", "omamassa", "kayttovoima", "iskutilavuus", 
        	"suurinnettoteho", "sylintereidenlkm", "ahdin", "sahkohybridi",
        	"vaihteistotyyppi", "vaihteidenlkm", "kunta", "matkamittarilukema"])
        return df
        
        # Commit the transaction
        #connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)
