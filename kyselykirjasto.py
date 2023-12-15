import pandas as pd
import numpy as np
import psycopg2

def luo_yhteys():
    try:
        connection = psycopg2.connect(
            host="<host_name>",
            database="<database_name>",
            user="<username>",
            password="<password>"
        )
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL database:", error)

    try:
        cursor = connection.cursor()
        return cursor
    except (Exception, psycopg2.Error) as error:
        print("Error while creating a cursor:", error)


def merkkien_ensirekisteroinnit_kunnittain(vuosi_alaraja, vuosi_ylaraja, kunta_lista, hae_malleittain):
    if vuosi_alaraja < 1975 or vuosi_ylaraja > 2023:
        print("vuosirajaus oltava välillä 1975 - 2023")
        return
    
    cursor = luo_yhteys()
    query = f''' SELECT ar.merkkiselvakielinen, '''
    if(hae_malleittain):
        query += f'''ar.mallimerkinta,'''
    query += f'''
        ku.selite_fi,
        COUNT(ar.id) AS maara
        FROM ajoneuvorekisteroinnit ar
        LEFT JOIN kunta ku ON ku.id = ar.kunta
        WHERE ku.selite_fi IN {tuple(kunta_lista)}
        AND ar.ajoneuvoluokka = 'M1' 
        AND ensirekisterointipvm >= '{str(vuosi_alaraja)}-01-01' 
        AND ensirekisterointipvm <= '{str(vuosi_ylaraja+1)}-01-01'
		GROUP BY ar.merkkiselvakielinen, ku.selite_fi 
    '''
    if(hae_malleittain):
        query += f''', ar.mallimerkinta'''

    query += f'''
        ORDER BY maara DESC;
    '''
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        columns = ["Merkki", "kunta", "Määrä"]
        if (hae_malleittain):
            columns.insert(1, "Malli")
        df = pd.DataFrame(records, columns=columns)

        return df
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)


def hybridiautot_luokittain(vuosi_alaraja, vuosi_ylaraja):
    if vuosi_alaraja < 1975 or vuosi_ylaraja > 2023:
        print("vuosirajaus oltava välillä 1975 - 2023")
        return
        
    cursor = luo_yhteys()
    query = f'''
        SELECT EXTRACT(YEAR FROM ar.ensirekisterointipvm), EXTRACT(MONTH FROM ar.ensirekisterointipvm), hl.lyhytselite_fi, COUNT(ar.*)
        FROM ajoneuvorekisteroinnit ar
    	LEFT JOIN hybridiluokka hl ON hl.id = ar.sahkohybridinluokka
        WHERE ar.sahkohybridi = true
        AND ar.ajoneuvoluokka = 'M1' 
        AND ar.ensirekisterointipvm >= '{str(vuosi_alaraja)}-1-1' 
        AND ar.ensirekisterointipvm <= '{str(vuosi_ylaraja)}-12-31'
        GROUP BY EXTRACT(YEAR FROM ar.ensirekisterointipvm), EXTRACT(MONTH FROM ar.ensirekisterointipvm), hl.lyhytselite_fi
        ORDER BY EXTRACT(YEAR FROM ar.ensirekisterointipvm) ASC, EXTRACT(MONTH FROM ar.ensirekisterointipvm) ASC, hl.lyhytselite_fi
    '''
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=["vuosi", "kuukausi", "hybridityyppi", "maara"])
        df.fillna("Ei määritelty", inplace=True)

        return df
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)

# Ei tullut tarvetta käyttää tätä data-analyysiin tässä työssä  
def ensirekisteroinnit_vuosittain_ominaisuuden_mukaan(ominaisuus, vuosi, luokka):
    if vuosi < 1975 or vuosi > 2023:
        print("vuosi oltava välillä 1975 - 2023")
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
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=[ominaisuus, "maara"])
        df.fillna("Ei määritelty", inplace=True)
        return df
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)

# Ei tullut tarvetta käyttää tätä data-analyysiin tässä työssä
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
        return df

    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)

def ensirekisteroinnit_vuosittain(vuosi_alaraja, vuosi_ylaraja, filteri="merkkijamalli"):
    if not filteri in ['merkkijamalli','merkki','malli']:
        print("Virheellinen filteri")
        return 
        
    if vuosi_alaraja < 1975 or vuosi_ylaraja > 2023:
        print("vuosirajaus oltava välillä 1975 - 2023")
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
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=kolumnit)
        df.fillna("Ei määritelty", inplace=True)
        return df
        
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)
  
def ensirekisteroidyt_merkit_vuosittain_aikavalilla(vuosi_alaraja, vuosi_ylaraja):
    if vuosi_alaraja < 1975 or vuosi_ylaraja > 2023:
        print("vuosirajaus oltava välillä 1975 - 2023")
        return
    
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
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=kolumnit)
        df.fillna("Ei määritelty", inplace=True)
        return df
        
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)

def rekisteroityjen_autojen_tiedot_aikavalilla(vuosi_alaraja, vuosi_ylaraja):
    if vuosi_alaraja < 1975 or vuosi_ylaraja > 2023:
        print("vuosirajaus oltava välillä 1975 - 2023")
        return
        
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
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=["merkkiselvakielinen", "vari", "korityyppi", "ohjaamotyyppi", 
            "istumapaikkojenlkm", "omamassa", "kayttovoima", "iskutilavuus", 
        	"suurinnettoteho", "sylintereidenlkm", "ahdin", "sahkohybridi",
        	"vaihteistotyyppi", "vaihteidenlkm", "kunta", "matkamittarilukema"])
        return df
        
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)

def datan_puutteellisuus_kolumnittain_henkiloauto_luokassa():
    cursor = luo_yhteys()
    query = f'''
        SELECT 
        	COUNT(CASE WHEN merkkiselvakielinen IS NULL THEN 1 END) AS merkki,
          	COUNT(CASE WHEN vari IS NULL THEN 1 END) AS vari,
        	COUNT(CASE WHEN korityyppi IS NULL THEN 1 END) AS korityyppi,
          	COUNT(CASE WHEN ohjaamotyyppi IS NULL THEN 1 END) AS ohjaamotyyppi,
          	COUNT(CASE WHEN istumapaikkojenlkm IS NULL THEN 1 END) AS istumapaikkojenlkm,
          	COUNT(CASE WHEN omamassa IS NULL THEN 1 END) AS omamassa,
          	COUNT(CASE WHEN kayttovoima IS NULL THEN 1 END) AS kayttovoima,
          	COUNT(CASE WHEN iskutilavuus IS NULL THEN 1 END) AS iskutilavuus,
          	COUNT(CASE WHEN suurinnettoteho IS NULL THEN 1 END) AS suurinnettoteho,
        	COUNT(CASE WHEN sylintereidenlkm IS NULL THEN 1 END) AS sylintereidenlkm,
          	COUNT(CASE WHEN ahdin IS NULL THEN 1 END) AS ahdin,
        	COUNT(CASE WHEN vaihteistotyyppi IS NULL THEN 1 END) AS vaihteisto,
        	COUNT(CASE WHEN vaihteidenlkm IS NULL THEN 1 END) AS vaihteidenlkm,
        	COUNT(CASE WHEN kunta IS NULL THEN 1 END) AS kunta,
        	COUNT(CASE WHEN matkamittarilukema IS NULL THEN 1 END) AS matkamittarilukema,
        	COUNT(ID) AS yhteensa
        FROM ajoneuvorekisteroinnit
        WHERE ajoneuvoluokka = 'M1'
        	
        UNION
        
        SELECT 
        	ROUND(100 - (COUNT(CASE WHEN merkkiselvakielinen IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4)  AS merkki,
        	ROUND(100 - (COUNT(CASE WHEN vari IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS vari,
          	ROUND(100 - (COUNT(CASE WHEN korityyppi IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS korityyppi,
          	ROUND(100 - (COUNT(CASE WHEN ohjaamotyyppi IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS ohjaamotyyppi,
          	ROUND(100 - (COUNT(CASE WHEN istumapaikkojenlkm IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS istumapaikkojenlkm,
          	ROUND(100 - (COUNT(CASE WHEN omamassa IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS omamassa,
          	ROUND(100 - (COUNT(CASE WHEN kayttovoima IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS kayttovoima,
          	ROUND(100 - (COUNT(CASE WHEN iskutilavuus IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS iskutilavuus,
          	ROUND(100 - (COUNT(CASE WHEN suurinnettoteho IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS suurinnettoteho,
        	ROUND(100 - (COUNT(CASE WHEN sylintereidenlkm IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS sylintereidenlkm,
          	ROUND(100 - (COUNT(CASE WHEN ahdin IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS ahdin,
        	ROUND(100 - (COUNT(CASE WHEN vaihteistotyyppi IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS vaihteisto,
        	ROUND(100 - (COUNT(CASE WHEN vaihteidenlkm IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS vaihteidenlkm,
        	ROUND(100 - (COUNT(CASE WHEN kunta IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS kunta,
        	ROUND(100 - (COUNT(CASE WHEN matkamittarilukema IS NULL THEN 1 END) * 100.0 / COUNT(ID)), 4) AS matkamittarilukema,
        	0 AS yhteensa
        FROM ajoneuvorekisteroinnit
        WHERE ajoneuvoluokka = 'M1'
        ORDER BY yhteensa DESC
    '''
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=["merkki", "vari", "korityyppi", "ohjaamotyyppi", 
            "istumapaikkojenlkm", "omamassa", "kayttovoima", "iskutilavuus", 
        	"suurinnettoteho", "sylintereidenlkm", "ahdin",
        	"vaihteisto", "vaihteidenlkm", "kunta", "mittarilukema", "yhteensa"])
        
        return df
        
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)

def ensirekisteroinnit_vuosittain_henkiloauto_luokassa(vuosi_alaraja, vuosi_ylaraja):
    if vuosi_alaraja < 1975 or vuosi_ylaraja > 2023:
        print("vuosirajaus oltava välillä 1975 - 2023")
        return
        
    cursor = luo_yhteys()
    query = f''' 
        SELECT COUNT(ar.ensirekisterointipvm) AS yhteensa'''

    kolumnit = ['yhteensa']
    for vuosi in range(vuosi_alaraja, vuosi_ylaraja + 1, 1):
        query += f''', 
            COUNT(CASE WHEN EXTRACT(YEAR FROM ar.ensirekisterointipvm) = {vuosi} THEN 1 END) AS vuosi{vuosi}'''
        kolumnit.append('vuosi' + str(vuosi))

    query += f''' 
          FROM ajoneuvorekisteroinnit ar
          WHERE ar.ajoneuvoluokka = 'M1' AND ar.ensirekisterointipvm IS NOT NULL; '''

    try:
        cursor.execute(query)
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=kolumnit)
        df.fillna("Ei määritelty", inplace=True)
        return df
        
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)

def rekisteroinnit_ajokilometreittain_kunnissa_aikavalilla(kunnat, vuosi_alaraja, vuosi_ylaraja, vali, rajaus):
    if vuosi_alaraja < 1975 or vuosi_ylaraja > 2023:
        print("vuosirajaus oltava välillä 1975 - 2023")
        return
        
    cursor = luo_yhteys()
    query = f''' 
        SELECT
            ku.selite_fi AS kunta,
            FLOOR(ar.matkamittarilukema / {vali}) * {vali} AS ajokilometrit,
            COUNT(ar.id) AS maara
        FROM ajoneuvorekisteroinnit ar
        JOIN kunta ku ON ku.id = ar.kunta
        WHERE ajoneuvoluokka = 'M1'
        AND ar.matkamittarilukema IS NOT NULL 
        AND ar.matkamittarilukema < {rajaus}
        AND ensirekisterointipvm >= '{str(vuosi_alaraja)}-01-01' 
        AND ensirekisterointipvm <= '{str(vuosi_ylaraja+1)}-01-01'
        AND ku.selite_fi IN {tuple(kunnat)}
        GROUP BY
            GROUPING SETS ((ku.selite_fi, FLOOR(matkamittarilukema / {vali})), (ku.selite_fi))
        ORDER BY maara DESC;'''

    try:
        cursor.execute(query)
        records = cursor.fetchall()
        cursor;
        column2 = "ajokilometrit (" + str(rajaus) +")"
        df = pd.DataFrame(records, columns=["kunta","ajokilometrit", "maara"])
        df['ajokilometrit'] = df['ajokilometrit'].astype(str)
        return df
        
    except (Exception, psycopg2.Error) as error:
        print("Error while executing SQL query:", error)
