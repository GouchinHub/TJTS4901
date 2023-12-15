# Ohjeet tietokannan pystyttämiseen

postgresql tietokanta on saatavilla sql dump muodossa.

dumppi tiedosto on ladattavissa osoitteesta: https://drive.google.com/file/d/1OHDhCm5aTnE6vkHV06y8HmDgmxk8SqZN/view?usp=drive_link

Tietokannan saa itselleen kun luo uuden tyhjän postgres tietokannan, ja tekee sille restoren käyttäen edellämainittua tiedostoa

Yksinkertaiset ohjeet miten tämän voi tehdä pgadminissa löytyy täältä (3:10 kohdasta eteenpäin): https://www.youtube.com/watch?v=S108Rh6XxPs&t=166s&ab_channel=Learning

Jos meidän [kyselykirjastoa](https://github.com/GouchinHub/TJTS4901/blob/main/kyselykirjasto.py) haluaa ajaa tietokantaa vasten, voi tämän repositoryn cloonata, ja muuttaa kyselykirjasto.py:n luo_yhteys() funktioon oman postgres serverin osoite, käyttäjä, ja salasana, sekä luotu tietokanta (tiedoston rivit 8-11)

