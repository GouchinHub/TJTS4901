import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import kyselykirjasto as kk
import warnings

def hybridiajoneuvojen_maaran_kehitys(vuosi_alaraja, vuosi_ylaraja):
    df = kk.hybridiautot_luokittain(vuosi_alaraja, vuosi_ylaraja)
    df['date'] = pd.to_datetime(df['vuosi'].astype(str) + '-' + df['kuukausi'].astype(str), format='%Y-%m')
    condition = df['hybridityyppi'] == 'Ei määritelty'
    df = df[~condition]
    df_pivot = df.pivot(index='date', columns='hybridityyppi', values='maara')
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    df_pivot.plot(ax=ax1, y='Pelkästään polttomoottorilla ladattava', marker='o', label='Pelkästään polttomoottorilla ladattava')
    ax1.set_title('Hybridiajoneuvojen määrän kehitys')
    ax1.set_xlabel('Aika')
    ax1.set_ylabel('Määrä - Pelkästään polttomoottorilla ladattava')
    ax1.legend(title='Hybridiajoneuvon tyyppi',loc=(0.05, 0.875))
    
    ax2 = ax1.twinx()
    ax2.set_ylabel('Määrä - Sähköverkosta ladattava') 
    
    #ax2.set_ylim(0, 200) 
    df_pivot.plot(ax=ax2, y='Sähköverkosta ladattava', marker='s', linestyle='--', label='Sähköverkosta ladattava', color="pink")
    # Creating a legend for the second axis
    ax2.legend(loc=(0.05, 0.8))
    plt.grid(True)
    return plt.show()

def top10_myydyinta_automerkkia(vuosi_alaraja, vuosi_ylaraja):
    df = kk.ensirekisteroinnit_vuosittain(vuosi_alaraja, vuosi_ylaraja, 'merkki')
    df['Total'] = df[df.columns[1:]].sum(axis=1)
    df_sorted = df.sort_values(by='Total', ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    for index, row in df_sorted.iterrows():
        plt.plot(df.columns[1:-1], row[1:-1], marker='o', label=row['merkki'])
    plt.title('10 myydyintä automerkkiä')
    plt.xlabel('Vuosi')
    plt.ylabel('Myytyjen autojen määrä')
    plt.legend(title='Automerkki', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    return plt.show()

def prosentuaalinen_muutos_automerkeittain(vuosi_alaraja, vuosi_ylaraja):
    df = kk.ensirekisteroinnit_vuosittain(vuosi_alaraja, vuosi_ylaraja, 'merkki')
    df['Total'] = df[df.columns[1:]].sum(axis=1)
    df_sorted = df.sort_values(by='Total', ascending=False).head(10)
    merkit = df_sorted["merkki"].values
    df_pc_changes = df_sorted.drop(columns=["merkki", "Total"]).transpose().pct_change() * 100
    df_pc_changes = df_pc_changes.transpose()
    df_pc_changes["merkki"]= merkit
    df_pc_changes.set_index('merkki', inplace=True)
    df_transposed = df_pc_changes.transpose()
    
    plt.figure(figsize=(15, 6))
    df_transposed.plot(marker='o', linestyle='-', ax=plt.gca())
    plt.title('Prosentuaalinen muutos automerkeittäin')
    plt.xlabel('Vuosi')
    plt.ylabel('Prosentuaalinen muutos')
    plt.legend(title='Automerkki', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    return plt.show()

def sahkoautojen_ensirekisteroinnit_merkeilla(vuosi_alaraja, vuosi_ylaraja, merkit):
    df = kk.ensirekisteroinnit_vuosittain(vuosi_alaraja, vuosi_ylaraja, 'merkki')
    df['Total'] = df[df.columns[1:]].sum(axis=1)
    df = df[df['merkki'].isin(merkit)]
    df_sorted = df.sort_values(by='Total', ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    for index, row in df_sorted.iterrows():
        plt.plot(df.columns[1:-1], row[1:-1], marker='o', label=row['merkki'])
    plt.title('Sähköautojen ensirekisteröinnit')
    plt.xlabel('Vuosi')
    plt.ylabel('Rekisteröityjen autojen määrä')
    plt.legend()
    plt.grid(True)
    return plt.show()


def rekisterointien_osuus_ajokilometriluokittain_eri_kunnissa(kunnat, vuosi_alaraja, vuosi_ylaraja, vali, rajaus):
    df = kk.rekisteroinnit_ajokilometreittain_kunnissa_aikavalilla(kunnat, vuosi_alaraja, vuosi_ylaraja, vali, rajaus)
    
    df['ajokilometrit'] = pd.to_numeric(df['ajokilometrit'], errors='coerce')
    df = df.dropna(subset=['ajokilometrit'])
    
    rekisteroinnit_per_kunta = df.groupby('kunta')['maara'].sum()
    df['prosentit'] = df.apply(lambda row: (row['maara'] / rekisteroinnit_per_kunta[row['kunta']]) * 100, axis=1)
    pivot_df = df.pivot(index='kunta', columns='ajokilometrit', values='prosentit')
    
    warnings.filterwarnings("ignore", category=matplotlib.MatplotlibDeprecationWarning)
    cmap = plt.cm.get_cmap('RdYlBu', len(pivot_df.columns))

    ax = pivot_df.plot(kind='bar', stacked=True, figsize=(10, 8), cmap=cmap)
    ax.set_ylabel('Osuus prosentteina')
    ax.set_xlabel('Kunta')
    ax.set_title('Rekisteröintien osuus ajokilometriluokittain eri kunnissa')
    legend = plt.legend(title='Ajokilometrit', bbox_to_anchor=(1.05, 1), loc='upper left')
    legend.get_title().set_fontsize('10')  
    for text in legend.texts:
        text.set_fontsize('8') 
    plt.tight_layout()
    return plt.show()

def top10_suurimman_merkin_markkinaosuudet_kaupungeittain(vuosi_alaraja, vuosi_ylaraja, kunnat):
    df = kk.merkkien_ensirekisteroinnit_kunnittain(vuosi_alaraja, vuosi_ylaraja, kunnat, False)
    merkkien_määrät = df.groupby('Merkki')['Määrä'].sum()
    
    isoimmat_merkit = merkkien_määrät.sort_values(ascending=False).head(10).index
    df_top10 = df[df['Merkki'].isin(isoimmat_merkit)]
    df_pivot = df_top10.pivot_table(index='kunta', columns='Merkki', values='Määrä', aggfunc='sum')
    df_percentage = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100
    
    ax = df_percentage.plot(kind='bar', stacked=True, figsize=(12, 8))
    ax.set_xlabel('Kaupunki')
    ax.set_ylabel('Osuus ensirekisteröinneistä')
    ax.set_title('Kymmenen suurimman automerkin osuudet kaupungeittain')
    ax.legend(title='Automerkki', bbox_to_anchor=(1.05, 1), loc='upper left')
    return plt.show()

def assosisaatio_kunnan_kayttovoiman_ja_merkin_valilla(vuosi_alaraja, vuosi_ylaraja, kunnat, merkit):
    df = kk.rekisteroityjen_autojen_tiedot_aikavalilla(vuosi_alaraja, vuosi_ylaraja)
    df.head()
    
    plt.figure(figsize=(10, 8))
    
    df_filtered = df[df["kunta"].isin(kunnat)]
    df_filtered = df_filtered[df_filtered["merkkiselvakielinen"].isin(merkit)]
    kayttovoimat = ['Bensiini', 'Dieselöljy']
    df_filtered = df_filtered[df_filtered["kayttovoima"].isin(kayttovoimat)]
    
    assosiaatiot = pd.crosstab(index=df_filtered['kunta'], columns=[df_filtered['merkkiselvakielinen'],df_filtered["kayttovoima"]])
    sns.heatmap(assosiaatiot, cmap='Blues', annot=True, fmt='d')
    plt.title('Assosiaatio kunnan, käyttövoiman ja automerkin välillä')
    return plt.show()
