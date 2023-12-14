import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import kyselykirjasto as kk


def rekisterointien_osuus_ajokilometriluokittain_eri_kunnissa(kunnat, vuosi_alaraja, vuosi_ylaraja, vali, rajaus):
    df = rekisteroinnit_ajokilometreittain_kunnissa_aikavalilla(kunnat, vuosi_alaraja, vuosi_ylaraja, vali, rajaus)
    
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
    legend.get_title().set_fontsize('10')  # Set the font size for the legend title
    for text in legend.texts:
        text.set_fontsize('8') 
    plt.tight_layout()
    return plt.show()
