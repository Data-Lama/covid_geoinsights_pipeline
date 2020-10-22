import numpy as np
import pandas as pd
import os 
import scipy.stats as stats    
from scipy.stats import gamma  
import matplotlib.pyplot as plt


FIS_key       = 'FIS' 
FDIAG_key     = 'Fecha diagnostico'
poly_id_key   = 'Código DIVIPOLA'
poly_name_key = 'Ciudad de ubicación'
poly_dpto_key = 'Departamento o Distrito '
df = pd.read_csv('/Users/chaosdonkey06/Dropbox/covid_fb/data/data_stages/colombia/raw/cases/cases_raw.csv', usecols=[FIS_key, FDIAG_key, poly_id_key , poly_name_key, poly_dpto_key], )  


df[FIS_key]   = pd.to_datetime(df[FIS_key])
df[FDIAG_key] = pd.to_datetime(df[FDIAG_key])
df['time_between_FIS_DIAG'] = df[FDIAG_key] - df[FIS_key]
# drop asymptomatic cases
df = df.dropna()
df['time_between_FIS_DIAG_int'] =  df['time_between_FIS_DIAG'].apply(lambda x: int(x.days))
 
df = df.query( "time_between_FIS_DIAG_int>=0 and time_between_FIS_DIAG_int<=60" ) 

path_to_delay = os.path.join('/Users/chaosdonkey06/Dropbox/covid_fb/analysis/colombia/geometry')


def estimate_gamma_delay(df, agglomeration_type, polys_ids):
    polys_ids = [11001, 5001]
    df = df[ df[poly_id_key].isin(polys_ids) ]
    fit_alpha, fit_loc, fit_beta = stats.gamma.fit(df['time_between_FIS_DIAG_int'], floc = -1)
    mean_g =  fit_alpha*fit_beta
    var_g  = fit_alpha*fit_beta**2

    x = np.linspace(-1, 6, df['time_between_FIS_DIAG_int'].max()+1)
    pdf_fitted = gamma.pdf(x, *(fit_alpha, fit_loc, fit_beta))

    bins, counts = np.unique(df['time_between_FIS_DIAG_int'], return_counts=True)
    counts = counts/np.sum(counts)


    fig, ax = plt.subplots(1,1, figsize=(15.5, 7))
    ax.plot(x, pdf_fitted, color='r', label = 'Distribución estimada (Gamma)')
    ax.bar( bins, counts, facecolor='k', alpha=0.3, label = 'Tiempos observados')
    xlim = ax.axes.get_xlim()
    #ax.set_xlim([xlim[0], 60-xlim[0]+xlim[0]/2])
    ax.set_xlabel('Tiempo en días entre FIS y fecha de diagnóstico')
    ax.set_ylabel('Frecuencia')
    ax.legend()
    plt.show()
    fig.savefig()