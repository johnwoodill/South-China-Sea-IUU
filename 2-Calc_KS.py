import pandas as pd
import numpy as np
from scipy import stats
import multiprocessing
import glob
from scipy.spatial import distance
from scipy.stats import entropy
from math import log2
import math
from math import radians, cos, sin, asin, sqrt
import os
from pathlib import Path
import dask.dataframe as dd
from dask import compute, delayed
from dask.distributed import Client

# ProgressBar().register()

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km
  



def kl_divergence(p, q):
	return sum(p[i] * log2(p[i]/q[i]) for i in range(len(p)))


def hellinger_explicit(p, q):
    """
    Hellinger distance between two discrete distributions.
    """
    list_of_squares = []
    for p_i, q_i in zip(p, q):

        # caluclate the square of the difference of ith distr elements
        s = (math.sqrt(p_i) - math.sqrt(q_i)) ** 2

        # append 
        list_of_squares.append(s)

    # calculate sum of squares
    sosq = sum(list_of_squares)    
    return sosq / math.sqrt(2)



def calc_stats(i, j, rvs1, rvs2):
    # Calculate KS statistic
    ks_stat, pvalue = stats.ks_2samp(rvs1, rvs2)

    rvs1_dat = pd.DataFrame({'rvs1': rvs1, 'rvs1_d': pd.cut(rvs1, bins = 30, right=False)})
    rvs2_dat = pd.DataFrame({'rvs2': rvs2, 'rvs2_d': pd.cut(rvs2, bins = 30, right=False)})

    rvs1_dat.loc[:, 'pdf'] = (rvs1_dat.groupby(rvs1_dat['rvs1_d']).transform('count') / len(rvs1_dat)).values
    rvs1_pdf = rvs1_dat.groupby('rvs1_d', as_index=False).mean().pdf

    rvs2_dat.loc[:, 'pdf'] = (rvs2_dat.groupby(rvs2_dat['rvs2_d']).transform('count') / len(rvs2_dat)).values
    rvs2_pdf = rvs2_dat.groupby('rvs2_d', as_index=False).mean().pdf
   
    # print(j)
    
    return pd.DataFrame({'t': [i], 
                         'lag': [(i - j)/np.timedelta64(1,'h')], 
                         'ks': [ks_stat], 
                         'pvalue': [pvalue],
                         'kl': [kl_divergence(rvs1_pdf, rvs2_pdf)]})
                         #'js': [distance.jensenshannon(rvs1_pdf, rvs2_pdf)], 
                         #'hl': [hellinger_explicit(rvs1_pdf, rvs2_pdf)]})




def process_stats(i):
    rvs1 = dat[dat['timestamp'] == str(i)]['distance'].values
    hdat = dat[( dat['timestamp'] < i ) & ( dat['timestamp'] >= i - pd.Timedelta(hours=24*10) )]
    date_range = pd.to_datetime(hdat.timestamp.unique()).sort_values(ascending=False)    
    
    # Check if initial day
    if len(date_range) == 0:
        print("Skipping start of timestamp")
        return 1
    
    # Get time delta
    t_delta = int([(i - date_range[-1])/np.timedelta64(1, 'h')][0])
    
    dfs = [x for _, x in hdat.groupby('timestamp')]
    
    # delayed_results = [delayed(process_stats)(i) for i in list(gdat.timestamp)]
    # delayed_results = [delayed(calc_stats)(i, dfs[j]['timestamp'].iat[0], rvs1, dfs[j]['distance'].ravel()) for j in range(len(dfs))]
    # results = compute(*delayed_results)
    
    results = [calc_stats(i, dfs[j]['timestamp'].iat[0], rvs1, dfs[j]['distance'].ravel()) for j in range(len(dfs))]    
    
    # files = len(glob.glob('tmp/daily_ks/*.csv'))
    progress_bar = len(glob.glob('tmp/daily_ks/*.csv'))
    # progress_bar = round( (len(files)/progress_end)*100, 2 )
    
    if len(results) == 0:
        print(f"{i}: Not enough lag days")
        return 1
    if len(results) == t_delta:
        retdat = pd.concat(results).reset_index(drop=True)
        retdat = retdat.reset_index(drop=True)
        retdat.to_csv(f"tmp/daily_ks/{i}.csv")
        print(f"Saving: tmp/daily_ks/{i}.csv -- {len(date_range)}/{t_delta} --- {progress_bar}")
        return 0
    else:
        print(f"Failed: {i}")
        return 0





if __name__ == "__main__":

    beg_date = '2016-06-12'
    end_date = '2016-07-12'

    print(f"Opening file: data/South-China-Sea-IUU_Processed_{beg_date}_{end_date}.feather")

    # Get processed data
    dat = pd.read_feather(f"data/South-China-Sea-IUU_Processed_{beg_date}_{end_date}.feather")
    dat = dat[['timestamp', 'distance']].reset_index(drop=True) 
    dat = dat.assign(timestamp = pd.to_datetime(dat['timestamp']))

    # Filter out first time obs because nothing behind
    # first_t = sorted(dat.timestamp.unique())[0]
    # dat = dat[dat.timestamp != first_t]

    print(f"Processing daily KS files: tmp/daily_ks")
    # Filter out files already processed
    files = glob.glob('tmp/daily_ks/*.csv')
    files2 = sorted([Path(x).stem for x in files])
    mdat = dat[-dat.timestamp.isin(files2)].reset_index(drop=True)



    if len(dat) != 0:
        gdat = pd.DataFrame({'timestamp': sorted(mdat['timestamp'].unique())})
        # gdat = gdat[1:4].reset_index(drop=True)
        # gdat = gdat.assign(uid = gdat.groupby('timestamp').ngroup())
        # gb = gdat.groupby('uid')
        # gb_i = [gb.get_group(x) for x in gb.groups]
        
        # Dask implementation doesn't work                
        # client = Client(n_workers=30, threads_per_worker=1)
        
        timestamps_ = list(gdat.timestamp)
        # timestamps_ = timestamps_[0:5]       
        
        # i = timestamps_[1]
        # progress_end = len(timestamps_)
        # process_stats(timestamps_[1], progress_end)
        
        # res = [process_stats(timestamp_, len(timestamps_)) for timestamp_ in timestamps_]
        
        # delayed_results = [delayed(process_stats)(i) for i in timestamps_]
        # results = compute(*delayed_results, scheduler='processes', num_workers=3)

        
        # Standard Parallel Process
        pool = multiprocessing.Pool(12)
        pool.map(process_stats, timestamps_)
        pool.close()
    else:
        print("All files processed")


    print("Combine processed files: tmp/daily_ks/*.csv")

    # Combine processed files
    files = glob.glob('tmp/daily_ks/*.csv')


    list_ = []
    for file in files:
        df = pd.read_csv(file)
        list_.append(df)

    mdat = pd.concat(list_, sort=False)


    print(f"Saving: data/South-China-Sea-IUU_KSDaily_{beg_date}_{end_date}.csv")

    mdat = mdat.reset_index(drop=True)
    mdat.to_csv(f"data/South-China-Sea-IUU_KSDaily_{beg_date}_{end_date}.csv")


    # client.close()