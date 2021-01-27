import pandas as pd
import numpy as np
import os as os
import glob
import multiprocessing
from datetime import datetime, timedelta
import sys
from math import radians, cos, sin, asin, sqrt
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from dask.delayed import delayed
from dask import compute
from dask.distributed import Client, progress
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



def spherical_dist_populate(lat_lis, lon_lis, r=6371.0088):
    lat_mtx = np.array([lat_lis]).T * np.pi / 180
    lon_mtx = np.array([lon_lis]).T * np.pi / 180

    cos_lat_i = np.cos(lat_mtx)
    cos_lat_j = np.cos(lat_mtx)
    cos_lat_J = np.repeat(cos_lat_j, len(lat_mtx), axis=1).T

    lat_Mtx = np.repeat(lat_mtx, len(lat_mtx), axis=1).T
    cos_lat_d = np.cos(lat_mtx - lat_Mtx)

    lon_Mtx = np.repeat(lon_mtx, len(lon_mtx), axis=1).T
    cos_lon_d = np.cos(lon_mtx - lon_Mtx)

    mtx = r * np.arccos(cos_lat_d - cos_lat_i*cos_lat_J*(1 - cos_lon_d))
    return mtx




def GFW_directories(GFW_DIR):
    """ Get GFW directory list """
    
    dirs = os.listdir(GFW_DIR)
    # Remove subfolders 'BK' and 'identities'
    if 'BK' in dirs:
        dirs.remove('BK')
    
    if 'identities' in dirs:
        dirs.remove('identities')
    
    return dirs



def calc_kph(data):
    """
    Calculate kph
    Args:
        data: DataFrame with datetime, lat, and lon
    Returns:
        Processed DataFrame
    """
    # Calculate distance traveled
    data = data.sort_values('timestamp')
    fobs_lat = data['lat'].iat[0]
    fobs_lon = data['lon'].iat[0]
    lat_lag = data['lat'].shift(1, fill_value=fobs_lat)
    lon_lag = data['lon'].shift(1, fill_value=fobs_lon)
    lat = data['lat'].values
    lon = data['lon'].values
    
    tlag = data['timestamp'].shift(1, fill_value=data['timestamp'].iat[0])
    
    # Calc distance traveled and kph
    outvalues = pd.Series()
    outvalues2 = pd.Series()
    for i in range(len(data)):
        # Calculate distance
        lat1 = lat_lag.iat[i]
        lat2 = lat[i]
        lon1 = lon_lag.iat[i]
        lon2 = lon[i]
        d = pd.Series(round(spherical_dist_populate([lat1, lat2], [lon1, lon2] )[0][1], 2))
        outvalues = outvalues.append(d, ignore_index=True)
        
        # Calculate travel time
        t1 = str(data.timestamp.iat[i])
        t2 = str(tlag.iat[i])
        
        t1 = datetime.strptime(t1, "%Y-%m-%d %H:%M:%S UTC")
        t2 = datetime.strptime(t2, "%Y-%m-%d %H:%M:%S UTC")
    
        tdiff = abs(t2 - t1)
        tdiff = pd.Series(round(tdiff.seconds/60/60, 4))
        outvalues2 = outvalues2.append(tdiff)
        
    data['dist'] = outvalues.values
    data['travel_time'] = outvalues2.values   
    data['kph'] = data['dist']/data['travel_time'] 
    data['kph'] = np.where(data['travel_time'] == 0, 0, data['kph'])
    return data



def interp_hr(data, start, end):
    '''
    Interpolate data points across start and end dates
    '''
    data = data.assign(timestamp = pd.to_datetime(data.timestamp))
    indat = data
        
    # Sort data by timestamp
    data = data.sort_values('timestamp')
    data['timestamp'] = data['timestamp'].dt.round('min')

    # Get average lat/lon incase duplicate
    data = data.groupby('timestamp', as_index=False)[['lat', 'lon']].agg('mean')

    # Merge and interpolate between start and end
    pdat = pd.DataFrame({'timestamp': pd.date_range(start=start, end=end, freq='min')})

    # Merge and interpolate
    pdat = pdat.merge(data, on='timestamp', how='left')
    pdat['lat'] = pd.Series(pdat['lat']).interpolate()
    pdat['lon'] = pd.Series(pdat['lon']).interpolate()

    # Keep on the hour
    pdat = pdat[pd.Series(pdat['timestamp']).dt.minute == 0].reset_index(drop=True)

    # Back/Forward fill
    pdat = pdat.fillna(method='bfill')
    pdat = pdat.fillna(method='ffill')
    pdat['mmsi'] = indat['mmsi'].iat[0]
 
    mmsi = indat['mmsi'].iat[0]
    pdat = pdat.reset_index(drop=True)
    pdat.to_csv(f"tmp/interp_hr/{mmsi}.csv", index=False)
    # print(pdat)
    return 0


def processGFW(ndat, GFW_DIR, MAX_SPEED, GFW_OUT_DIR_CSV, lon1, lon2, lat1, lat2):
    '''
    Process GFW in parallel function
    '''
    # Get subdirectory list of files
    i = ndat['folders'].iat[0]
    subdir = GFW_DIR + i
    allFiles = glob.glob(subdir + "/*.csv")
    
    list_ = []
    # Append files in subdir
    for file_ in allFiles:
        df = pd.read_csv(file_, index_col=None, header=0, low_memory=False)
        list_.append(df)
    
    dat = pd.concat(list_, axis = 0, ignore_index = True)
   
    # (1) Subset Region
    outdat = dat[(dat['lon'] >= lon1) & (dat['lon'] <= lon2)] 
    outdat = outdat[(outdat['lat'] >= lat1) & (outdat['lat'] <= lat2)]
  
    # Remove vessels on land
    outdat = outdat[outdat['elevation_m'] <= 0]
    
    # Groupby mmsi and get distance and time between timestamps
    outdat = outdat.groupby('mmsi').apply(calc_kph).reset_index(drop=True)

    # Determine if stationary where distance_traveled > 1
    outdat['stationary'] = np.where(outdat['kph'] > 1, 0, 1)
    outdat = outdat[outdat['stationary'] == 0]
    
    # Get max speed for each mmsi
    mmsi_kph = outdat.groupby('mmsi', as_index=False)['kph'].max()

    # Keep vessels travel less than 32kph but greater than 1 km
    mmsi_all = mmsi_kph['mmsi'].unique()
    mmsi_kph2 = mmsi_kph[mmsi_kph['kph'] <= MAX_SPEED]
    mmsi_keep = mmsi_kph2['mmsi'].unique()
    outdat = outdat[outdat['mmsi'].isin(mmsi_keep)]

    # Separate Year, month, day, hour, minute, second
    outdat.loc[:, 'timestamp'] = pd.to_datetime(outdat['timestamp'], format="%Y-%m-%d %H:%M:%S UTC")
    outdat.loc[:, 'year'] = pd.DatetimeIndex(outdat['timestamp']).year 
    outdat.loc[:, 'month'] = pd.DatetimeIndex(outdat['timestamp']).month
    outdat.loc[:, 'day'] = pd.DatetimeIndex(outdat['timestamp']).day
    outdat.loc[:, 'hour'] = pd.DatetimeIndex(outdat['timestamp']).hour
    outdat.loc[:, 'minute'] = pd.DatetimeIndex(outdat['timestamp']).minute
    outdat.loc[:, 'second'] = pd.DatetimeIndex(outdat['timestamp']).second
    
    # Organize columns
    outdat = outdat[['timestamp', 'year', 'month', 'day', 'hour', 'minute', 'second', 'mmsi', 'lat', 'lon', \
                     'kph', 'dist', 'travel_time', 'stationary', \
                     'segment_id', 'message_id', 'type', 'speed', 'course', 'heading', 'shipname', 'callsign', \
                     'destination', 'elevation_m', 'distance_from_shore_m', 'distance_from_port_m']]
    
    filename = f"{outdat['year'].iat[0]}-" + f"{outdat['month'].iat[0]}".zfill(2) + f"-" + f"{outdat['day'].iat[0]}".zfill(2)
    print(f"   Complete: {filename}")
    outdat = outdat.reset_index(drop=True)
    outdat.to_csv(f"{GFW_OUT_DIR_CSV}{filename}.csv")
    # print(i)
    return 0




def NN_dist(ndat=None, lat_lis=None, lon_lis=None):
    """
    Calculate NN distances for all vessels in sample
    """
    # In miles
    # r = 3958.75

    # In km
    r = 6371.0088
    
    ndat = ndat.reset_index()
    if ndat is not None:
        ndat = ndat.dropna()
        mmsi = ndat.mmsi
        ndat = ndat.sort_values('mmsi')
        lat_lis = ndat['lat']
        lon_lis = ndat['lon']
        timestamp = ndat['timestamp'].iat[0]

    # Calc NN distances using Circle Distance
    lat_mtx = np.array([lat_lis]).T * np.pi / 180
    lon_mtx = np.array([lon_lis]).T * np.pi / 180

    cos_lat_i = np.cos(lat_mtx)
    cos_lat_j = np.cos(lat_mtx)
    cos_lat_J = np.repeat(cos_lat_j, len(lat_mtx), axis=1).T

    lat_Mtx = np.repeat(lat_mtx, len(lat_mtx), axis=1).T
    cos_lat_d = np.cos(lat_mtx - lat_Mtx)

    lon_Mtx = np.repeat(lon_mtx, len(lon_mtx), axis=1).T
    cos_lon_d = np.cos(lon_mtx - lon_Mtx)

    mtx = r * np.arccos(cos_lat_d - cos_lat_i*cos_lat_J*(1 - cos_lon_d))

    # Build data.frame
    matdat = pd.DataFrame(mtx)
    matdat.columns = mmsi[:]
    matdat = matdat.set_index(mmsi[:])

    # Stack and form three column data.frame
    tmatdat = matdat.stack()
    lst = tmatdat.index.tolist()
    vessel_A = pd.Series([item[0] for item in lst])
    vessel_B = pd.Series([item[1] for item in lst])
    distance = tmatdat.values

    # Get lat/lon per mmsi
    posdat = ndat[['mmsi', 'lat', 'lon']]
    posdat = posdat.sort_values('mmsi')

    # Build data frame
    odat = pd.DataFrame({'timestamp': timestamp, 'vessel_A': vessel_A,
                         'vessel_B': vessel_B, 'distance': distance})
    odat = odat.sort_values(['vessel_A', 'distance'])

    # Get 05-NN
    # odat = odat.sort_values('distance').groupby('vessel_A').nth(list(np.arange(0, 100))).reset_index()
    # odat = odat.sort_values(['vessel_A', 'distance'])

    # Merge in vessel_B lat/lon
    posdat.columns = ['mmsi', 'vessel_B_lat', 'vessel_B_lon']
    odat = odat.merge(posdat, how='left', left_on='vessel_B', right_on='mmsi')

    # Merge in vessel_A lat/lon
    posdat.columns = ['mmsi', 'vessel_A_lat', 'vessel_A_lon']
    odat = odat.merge(posdat, how='left', left_on='vessel_A', right_on='mmsi')

    odat['NN'] = odat.groupby(['vessel_A'], as_index=False).cumcount()
    odat = odat.reset_index(drop=True)
    odat = odat[['timestamp', 'vessel_A', 'vessel_B', 'vessel_A_lat',
                 'vessel_A_lon', 'vessel_B_lat', 'vessel_B_lon', 'NN', 'distance']]
    odat = odat.sort_values(['vessel_A', 'NN'])
    print(f"Complete:   {timestamp}")
    odat.to_csv(f"tmp/NN_dist/{timestamp}.csv", index=False)
    # print(odat)
    return 0








def compileData(beg_date, end_date):
    """
    Calculate all data given parameters
    """
    # Get folder list
    gfw_list_dirs = sorted(GFW_directories(GFW_DIR))

    # Need to shift dates because previous date equals current date 
    # (wrong time stamp data)
    new_gfw_list_dirs = []
    for i in gfw_list_dirs:
        indate = i
        outdate = datetime.strptime(indate, "%Y-%m-%d")
        outdate = outdate + timedelta(days=-1)
        outdate = datetime.strftime(outdate, "%Y-%m-%d")
        new_gfw_list_dirs.append(outdate)

    # Subset beg_dat and end_date
    # Get start and end index from file list
    start_index = [new_gfw_list_dirs.index(i) for i in new_gfw_list_dirs if f"{beg_date}" in i]
    end_index = [new_gfw_list_dirs.index(i) for i in new_gfw_list_dirs if f"{end_date}" in i]

    # Filter
    # subtract and add 2 because of filename mix up
    nmargin = 5
    folders = new_gfw_list_dirs[start_index[0] - nmargin:end_index[0] + nmargin]

    print(f"{datetime.now()}: [1/5] - Processing GFW Folders {beg_date} to {end_date}")
    
    infiles = glob.glob(f"{GFW_OUT_DIR_CSV}/*.csv")
    infiles = [os.path.splitext(os.path.basename(x))[0] for x in infiles]
    
    # Use Dask to process
    fold_dat = pd.DataFrame({'uid': np.linspace(1, len(folders), len(folders)), 'folders': folders})
    
    # Check if file already processed
    fold_dat = fold_dat[~fold_dat['folders'].isin(infiles)]
    fold_dat = dd.from_pandas(fold_dat, npartitions=20)
    print(f"Processing {len(fold_dat)} files")
    results = fold_dat.groupby('uid').apply(lambda x: processGFW(x, GFW_DIR, MAX_SPEED, GFW_OUT_DIR_CSV, lon1, lon2, lat1, lat2), meta='f8').compute(scheduler='processes')
    
    # Standard multiprocessing
    # if parallel == True:
    #     #results = ray.get([processGFW.remote(i) for i in folders])
    #     pool = multiprocessing.Pool(ncores, maxtasksperchild=1)         
    #     pool.map(processGFW, fold_dat)
    #     pool.close()
    # else:
    #     for folder in folders: 
    #         ndat = processGFW(folder)
    
    # Dask Alternative
    # processGFW(gb_i[0])
    # client = Client(n_workers=20, threads_per_worker=1)
    # gb = fold_dat.groupby('uid')
    # gb_i = [gb.get_group(x) for x in gb.groups]
    # compute([delayed(processGFW)(v, GFW_DIR, MAX_SPEED, GFW_OUT_DIR_CSV, lon1, lon2, lat1, lat2) for v in gb_i])

    print(f"{datetime.now()}: [2/4] - Binding data {beg_date} to {end_date}")
    
    # Get feather files
    feather_files = sorted(glob.glob(GFW_OUT_DIR_CSV + "*.csv"))
    feather_files = [item.replace(f"{GFW_OUT_DIR_CSV}", '') for item in feather_files]
    feather_files = [item.replace('.csv', '') for item in feather_files]
    
    # Bind data
    list_ = []
    for file in feather_files:
        df = pd.read_csv(f"{GFW_OUT_DIR_CSV}{file}.csv")
        list_.append(df)
    
    mdat = pd.concat(list_, sort=False)

    # Ensure vessel moves at least 1 km during the period
    vessel_dist = mdat.groupby(['mmsi'], as_index=False)['dist'].sum()
    vessel_dist = vessel_dist[vessel_dist['dist'] >= 1]
    vessel_dist = vessel_dist['mmsi'].unique()

    # Keep vessels that move at least 1km    
    outdat = mdat[mdat.mmsi.isin(vessel_dist)]
    outdat = outdat.sort_values('timestamp')
    start_year = pd.DatetimeIndex(outdat['timestamp'])[0].year
    start_month = pd.DatetimeIndex(outdat['timestamp'])[0].month
    start_day = pd.DatetimeIndex(outdat['timestamp'])[0].day

    end_year = pd.DatetimeIndex(outdat['timestamp'])[-1].year
    end_month = pd.DatetimeIndex(outdat['timestamp'])[-1].month
    end_day = pd.DatetimeIndex(outdat['timestamp'])[-1].day

    start = pd.Timestamp(f"{start_year} - {start_month} - {start_day} 00:00")
    end = pd.Timestamp(f"{end_year} - {end_month} - {end_day} 23:59")
    
    # outdat.to_csv('data/Patagonia_AIS_data_2018-01-01_2018-04-01.csv', index=False)
    
    print(f"{datetime.now()}: [3/5] - Interpolating by MMSI")
    
    # Group by mmis and interpolate to hour
    outdat = dd.from_pandas(outdat, npartitions = 56)
    results = outdat.groupby('mmsi').apply(lambda x: interp_hr(x, start, end), meta='f8').compute(scheduler='processes')

    # Get all interpolated files
    csv_files = glob.glob(f"tmp/interp_hr/*.csv")    
    savedat = dd.read_csv("tmp/interp_hr/*.csv").compute()

    # Calc KPH
    savedat['lat_lead'] = savedat.groupby('mmsi', as_index=False)['lat'].shift(1)
    savedat['lon_lead'] = savedat.groupby('mmsi', as_index=False)['lon'].shift(1)
    savedat['dist'] = savedat.apply(lambda x: haversine(x['lon'], x['lat'], x['lon_lead'], x['lat_lead']), axis = 1)

    # Get proportion of distances and make sure greater than 50%    
    groups_dist = savedat.groupby('mmsi', as_index=True)['dist'].apply(np.count_nonzero).reset_index()
    groups_count = savedat.groupby('mmsi', as_index=True)['dist'].count().iat[0]
    groups_dist['prop'] = groups_dist['dist']/groups_count
    groups_dist = groups_dist[groups_dist.prop >= 0.5]
    
    savedat = savedat[savedat.mmsi.isin(groups_dist.mmsi.unique())]

    savedat = savedat.reset_index(drop=True)
    savedat.to_csv(f"{PROC_DATA_LOC}{REGION}_inter_hourly_loc_{beg_date}_{end_date}.csv")

    savedat = pd.read_csv(f"{PROC_DATA_LOC}{REGION}_inter_hourly_loc_{beg_date}_{end_date}.csv")
    
    print(f"{datetime.now()}: [4/5] - Calculating NN")
    
    infiles = glob.glob(f"tmp/NN_dist/*.csv")
    infiles = [os.path.splitext(os.path.basename(x))[0] for x in infiles]

    outdat = savedat[['timestamp', 'lat', 'lon', 'mmsi']]
    outdat = outdat.assign(uid = outdat.groupby('timestamp').ngroup())
    
    outdat = outdat[~outdat['timestamp'].isin(infiles)]
    
    outdat = dd.from_pandas(outdat, npartitions = 20)
    
    # odat = outdat.groupby('timestamp').apply(lambda x: NN_dist(x))
    odat = outdat.groupby('uid').apply(lambda x: NN_dist(x), meta='f8').compute(scheduler='processes')

    # odat2 = dd.read_csv('tmp/NN_dist/*.csv', assume_missing=True).compute()
    
    print("binding NN_dist data")
    
    files = glob.glob('tmp/NN_dist/*.csv')
    
    list_ = []
    for file in files:
        df = pd.read_csv(file)
        list_.append(df)
    
    odat2 = pd.concat(list_, sort=False)
    
    # subset days from beg_date end_date
    # odat2 = odat2[(odat2.timestamp >= f"{beg_date} 00:00:00") & (odat2.timestamp <= f"{end_date} 23:59:00")]
    odat2 = odat2[odat2.timestamp <= f"{end_date} 23:59:00"]

    print(f"{datetime.now()}: [5/5] - Saving: {PROC_DATA_LOC}{REGION}_{beg_date}_{end_date}.feather")

    odat2 = odat2.reset_index(drop=True)
    odat2.to_feather(f"{PROC_DATA_LOC}{REGION}_{beg_date}_{end_date}.feather")
