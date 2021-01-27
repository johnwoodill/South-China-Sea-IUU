#!/home/server/pi/homes/woodilla/.conda/envs/baseDS_env/bin/python

import spatialIUU.processGFW as siuu
import os
import pandas as pd
from datetime import datetime
import glob

if __name__ == "__main__":

    # Set global constants
    global GFW_DIR, GFW_OUT_DIR_CSV, PROC_DATA_LOC, MAX_SPEED, REGION, lon1, lon2, lat1, lat2

    PROJECT_NAME = 'South-China-Sea-IUU'

    siuu.GFW_DIR = f"/data2/GFW_point/"
    siuu.GFW_OUT_DIR_CSV = f"/home/server/pi/homes/woodilla/Projects/{PROJECT_NAME}/tmp/gfw_point/"
    siuu.PROC_DATA_LOC = f"/home/server/pi/homes/woodilla/Projects/{PROJECT_NAME}/data/"
    siuu.REGION = f"{PROJECT_NAME}"
    siuu.MAX_SPEED = 32

    # Check if dir exists and create
    os.makedirs(siuu.GFW_OUT_DIR_CSV, exist_ok=True) 
    os.makedirs(siuu.PROC_DATA_LOC, exist_ok=True) 
    os.makedirs(f"tmp/daily_ks/", exist_ok=True) 
    os.makedirs(f"tmp/interp_hr/", exist_ok=True) 
    os.makedirs(f"tmp/NN_dist/", exist_ok=True) 
    os.makedirs(f"{siuu.PROC_DATA_LOC}daily_ks/", exist_ok=True) 
    
    remove_files = True
    if remove_files == True:
        print("Removing gfw_point files")
        print("Removing interp_hr files")
        print("Removing NN_dist files")
        print("Removing daily_ks files")
        print("Removing gfw_point_csv files")
        gfw_point_files = glob.glob(f"tmp/gfw_point/*.csv")
        interp_hr_files = glob.glob(f"tmp/interp_hr/*.csv")
        nn_dist_files = glob.glob(f"tmp/NN_dist/*.csv")
        daily_ks_files = glob.glob(f"tmp/daily_ks/*.csv")
        for f in gfw_point_files:
            os.remove(f)        
        for f in interp_hr_files:
            os.remove(f)        
        for f in nn_dist_files:
            os.remove(f)
        for f in daily_ks_files:
            os.remove(f)




    # ----------------------
    # April 30 -- May 13 2016 -- May 28 (Narcosub first event)
    # Off the coast of Candelilla de la Mar, close to the border with Ecuador
    # LAT = 1.472420
    # LON = -79.006601
    
    # Event dates
    # beg_date = '2016-04-30'
    # end_date = '2016-05-28'

    # Null dates    
    # beg_date = '2016-05-29'
    # end_date = '2016-06-29'
    
    # LAT_LON_MARGIN = 2
    
    # timezone_ = f"America/Bogota"
    # ----------------------
        
    
    
    
    # ----------------------
    # Event in South China Sea Indonesia May 28, 2016
    LON = 108.207996
    LAT = 3.996386
    
    # # Event dates
    # # beg_date = '2016-05-12'
    # # end_date = '2016-06-11'
    
    # NULL dates
    beg_date = '2016-06-12'
    end_date = '2016-07-12'
    
    LAT_LON_MARGIN = 2
    
    timezone_ = f"Asia/Jakarta"
    # ----------------------
    
    
    # --------------------
    # March 20 - April 5 - April 20, 2017
    # Illegal Fishing Boat With 59 Domincan Republic Nationals On Board Seized

    # LAT = 17.003387
    # LON = -78.385937
    
    # # Event dates
    # # beg_date = '2017-03-10'
    # # end_date = '2017-04-20'
    
    # # Null dates
    # beg_date = '2017-04-21'
    # end_date = '2017-05-21'

    # LAT_LON_MARGIN = 4.5
    
    # timezone_ = f"America/Dominica"
    # ----------------------
    
    
    
    # ----------------------
    # Siera Lion and Liberia event (Sept 28, 2017)
    # LON = -11.881236
    # LAT = 6.415931
    
    # Event dates
    # beg_date = '2017-09-13'
    # end_date = '2017-10-13'

    # Null dates
    # beg_date = '2017-10-14'
    # end_date = '2017-11-14'

    # LAT_LON_MARGIN = 1.5

    # timezone_ = f"Africa/Monrovia"

    # ----------------------
    
    
    
    
    # ----------------------
    # Colombia Event
    # siuu.lon1 = -96.517685
    # siuu.lon2 = -77.515387
    # siuu.lat1 = -5.186610
    # siuu.lat2 = 15.709619
    
    # # Old Dates
    # # # beg_date = '2018-03-10'
    # # # end_date = '2018-04-22'
    
    # # Month Date
    # # beg_date = '2018-03-28'
    # # end_date = '2018-04-22'
    
    # # Null Date
    # beg_date = '2018-04-23'
    # end_date = '2018-05-23'
    
    # timezone_ = f"America/Bogota"
    # ----------------------
    
    
    
    # ----------------------    
    # JUNE 19, 2018 Malaysia arrests 12 Vietnamese for illegal fishing

    # LAT = 6.867462
    # LON = 116.937133
    
    # # Event dates
    # # beg_date = '2018-06-03'
    # # end_date = '2018-07-03'
    
    # # Null dates
    # beg_date = '2018-07-04'
    # end_date = '2018-08-04'

    # LAT_LON_MARGIN = 1.5
        
    # timezone_ = f"Asia/Kuala_Lumpur"
    # ----------------------
    
    
    
    
    
    
    
    
    
    # ----------------------
    # Event in Korea (10-7-2016)
    # LON = 124.744211
    # LAT = 37.769946
    
    # beg_date = '2016-09-22'
    # end_date = '2016-10-22'
    # ----------------------
    
    
    # ----------------------
    # Event in Korea (11-1-2016)
    # LON = 126.103313
    # LAT = 37.545004
    
    # beg_date = '2016-10-15'
    # end_date = '2016-11-15'
    # ----------------------
    
    # ----------------------
    # Patagonia Shelf for Zhuosen
    # lat: -40 - -50 Lon: -70 - -60)?
    
    # beg_date = '2018-01-01'
    # end_date = '2018-04-01'
    
    # # lon1 = -70
    # # lon2 = -60
    # # lat1 = -50
    # # lat2 = -40

    # siuu.lon1 = -70
    # siuu.lon2 = -60
    # siuu.lat1 = -50
    # siuu.lat2 = -40
    # ----------------------
    
    


    

    # ----------------------
    
    # LAT_LON_MARGIN = 4.5
    # timezone_ = f"Africa/Monrovia"
        
    siuu.lon1 = LON - LAT_LON_MARGIN
    siuu.lon2 = LON + LAT_LON_MARGIN
    siuu.lat1 = LAT - LAT_LON_MARGIN
    siuu.lat2 = LAT + LAT_LON_MARGIN

    GFW_DIR = f"/data2/GFW_point/"
    GFW_OUT_DIR_CSV = f"/home/server/pi/homes/woodilla/Data/GFW_point/{PROJECT_NAME}/csv/"
    # GFW_OUT_DIR_FEATHER = f"/home/server/pi/homes/woodilla/Data/GFW_point/{PROJECT_NAME}-IUU/feather/"
    PROC_DATA_LOC = f"/home/server/pi/homes/woodilla/Projects/{PROJECT_NAME}/data/"
    REGION = f"{PROJECT_NAME}"
    MAX_SPEED = 32


    # lon1 = LON - LAT_LON_MARGIN
    # lon2 = LON + LAT_LON_MARGIN
    # lat1 = LAT - LAT_LON_MARGIN
    # lat2 = LAT + LAT_LON_MARGIN




    # -------------------------------------------------------
    ### Compile Data
    siuu.compileData(beg_date, end_date)

    print("Cleaning up data")
    # Combine processed files
    mdat = pd.read_feather(f"data/{PROJECT_NAME}_{beg_date}_{end_date}.feather")

    mdat = mdat.sort_values('timestamp')

    # len(mdat.timestamp.unique())
    # len(mdat.vessel_A.unique())

    print(f"Adjusting Timestamp to {timezone_}")
    # Get unique timestamps
    unique_timestamps = mdat.timestamp.unique()
    unique_timestamps = pd.DatetimeIndex(unique_timestamps, tz='UTC')
    unique_timestamps = pd.to_datetime(unique_timestamps, format="%Y-%m-%d %H:%M:%S")
    
    # Convert to timezone
    converted_timestamps = unique_timestamps.tz_convert(timezone_)
    converted_timestamps = converted_timestamps.strftime("%Y-%m-%d %H:%M:%S")
    converted_timestamps = pd.to_datetime(converted_timestamps, format="%Y-%m-%d %H:%M:%S")
    
    # Merge on timezone change
    unique_timestamps = unique_timestamps.strftime("%Y-%m-%d %H:%M:%S")
    merge_timestamps = pd.DataFrame({'timestamp': pd.Series(unique_timestamps), 'converted_timestamps': pd.Series(converted_timestamps)})
        
    mdat = mdat.merge(merge_timestamps, on='timestamp')
    
    # Select columns
    mdat = mdat[['converted_timestamps', 'vessel_A', 'vessel_B', 'vessel_A_lat', 
                 'vessel_A_lon',  'vessel_B_lat', 'vessel_B_lon', 'NN', 'distance']]
    mdat = mdat.rename(columns={'converted_timestamps': 'timestamp'})
    
    # mdat_timestamp = pd.DatetimeIndex(mdat.timestamp, tz='UTC')
    # mdat_timestamp = mdat_timestamp.tz_convert('Asia/Ho_Chi_Minh')
    # mdat_timestamp = mdat_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    # mdat.loc[:, 'timestamp'] = mdat_timestamp
    # mdat.loc[:, 'timestamp'] = pd.to_datetime(mdat.timestamp, format="%Y-%m-%d %H:%M:%S")

    print(f"Saving processed file: data/{PROJECT_NAME}_Processed_{beg_date}_{end_date}.feather")
    # Save Processed
    mdat = mdat.reset_index(drop=True)
    mdat.to_feather(f"data/{PROJECT_NAME}_Processed_{beg_date}_{end_date}.feather")
