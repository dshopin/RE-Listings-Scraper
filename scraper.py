# -*- coding: utf-8 -*-
"""
Web Scraper of listing from realtor.ca
Created on Thu Dec  6 15:36:13 2018

"""

import requests
from time import sleep
from random import randint
import datetime
import logging
import re

import pandas as pd
import sqlalchemy as sq

from config import config

mysql_url = "mysql://" + \
                config['mysql_user'] + ':' + \
                config['mysql_pass'] + '@' + \
                config['mysql_host'] + ':' + \
                config['mysql_port'] + '/' + \
                config['mysql_db']

# flattening nested dict
def flatten(d, parent_key='', sep='_'):
    res = {}
    for k,v in d.items():
        new_key = k
        if parent_key:
            new_key = parent_key + sep + new_key
        if isinstance(v, dict):
            res.update(flatten(v, parent_key = new_key))
        else:
            res.update({new_key:v})
    return res


#POST request with max number of retrials
def try_post(url, data, headers, timeout, maxiter):
    for _ in range(maxiter):
            logging.info("Connection attempt #"+str(_+1))
            try:
                r = requests.post(url=url, data = data, headers = headers, timeout=timeout)
                break
            except:
                logging.exception("Connection error: ")
                sleep(2**_)
                continue
    else:
        raise requests.exceptions.ConnectionError
    return r


# converting string area to numeric
def area_to_sqft(string):
    re_area = re.compile("^([0-9\.]+)\s*x?\s*([0-9\.]*)\s*(ac|sqft|FT|m2|hec)?$", re.I)
    match = re_area.search(string)
    if not match:
        return None
    res = re_area.search(string).groups()
    if res[1] != '':
        value = float(res[0]) * float(res[1])
    else:
        value = float(res[0])
    if res[2] is not None:
        if res[2].lower() == 'ac':
            return value * 43560
        if res[2].lower() == 'm2':
            return value * 10.76
        if res[2].lower() == 'hec':
            return value * 107639
    return value
    

if __name__ == '__main__':
    logname = 'log_' + datetime.datetime.now().strftime("%Y%m%d")
    logging.basicConfig(level=logging.DEBUG,
                        handlers=[logging.FileHandler("{0}\\{1}.log".format(config['logpath'], logname), mode='w'),
                                  logging.StreamHandler()],
                        format='%(asctime)s %(message)s',
                        datefmt='%d/%m/%Y %H:%M:%S')
    
    # Scraping all Lower Mainland listings by rectangles (going by west-east rows 
    # starting from southernmost), adjusting west-east length to try to return 
    # close to 600 listings each time (site's limit):
    # 1) widen next rectangle if the current one returned less than 600
    # 2) repeat current rectangles with smaller length if returned > 600
    # 3) if 0 returned, double the  length of the next rectangle
        
    LAT_MAX = 49.770314
    LAT_MIN = 49.000000
    LONGIT_MAX = -121.664622
    LONGIT_MIN = -123.324823
    REQUEST_URL = 'https://api2.realtor.ca/Listing.svc/PropertySearch_Post'
    HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36"}
    
    formdata = {'ZoomLevel': 10,
                'CurrentPage': 1,
                'PropertyTypeGroupID': 1,
                'PropertySearchTypeId': 1,
                'TransactionTypeId': 2,
                'PriceMin': 0,
                'PriceMax': 0,
                'BedRange': '0-0',
                'BathRange': '0-0',
                'RecordsPerPage': 200,
                'ApplicationId': 1,
                'CultureId': 1,
                'Version': 7.0}
    
    listings = []
    
    south = LAT_MIN
    step_to_east = 0.1 #initial step
    
    i = 0
    while south < LAT_MAX:
        west = LONGIT_MIN
        while west < LONGIT_MAX:
            i += 1
            logging.info("Iteration: "+str(i)+"; Step to east: "+str(step_to_east))
            logging.info("Latitude from "+str(south)+" to "+str(south+0.1))
            logging.info("Longitude from "+str(west)+" to "+str(west+step_to_east))
                  
            formdata['LatitudeMax'] = south + 0.1,
            formdata['LongitudeMax'] = west + step_to_east
            formdata['LatitudeMin'] = south
            formdata['LongitudeMin'] = west
    
            r = try_post(url=REQUEST_URL, data=formdata, headers=HEADERS, timeout=2, maxiter=10)
                        
            tot_recs = r.json()['Paging']['TotalRecords']
            tot_pages = r.json()['Paging']['TotalPages']
            logging.info("Total: "+str(tot_recs)+"; Pages: "+str(tot_pages))
            
            if tot_recs == 0:
                logging.info("Skip")
                west += step_to_east
                step_to_east *= 2
                continue
            if tot_recs > 600:
                logging.info("Skip")
                step_to_east = step_to_east * (1 - (tot_recs - 600) / tot_recs)
                continue
            
            #processing the first page
            listings += r.json()['Results']
            logging.info("Page 1")
            
            #processing remaining pages
            for p in range(2,tot_pages + 1):
                formdata['CurrentPage'] = p
                r = try_post(url=REQUEST_URL, data=formdata, headers=HEADERS, timeout=2, maxiter=10)
                listings += r.json()['Results']
                logging.info("Page "+str(p))
                
            formdata['CurrentPage'] = 1
            west += step_to_east
            step_to_east = step_to_east * (1 + (600 - tot_recs) / tot_recs)
            if west + step_to_east > LONGIT_MAX:
                step_to_east = max(0.1, LONGIT_MAX - west)
            sleep(randint(1,5))
        south += 0.1
    
    # deduplicate
    unique = []
    for l in listings:
        if l not in unique:
            unique.append(l)
    logging.info(str(len(listings) - len(unique)) + " duplicates deleted")
    

   
    # keep only neccessary fields
    details = []
    for l in unique:
        details.append({'Id':l['Id'],
                        'MlsNumber':l['MlsNumber'],
                        'Building':l['Building'],
                        'Property':l['Property'],
                        'Land':l['Land'],
                        'RelativeDetailsURL':l['RelativeDetailsURL']})
    
    
    flat = [flatten(l) for l in details]
    
    
    #add load date
    for d in flat:
        d['DownloadDate'] = datetime.datetime.now().strftime("%d-%m-%Y")
            
    keys = ['Id',
            'MlsNumber',
            'Building_BathroomTotal',
            'Building_Bedrooms',
            'Building_SizeInterior',
            'Building_Type',
            'Property_Price',
            'Property_Type',
            'Property_Address_AddressText',
            'Property_Address_Longitude',
            'Property_Address_Latitude',
            'Property_TypeId',
            'Property_OwnershipType',
            'Land_SizeTotal',
            'RelativeDetailsURL',
            'Building_StoriesTotal',
            'DownloadDate']
             
    
    df=pd.DataFrame.from_records(flat, columns=keys) 
    
    
    #parse areas
    df['Building_SizeInterior_SqFt'] = df['Building_SizeInterior'].astype('str').apply(area_to_sqft)
    df['Land_SizeTotal_SqFt'] = df['Land_SizeTotal'].astype('str').apply(area_to_sqft)
    
    
    
    
    engine = sq.create_engine(mysql_url)    
    
    # 1). Snapshot table update    
    df.to_sql('listings', con=engine, if_exists='append', index=False)
    
    logging.info(str(len(df)) + " records added to the snapshot table")
    
    # delete old records
    engine.execute(sq.text('''delete from listings
                           where str_to_date(DownloadDate, '%d-%m-%Y') < date_add(now(), interval - 10 day);''').execution_options(autocommit=True))
    
    
    # 2). Incremental table update
    
    # separate bad addresses with multiple records to another table
    dups = df.groupby('Property_Address_AddressText').size()
    dups = dups[dups>1]
    
    df = df[~df['Property_Address_AddressText'].isin(dups.index)]
    
    dups_df = pd.DataFrame({'Property_Address_AddressText':dups.index,
                            'StartDate':datetime.date.today(),
                            'count(*)':dups})
        
    dups_df.to_sql('duplicates', con=engine, if_exists='append', index=False,
                   dtype={'StartDate':sq.DateTime()})
    
    
    df['StartDate'] = datetime.date.today()
    df['EndDate'] = None
    df = df.drop('DownloadDate', axis = 1)
    
    
    df.to_sql('upd', con=engine, if_exists='replace', index=False,
              dtype={'StartDate':sq.DateTime(), 'EndDate':sq.DateTime()})
    
    engine.execute(sq.text("CREATE INDEX address ON upd(Property_Address_AddressText(10))").execution_options(autocommit=True))
    
    engine.execute(sq.text("call add_incremental(CURDATE())").execution_options(autocommit=True))
    
    
    # stats
    new_listings = '''
    select Property_Address_AddressText, Id, MlsNumber
    from listings_inc
    group by Property_Address_AddressText, Id, MlsNumber
    having max(StartDate) = curdate() and max(EndDate) is Null;'''
    
    updated_listings = '''
    select Property_Address_AddressText, Id, MlsNumber
    from listings_inc
    group by Property_Address_AddressText, Id, MlsNumber
    having max(StartDate) = curdate() and max(EndDate) = curdate();'''
    
    revived_listings = '''
    select Property_Address_AddressText, Id, MlsNumber
    from listings_inc
    group by Property_Address_AddressText, Id, MlsNumber
    having max(StartDate) = curdate() and max(EndDate) < curdate();'''
    
    withdrawn_listings = '''
    select Property_Address_AddressText, Id, MlsNumber
    from listings_inc
    group by Property_Address_AddressText, Id, MlsNumber
    having max(EndDate) = curdate() and max(StartDate) < curdate();'''
    
    
                   
    logging.info(str(len(engine.execute(new_listings).fetchall())) + ' new listings')
    logging.info(str(len(engine.execute(updated_listings).fetchall())) + ' updated listings')
    logging.info(str(len(engine.execute(revived_listings).fetchall())) + ' revived listings')
    logging.info(str(len(engine.execute(withdrawn_listings).fetchall())) + ' withdrawn listings')

    
    
    
    
    
    
