# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 16:11:30 2018

@author: e6on6gv
"""
import pandas as pd
from sqlalchemy import create_engine

from scraper import area_to_sqft
from scraper import mysql_url

engine = create_engine(mysql_url)

df = pd.read_sql_table('listings', engine)
df = df.drop('index', axis=1)

df['Building_SizeInterior_SqFt'] = df['Building_SizeInterior'].astype('str').apply(area_to_sqft)
df['Land_SizeTotal_SqFt'] = df['Land_SizeTotal'].astype('str').apply(area_to_sqft)

df.to_sql('listings_temp',con=engine, if_exists='append', index=False)
