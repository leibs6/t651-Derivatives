# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 09:12:08 2021

@author: ELeibowitz
"""

import sqlite3 as sql
import pyodbc
from pathlib import Path
import pandas as pd
import datetime as dt
import numpy as np
import toml


# Read local `config.toml` file - this doesn't need to be updated every month, static parameters
config = toml.load(r"C:\Users\ELeibowitz\OneDrive - BankUnited, N.A\Long Term Retention\Technical\Py Code Repository\Master.toml")
#print(config)





"""
L1 - Import the MTM report 
"""
#/---------------------------------------------------------------------------------------------------------------------------
#Variables to define
#/---------------------------------------------------------------------------------------------------------------------------

report_date=dt.datetime(2022,10,31)
cur_month = '20221031'
cur_month_dash = cur_month[:4] +'-' + cur_month[4:6] + '-' + cur_month[-2:]


#Define Input File Location
path_mtm = Path(config['Projects']['SwapCustExposure']['drive'],cur_month)
fn_MTM = cur_month_dash + config['Projects']['SwapCustExposure']['fn_MTM']
rowcount=363 # of the MTM spreadsheet, excluding header row 


fn_PFE = cur_month_dash + config['Projects']['SwapCustExposure']['fn_PFE']
fn_swpExposure = cur_month_dash + config['Projects']['SwapCustExposure']['fn_swpExposure']

#Create the MTM dataframe
filepath=Path(path_mtm, fn_MTM)
dfDerivs=pd.read_excel(filepath,header=4,thousands=',',parse_dates=['Mat Date','Eff Date'],nrows=rowcount)
dfDerivs.drop(columns=['Unnamed: 28','Unnamed: 29'],inplace=True)
dfDerivs.insert(0,'PortfolioDate', report_date)

#save space by creating category columns
cats = ['DPI Id', 'Product','Entity','Type','Cleared Exchange', 'Status', 'Portfolio']
dfDerivs[cats] = dfDerivs[cats].astype('category')


#Add PFE from separate report
filepath=Path(path_mtm, fn_PFE)
dfPFE = pd.read_excel(filepath,header=4,thousands=',',usecols=['DPI Id','PFE'],index_col='DPI Id')
dfDerivs['PFE']=dfDerivs['DPI Id'].map(dfPFE['PFE']) #  .replace(np.nan,"Missing")


#/-----------------------------------------------------------------------------------------------------------------/
#upload into SQL database
#/-----------------------------------------------------------------------------------------------------------------/
pathROOTdb = (r"C:\Users\eleibowitz\OneDrive - BankUnited, N.a\Long Term Retention\Technical\Py Code Repository\sqlite_databases")
pathFNdb  = (r"Derivatives.db")
pathFULL =Path(pathROOTdb,pathFNdb)
sql_conn = sql.connect(str(pathFULL))

# Name the datatable and create it
table_name = "rptMTM"
dfDerivs.to_sql(table_name, sql_conn, if_exists = 'append', index=True)


sql_conn.close()

#/-----------------------------------------------------------------------------------------------------------------/
#Reading the SQL database
#/-----------------------------------------------------------------------------------------------------------------/
#Read in a particular date from MTM table
conn = sql.connect(str(pathFULL))
df = pd.read_sql_query('select * from rptMTM',conn) 


"""
L2
"""
#Create Cust_SwapExposure Dataframe to upload to SQL Table

#Exposure Calculations
#MPE calculations

from bisect import bisect_left
import math #for infinity

def BKUcreditPeak(notional,tenor):
    MPE1 = .02 * min(tenor,5)
    MPE2 = .01 * max(0,tenor-5)
    return notional*(MPE1 + MPE2)

#-----------------------------------------------------------------------------/
Factors_CFM=[0.015,0.03,0.06,0.12,0.3]
breaks_CFM=[1,3, 5, 10, math.inf]

def CFM(notional,tenor):
    index=bisect_left(breaks_CFM,tenor)    
    MPE=Factors_CFM[index]
    return notional * MPE 

#-----------------------------------------------------------------------------/
Factors_CEM=[0,.005,.015]
breaks_CEM=[1,5,math.inf]

def CEM(notional,tenor,MTM):
    index=bisect_left(breaks_CEM,tenor)    
    MPE=Factors_CEM[index]
    return notional * MPE + MTM

#-----------------------------------------------------------------------------/
def MPE(notional,tenor,MTM):
    return BKUcreditPeak(notional,tenor),CFM(notional,tenor), CEM(notional,tenor,MTM)


#first adjust the years to maturity figures
#filter for just customer swaps
dfCust=dfDerivs[dfDerivs['Portfolio']=='Borrower Trades']

dfCust['YearsTM@Inception']=(dfCust['Mat Date'].sub(dfCust['Eff Date']).dt.days.div(365.25).round(4))
dfCust['YearsTM@Current']=(dfCust['Mat Date'].sub(report_date).dt.days.div(365.25).round(4))

dfCust['BKU']=dfCust.apply(lambda p: BKUcreditPeak(p['Original Notional'],p['YearsTM@Inception']),axis=1)
dfCust['CFM']=dfCust.apply(lambda p: CFM(p['Original Notional'],p['YearsTM@Inception']),axis=1)
dfCust['CEM']=dfCust.apply(lambda p: CEM(p['Current Notional'],p['YearsTM@Current'],p['MTM']),axis=1)

#Swap Exposure Report
filepath=Path(path_mtm, fn_swpExposure)
dfCustSwapExpos=pd.read_excel(filepath,header=3,thousands=',',
                              usecols=['Trade Ref', 'Loan Number','Approved Exposure', 'Current Exposure','Relationship Manager'],
                              index_col='Trade Ref',dtype={"Loan Number":"str"},skipfooter=9)
#dfCustSwapExpos=dfCustSwapExpos[~dfCustSwapExpos['Product'].isin(['RPASold','RPABought'])] #this filter may not be necessary


#Retrive ratings and delinquency from FLDM.db
pathROOTdb = (r"C:\Users\eleibowitz\OneDrive - BankUnited, N.a.\Long Term Retention\Technical\Py Code Repository\sqlite_databases")
pathFNdb  = (r"FLDM.db")
pathFULL =Path(pathROOTdb,pathFNdb)
sql_conn = sql.connect(str(pathFULL))

#FLDM = pd.read_sql('SELECT * FROM AccountingMonthly', sql_conn)
FLDM = pd.read_sql("SELECT * FROM AccountingMonthly as a where a.[Reporting Date] = '{0}'".format(report_date), sql_conn, index_col='Loan Account Number')


#set data types
FLDM.index = FLDM.index.astype(str,copy=True)

#dates
fields_date = ['Origination Date','Last Renewal Date','Maturity Date','Expected Pay off Date','Last Full Payment Date',
               'Next Payment Date','Non Accrual Start Date','First Disbursement Date','Reporting Date','Date of Forbearance Agreement',
               'Date of Deferment Termination']

FLDM[fields_date]=FLDM[fields_date].astype('datetime64[D]')

#categories
fields_cat = ['Collection Officer','NAICS Code','Primary Branch Number','Primary Officer ID','Secondary Officer ID',
              'ARM Index Description','Portfolio','Product Code','Occupancy Code','SLA Indicator', 'Loan Class', 
              'Amortization Type','Current Property Status']
FLDM[fields_cat]=FLDM[fields_cat].astype('category')

#remove duplicate indices
FLDM = FLDM.loc[~FLDM.index.duplicated(), :]

#Join Credit Rating and Days Past Due from Loan Tape
dfCustSwapExpos['CreditRating']=dfCustSwapExpos['Loan Number'].map(FLDM[FLDM.index.notnull()]['Loan Borrower Risk Rating']).replace(np.nan,"")
dfCustSwapExpos['Days Past Due']=dfCustSwapExpos['Loan Number'].map(FLDM[FLDM.index.notnull()]['Days Past Due']).replace(np.nan,"Blank")
dfCustSwapExpos.index = dfCustSwapExpos.index.astype(str)

#Join dfCustSwapExpos to dfCust
dfCust = dfCust.merge(dfCustSwapExpos,how='left',left_on='Trade ID', right_index=True)
dfCust.to_clipboard()

#/-----------------------------------------------------------------------------------------------------------------/
#upload into SQL database
#/-----------------------------------------------------------------------------------------------------------------/
pathROOTdb = (r"C:\Users\eleibowitz\OneDrive - BankUnited, N.a\Long Term Retention\Technical\Py Code Repository\sqlite_databases")
pathFNdb  = (r"Derivatives.db")
pathFULL =Path(pathROOTdb,pathFNdb)
sql_conn = sql.connect(str(pathFULL))

# Name the datatable and create it
table_name = "CustomerExposures"
dfCust.to_sql(table_name, sql_conn, if_exists = 'append', index=True)

#Read from the table
#Read in a particular date from MTM table
conn = sql.connect(str(pathFULL))
dfCE = pd.read_sql_query('select * from CustomerExposures',conn) 
dfCE = pd.read_sql_query("select * from CustomerExposures as a where a.[PortfolioDate] = '{0}'".format(report_date), conn)


#/-----------------------------------------------------------------------------------------------------------------/
#Create the report
#/-----------------------------------------------------------------------------------------------------------------/

flds_report = ['Product','Entity','Counterparty','Type','Trade Date','Cleared Exchange'	,'Trade ID','DPI Id','Dealer ID',
               'Original Notional', 'Current Notional',	'Status','Eff Date','Mat Date',	'Description',	'Rec',	'Pay',	'Par Rate',	
               'DV01',	'Accrual',	'Prior DV01',	'Prior Accrual','Prior MTM','Change in DV01',	'Change in Accrual',	'Change in MTM','Portfolio',	
               'YearsTM@Inception',	'YearsTM@Current',	'MTM',	'BKU',	'CFM',	'CEM',	'PFE',	'Approved Exposure','Loan Number','CreditRating','Days Past Due']

dfCE[flds_report].to_clipboard(index=None)


