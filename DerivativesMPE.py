# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 14:43:59 2020

@author: ELeibowitz
"""

###############################################################################
#DV01 and Revenues
###############################################################################
def DV01 (notional,tenor,adjustment):
    return notional*tenor*adjustment/10000


def Revenue(DV01,grossmarkup,execution):
    return DV01*(grossmarkup-execution)

Revenue(17008,30,5)




###############################################################################
#MPE
###############################################################################

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

MPE(5813400,4,100)




###############################################################################
#Exposure Report Generation
###############################################################################
#Tasks
#Find best report(s) to run to integrate into single dataframe
#first match the 7/15 report from DPI and then re-run for June 30



###IMPORTS
import pandas as pd
import numpy as np
import calendar as cal
from datetime import datetime
import os



###FILE MANAGEMENT
workdir = os.path.join(r"G:\Long Term Retention\Project Support\t651-Derivative Path\Monthly Reports\20210331")
mtmreport = os.path.join(workdir, "2021-03-31_MTM_CLIENT_2143504503.xlsx")
pfereport = os.path.join(workdir, "2021-03-31_PFE_CLIENT_2143504503.xlsx")
#loanIDpath = os.path.join(workdir,"Trades.xlsx")
approvedpath = os.path.join(workdir, "2021-03-31_Swap_Exposure_Report_2143504503.xlsx")
ratingpath = os.path.join(workdir, "Commercial March - Validation DM.xlsx")
rowcount=339 #of the MTM spreadsheet, excluding header row 
report_date=datetime(2021,3,31)

#MTMpath=os.path.join(r"G:\Long Term Retention\Project Support\t651-Derivative Path\Monthly Reports\20201130\2020-11-30_MTM_CLIENT_2143504503.xlsx")
dfDerivs=pd.read_excel(mtmreport,header=4,thousands=',',parse_dates=['Mat Date','Eff Date'],nrows=rowcount)
dfDerivs['DPI Id'] = dfDerivs['DPI Id'].astype("category")
#dfDerivs['Mat Date'] = dfDerivs['Mat Date'].astype("datetime64")

#filter for just customer swaps
dfDerivs=dfDerivs[dfDerivs['Portfolio']=='Borrower Trades']

#MPE calculations
#first adjust the years to maturity figures
dfDerivs['YearsTM@Inception']=(dfDerivs['Mat Date'].sub(dfDerivs['Eff Date']).dt.days.div(365.25).round(4))
dfDerivs['YearsTM@Current']=(dfDerivs['Mat Date'].sub(report_date).dt.days.div(365.25).round(4))

dfDerivs['BKU']=dfDerivs.apply(lambda p: BKUcreditPeak(p['Original Notional'],p['YearsTM@Inception']),axis=1)
dfDerivs['CFM']=dfDerivs.apply(lambda p: CFM(p['Original Notional'],p['YearsTM@Inception']),axis=1)
dfDerivs['CEM']=dfDerivs.apply(lambda p: CEM(p['Current Notional'],p['YearsTM@Current'],p['MTM']),axis=1)

#Join PFE
#only grab the columns that I need from PFE Client Report
dfPFE=pd.read_excel(pfereport,header=4,thousands=',',usecols=['DPI Id','PFE'],index_col='DPI Id')
dfDerivs['PFE']=dfDerivs['DPI Id'].map(dfPFE['PFE']).replace(np.nan,"Missing")

#Join Loan number - should be able to migrate to swap exposure report below since trade ref now added to that report
#Loanpath=os.path.join(r"G:\Long Term Retention\Project Support\t651-Derivative Path\Monthly Reports\20201130\Trades.xlsx")
#dfLoanNo=pd.read_excel(loanIDpath,header=2,thousands=',',usecols=['DPI ID','Loan Number'],index_col='DPI ID')
#dfLoanNo.replace(np.nan,"Missing",inplace=True)
#dfLoanNo.index=dfLoanNo.index.str.strip()
#dfLoanNo.index=dfLoanNo.index.str.replace('DPI','')
#dfLoanNo.index = dfLoanNo.index.astype(int)
#dfLoanNo.index.name='DPI Id'
#dfDerivs['Loan No']=dfDerivs['DPI Id'].map(dfLoanNo['Loan Number'])

#Join original approved exposure and Loan Number from Swap Exposure report 
dfApproved=pd.read_excel(approvedpath,header=3,thousands=',',usecols=['Trade Ref', 'Loan Number','Approved Exposure'],index_col='Trade Ref',skipfooter=9)
dfApproved=dfApproved.reset_index().dropna().set_index('Trade Ref')
dfApproved.index = dfApproved.index.astype(str)
dfApproved.index.name='Trade Id'
dfDerivs['BKU Approved']=dfDerivs['Trade ID'].map(dfApproved['Approved Exposure'])
dfDerivs['Loan No']=dfDerivs['Trade ID'].map(dfApproved['Loan Number'])

#Join Credit Rating and Days Past Due from Loan Tape
#Ratingpath=os.path.join(r"G:\Long Term Retention\Project Support\t651-Derivative Path\Monthly Reports\20201130\11-2020 Commercial Card Validation DM.xlsx")
types_dict = {'Loan Account Number': str, 'Days Past Due': float,'Loan Borrower Risk Rating':float}

dfRatings=pd.read_excel(ratingpath,thousands=',',header=3,usecols=['Loan Account Number','Loan Borrower Risk Rating','Days Past Due'],index_col='Loan Account Number',dtype=types_dict)
#dfDerivs['CreditRating']=dfDerivs['Loan No'].map(dfRatings['Loan Borrower Risk Rating']).replace(np.nan,"")
dfDerivs['CreditRating']=dfDerivs['Loan No'].map(dfRatings[dfRatings.index.notnull()]['Loan Borrower Risk Rating']).replace(np.nan,"")
dfDerivs['Days Past Due']=dfDerivs['Loan No'].map(dfRatings[dfRatings.index.notnull()]['Days Past Due']).replace(np.nan,"Blank")

#Output the report
dfDerivs.to_clipboard()


###############################################################################
#PARCAT OF MTM REPORT
###############################################################################

#Create the dfDerivs dataframe above
dfDerivs['Cleared Exchange'].replace(np.nan,'Uncleared',inplace=True)

import plotly.offline as py
import plotly.graph_objects as go
from ipywidgets import widgets
from plotly.offline import plot



fparcat=go.Figure()
categorical_dimensions = ['Portfolio','Cleared Exchange']
dimensions = [dict(values=dfDerivs[label], label=label) for label in categorical_dimensions]
fparcat.add_trace(go.Parcats(
    domain={'y': [0, 0.8]}, dimensions=dimensions,
    #line={'colorscale': blue, 'cmin': 0,
     #     'cmax': 1, 'color': color, 'shape': 'hspline'}))
))


plot(fparcat) #b

print(categorical_dimensions)

fparcat.show(renderer="browser")
outputFilepath=os.path.join('C:\\Users\\eleibowitz\\Desktop\\Swaps.html')
fparcat.write_html(outputFilepath)
