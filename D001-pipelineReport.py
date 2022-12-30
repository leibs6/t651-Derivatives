# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 11:45:32 2022

@author: eleibowitz
"""

import pandas as pd
from pathlib import Path

#one day just scrape off the web, maybe using selenium
#url = "https://app.derivativepath.com/Opportunity/ListOpportunitiesPaged?l=0#"

#Read in the raw source file
filepath = Path(r"C:\Users\ELeibowitz\Downloads\Opportunities.xlsx")
df_raw = pd.read_excel(filepath, header=2)

#eliminate non-deal related entries
filternames = ['Treasury', 'Template', 'Strategy', 'Example', 'Options', 'Prepayable']
pattern = '|'.join(filternames)
filter_dealname = df_raw['Deal Name'].str.contains(pattern,case=False, regex=True, na=False)
df_filtered = df_raw[filter_dealname]
df_clean = df_raw[~filter_dealname].sort_values(by='Deal Owner')


#save Excel file to directory and format it
dt = "2022-12-30"
outpath = Path(r"C:\Users\ELeibowitz\OneDrive - BankUnited, N.A\Long Term Retention\Project Support\t651-Derivative Path\Pipeline Report")
filename = f"Opportunities({dt}).xlsx"

writer = pd.ExcelWriter(Path(outpath,filename))
df_clean.to_excel(writer,"Pipeline", index=False)

#format notional columns with commas
format1 = writer.book.add_format({'num_format': '#,##0'})
writer.sheets['Pipeline'].set_column(6, 7, 11, format1)

#fix issue with Added Date column width not corrected above
writer.sheets['Pipeline'].set_column(21, 22, 18)
writer.sheets['Pipeline'].set_column(2, 2, 50)

#freeze pains on top row
writer.sheets['Pipeline'].freeze_panes(1,0)

# Auto-adjust columns' width
#for column in df_clean:
#    column_width = max(df_clean[column].astype(str).map(len).max(), len(column))
#    col_idx = df_clean.columns.get_loc(column)
#    writer.sheets['Pipeline'].set_column(col_idx, col_idx, column_width)


# Add a header format.
header_format = writer.book.add_format({
    'bold': True,
    'text_wrap': True,
    'valign': 'top',
    'fg_color': '#d7e4bc',
    'border': 1})

# Write the column headers with the defined format.
for col_num, value in enumerate(df_clean.columns.values):
    writer.sheets['Pipeline'].write(0, col_num , value, header_format)

writer.save()
writer.close()


#prepare the email text with attachment
import win32com.client as win32
def outlook_conn():
    # test outlook connection
    try:
        return win32.GetActiveObject('Outlook.Application')
    except:
        return win32.Dispatch('Outlook.Application')

def send_email(recipient, Cc, subject, filepath, body):  # Email Script
    """Send emails using outlook - provide recipients, subject, body as strings"""

    outlook = outlook_conn()
    mail = outlook.CreateItem(0)
    mail.To = recipient
    mail.Cc = Cc
    mail.Subject = subject
    mail.Body = body
    mail.Attachments.Add(filepath)
    mail.Categories='t651-Derivative Path'
    mail.Display(True)
    #mail.Save()


distrolist = Path(r"C:\Users\ELeibowitz\ReposGH\t651-Derivatives\i0001-pipelineDistro.txt")
with open(distrolist) as f:
    recipients = f.read()
Cc = ""
sub = 'Customer Derivatives Pipeline Report'
filepath = Path(outpath,filename)
bod = """All,

Attached is the Customer Derivatives Pipeline report generated by Derivative Path.  The report is distributed on a weekly basis however it is available through their web platform for on-demand access:  https://app.derivativepath.com/Opportunity/ListOpportunitiesPaged?l=0#

Please let me know if the status of any deals needs to be updated or if you have any questions.

Regards,

Elliot Leibowitz
Senior Vice President/Treasury Analytics and Process Engineering Officer
BankUnited, N.A.
14817 Oak Lane
Miami Lakes, FL 33016

Office: 305-231-6496
Remote/Cell: 443-799-5285
"""

send_email(recipients,Cc, sub, str(filepath), bod)

#delete the source file from downloads
target = r"C:\Users\ELeibowitz\Downloads\Opportunities.xlsx"
Path(target).unlink()	

#put all this on Github under t651 with a subprocess
