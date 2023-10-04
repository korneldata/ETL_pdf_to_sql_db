#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os
import PyPDF2
from PyPDF2 import PdfReader
import camelot
import pandas as pd
import string
import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine


# In[ ]:


#EXTRACTNG AND TRANSFORMING


# In[8]:


folder_path = r'C:\Users\Nowy_użytkownik\Desktop\bank_statements'

pdf_files = []

#looping through pdf files in folder
for file in os.listdir(folder_path):
    if file.endswith('.pdf'):
        pdf_files.append(file)


# In[10]:


#creating a list to catch all receivers from bank statements
receivers = ['receiver']

for pdf in pdf_files:
    file_dir = os.path.join(folder_path, pdf)
    #reading tables from pdf files
    tbls = camelot.read_pdf(file_dir)
    table_df = tbls[2].df
    #removing unnecessary columns
    table_df = table_df.drop(columns=[1,4])
    #formatting and removing text from numeric column
    table_df[3] = table_df[3].str.rstrip('PLN')   
    table_df[3] = table_df[3].str.replace(',', '.')
    #splitting cells with multiple lines
    col = table_df[2].str.split("\n").to_list()
    
    #getting receivers from the list
    for r in col:
        try:
            receivers.append(r[1])
        except:
            pass
    
    #replacing column with receivers data
    table_df[2] = receivers  
    #promoting first row as headers
    table_df.columns = table_df.iloc[0]
    #using capwords method to capitalize each word in 'receiver' column
    table_df["receiver"] = table_df["receiver"].apply(lambda x: string.capwords(x))
    #removing unnecessary first row
    table_df = table_df[1:]
    #renaming columns in data frame
    table_df = table_df.rename(columns={"Data operacji": "date", "Kwota": "amount"})


# In[ ]:


#LOADING


# In[3]:


#connecting to sql server database
SERVER = r'Kornel\MSSQLSERVERDEV'
DATABASE = 'training'
USERNAME = r'Kornel\Nowy_użytkownik'

connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};Trusted_Connection=yes'
connection = pyodbc.connect(connection_string) 
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url)


# In[23]:


#loading data frame to sql server database
try:
    table_df.to_sql('statements', con=engine, if_exists='replace', index=False)
except Exception as e:
    print('Data load error:' + str(e))

