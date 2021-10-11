
# coding: utf-8

# In[298]:


import pandas as pd
import numpy as np


# In[299]:


in_file='C:\\Users\\IN22912403\\Downloads\\ddd.xlsx'


# In[300]:


df1=pd.read_excel(in_file,sheet_name='d1')
df2=pd.read_excel(in_file,sheet_name='d2')


# In[301]:


df1.head()
df1['hour']=[i[0] for i in df1['Time'].str.split(':')]


# In[302]:


df1.head()


# In[303]:


df1.shape


# In[304]:


df2.shape


# In[305]:


df3=pd.merge(df1,df2,how='left',left_on="zip_code" ,right_on="zipcode")


# In[306]:


df3['cspeed']=pd.to_numeric(df3['speed'],downcast='float',errors='coerce')


# In[307]:


df3.groupby('State',as_index=False)['cspeed'].mean().sort_values('cspeed').reset_index(drop=True)['State'][0]


# In[308]:


df3.groupby('hour')['hour'].count()


# In[313]:


df3['cspeedx']=df3['cspeed']*100


# In[314]:


df4=df3


# In[322]:


df4.loc[df4['Connection']=='Vi','cspeedx']=100


# In[323]:


df4


# In[ ]:


df4

