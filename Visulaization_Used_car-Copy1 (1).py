#!/usr/bin/env python
# coding: utf-8

# In[1]:


from neo4j import __version__ as neo4j_version
print(neo4j_version)


# In[2]:


import py2neo
import pandas as pd
from pandas import DataFrame
from py2neo import Graph


# In[3]:


import matplotlib.pyplot as plt
import seaborn as sns


# In[4]:


from neo4j import __version__ as neo4j_version
print(neo4j_version)


# In[5]:


from neo4j import GraphDatabase


# In[6]:


from neo4j import GraphDatabase
class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response


# In[7]:


connect = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="group4")
print(connect)


# In[11]:


###Data Visulizations###


# In[8]:


query_string = 'MATCH (Oldcars)-[:Condition]->(is_new) RETURN COUNT(Oldcars.oldcarid), is_new.is_new'
data = DataFrame([dict(_) for _ in connect.query(query_string, db='Neo4j')])
#data = connect.query(query_string, db='neo4j')


# In[9]:


data.columns = ['Count of cars', 'Condition']
data


# In[11]:


plt.figure(figsize=(8, 8))
colors= ['#FF0000','#0000FF']
explode= (0.05,0.05)
plt.pie(data['Count of cars'], colors=colors, labels=data['Condition'], autopct='%1.1f%%', pctdistance=0.95, explode=explode)
#centre_circle= plt.Circle((0,0),0.50, fc='white')
fig= plt.gcf()
#fig.gca().add_artist(centre_circle)
plt.title('Cars according to their condition')
plt.show()


# In[67]:


query_rating = 'MATCH (Oldcars)-[:Rated_As]->(seller_rating) where seller_rating.seller_rating = "5" RETURN COUNT(Oldcars.oldcarid), seller_rating.seller_rating limit 50'
data45 = DataFrame([dict(_) for _ in connect.query(query_rating, db='Neo4j')])
data45 = data45.rename(columns={'Oldcars.oldcarid':"oldcarid", 'seller_rating.seller_rating':"seller_rating"})
#data = connect.query(query_string, db='neo4j')
ig, ax =plt.subplots(1,1)
ax.axis('tight')
ax.axis('off')
ax.table(cellText=data45.values,colLabels=data45.columns,loc="center",colColours =["red"] * 2)


# In[40]:


data45


# In[63]:


query_rating1 = 'MATCH (Oldcars)-[:Rated_As]->(seller_rating) where seller_rating.seller_rating < "2" RETURN COUNT(Oldcars.oldcarid), seller_rating.seller_rating limit 50'
data46 = DataFrame([dict(_) for _ in connect.query(query_rating1, db='Neo4j')])
data46.columns = ['Count of cars', 'seller rating']
#data = connect.query(query_string, db='neo4j')


# In[64]:


#plpotting table for the cound of oldcar  and their selelr rating
ig, ax =plt.subplots(1,1)
ax.axis('tight')
ax.axis('off')
ax.table(cellText=data46.values,colLabels=data46.columns,loc="center",colColours =["red"] * 2)


# In[64]:


query_fuel = 'MATCH (Oldcars)-[:fuel_categories_are]->(fuel_type) RETURN COUNT(Oldcars.oldcarid), fuel_type.fuel_type'
data1 = DataFrame([dict(_) for _ in connect.query(query_fuel, db='Neo4j')])
#data = connect.query(query_string, db='neo4j')
data1.columns = ['Count of oldcars', 'fuel_type']
data1


# In[71]:


plt.figure(figsize=(8, 8))
colors= ['#FF0000','#0000FF','#FFFF00','#ADFF2F','#FFA500', '#00FF00', '#00B832']
explode= (0.05,0.05,0.05,0.05,0.05,0.05,0.05)
plt.pie(data1['Count of oldcars'], colors=colors, labels=data1['fuel_type'], autopct='%1.1f%%', pctdistance=0.95, explode=explode)
#centre_circle= plt.Circle((0,0),0.80, fc='white')
fig= plt.gcf()
#fig.gca().add_artist(centre_circle)
plt.title('Cars available according to fuel_type')
plt.show()


# In[13]:


query_year = '''MATCH (Oldcars)-[:made_on]->(year)
RETURN Oldcars.oldcarid, year.year LIMIT 100'''


data2 = DataFrame([dict(_) for _ in connect.query(query_year, db='Neo4j')])
data2 = data2.rename(columns={'Oldcars.oldcarid':"Oldcars", 'year.year':"year"})

data2 = data2.groupby(["year"]).count()
data2.sort_values(by=['Oldcars']).plot(kind='bar', title="Cars available  according to year of manufacture", ylabel='No. of Oldcars',
         xlabel='year', figsize=(10, 4))


# In[76]:


from pandas import DataFrame
query = '''MATCH (Oldcars)-[:car_brand_is]->(make_name)
RETURN Oldcars.oldcarid, make_name.make_name LIMIT 100'''


dtf_data = DataFrame([dict(_) for _ in connect.query(query, db='Neo4j')])
dtf_data = dtf_data.rename(columns={'Oldcars.oldcarid':"Oldcars", 'make_name.make_name':"make_name"})

dtf_dt = dtf_data.groupby(["make_name"]).count()
dtf_dt.sort_values(by=['Oldcars']).plot(kind='bar', title="Cars available  according to Brand", ylabel='No. of Oldcars',
         xlabel='brands', figsize=(10, 4))


# In[96]:


query = '''MATCH (Oldcars)-[:available_from]->(daysonmarket) 
RETURN Oldcars.oldcarid,daysonmarket.daysonmarket LIMIT 50  '''


dtf_data = DataFrame([dict(_) for _ in connect.query(query, db='Neo4j')])
dtf_data = dtf_data.rename(columns={'Oldcars.oldcarid':"Oldcars", 'daysonmarket.daysonmarket':"daysonmarket"})
dtf_dt = dtf_data.groupby(["daysonmarket"]).count()

dtf_dt1 = dtf_dt[1:]
dtf_dt1
dtf_dt.sort_values(by=['Oldcars']).plot(kind='bar', title="Cars available  according to daysonmarket", ylabel='No. of Oldcars',
         xlabel='days', figsize=(10, 5))


# In[133]:


from pandas import DataFrame
query12 = '''MATCH (Oldcars)-[:city_avaiable]->(city)
RETURN Oldcars.oldcarid, city.city LIMIT 50'''


dtf_data12 = DataFrame([dict(_) for _ in connect.query(query12, db='Neo4j')])
dtf_data12 = dtf_data12.rename(columns={'Oldcars.oldcarid':"Oldcars", 'city.city':"city"})

dtf_dt12 = dtf_data12.groupby(["city"]).count()
dtf_dt12.sort_values(by=['Oldcars']).plot(kind='bar', title="Cars available  according to city", ylabel='No. of Oldcars',
         xlabel='city', figsize=(10, 4) ,color = 'red')


# In[107]:


#Number of clients based on the top 10 organizations
querymaxseat = 'MATCH (Oldcars)-[:seat]->(maximum_seating) RETURN maximum_seating.maximum_seating, COUNT(Oldcars.oldcarid)'

datamaxseat = DataFrame([dict(_) for _ in connect.query(querymaxseat, db='Neo4j')])
datamaxseat = datamaxseat[1:]
datamaxseat.columns = ['Maximum seats in cars', 'Number of Oldcars of that seating']
datamaxseat


# In[110]:


datamaxseat.sort_values(by=['Maximum seats in cars']).plot(kind='bar', title="Cars available  according to seats", ylabel='No. of Oldcars',
         xlabel='Maximum seats in cars', figsize=(10, 5))


# In[59]:


query12 ='''MATCH (Oldcars)-[:Sold_at]->(price) 
RETURN Oldcars.oldcarid,price.price  limit 10  '''

#'MATCH (Oldcars)<-[:Sold_at]-(price) RETURN  Oldcars.oldcarid,price.price)'
data12 = DataFrame([dict(_) for _ in connect.query(query12, db='Neo4j')])
data12.columns = ['Oldcarid', 'Price of cars']


# In[62]:


#plpotting table for the oldcarid and their price
ig, ax =plt.subplots(1,1)
ax.axis('tight')
ax.axis('off')
ax.table(cellText=data12.values,colLabels=data12.columns,loc="center",colColours =["yellow"] * 2)


# In[ ]:


atamaxseat.sort_values(by=['Maximum seats in cars']).plot(kind='bar', title="Cars available  according to seats", ylabel='No. of Oldcars',
         xlabel='Maximum seats in cars', figsize=(10, 5))


# In[136]:


#Number of clients based on the top 10 organizations
querywheel = 'MATCH (Oldcars)-[:contains_wheel]->(wheel_base) RETURN wheel_base.wheel_base, COUNT(Oldcars.oldcarid)'
data18 = DataFrame([dict(_) for _ in connect.query(querywheel, db='Neo4j')])
#data5.columns= ['OrganizationType', 'Number of clients working']
data18 = data18[1:]
data18.columns = ['Wheel_base', 'Count of cars']
data18


# In[142]:


plt.figure(figsize=(8, 8))
colors= ['#FF0000','#0000FF','#FFFF00','#ADFF2F']
explode= (0.05,0.05,0.05,0.05)
plt.pie(data18['Count of cars'], colors=colors, labels=data18['Wheel_base'], autopct='%1.1f%%', pctdistance=0.95, explode=explode)
#centre_circle= plt.Circle((0,0),0.50, fc='white')
fig= plt.gcf()
#fig.gca().add_artist(centre_circle)
plt.title('Car according to wheelbase')
plt.show()


# In[68]:


import pandas as pd
df2 = pd.read_csv(r'S:\Groupproj_bigdata\used_car_clean_CSV.csv')
df2


# In[145]:


# MATCH (n:Person:Director)
# RETURN count(n) as count

q = '''CALL apoc.meta.stats() YIELD labels
RETURN labels;'''
sit_data = DataFrame([dict(_) for _ in connect.query(q, db='neo4j')])
x = sit_data['labels'].values
d = dict(x[0])


# In[146]:


df2.nunique()


# In[147]:


df2.columns


# In[148]:


query = '''CALL apoc.meta.stats() YIELD labels RETURN labels;'''
data = DataFrame([dict(_) for _ in connect.query(query, db='neo4j')])
data_dict = dict(data['labels'].values[0])


# In[152]:


for k,v in data_dict.items():
    try:
        if (len(pd.unique(df2[k])) == v):
            print(f"Distinct count test case passed for Node: {k}.\nDistinct count value: {v}")
        else:
            print(f"Distinct count test case failed for Node: {k}.\nDistinct count value of Node: {v} and Distinct count value of column: {len(pd.unique(df2[k]))}")
    except:
        
        continue


# In[153]:


len(pd.unique(df2["height"]))


# In[71]:


#Verification of datatypes


# In[70]:


query_string = "CALL apoc.meta.data();"
dtf_data = DataFrame([dict(_) for _ in connect.query(query_string, db='Neo4j')])
dtf_data = dtf_data[dtf_data.type!= "RELATIONSHIP"]
dtf_data = dtf_data.loc[:,['property','type']].reset_index()
dtf_data = dtf_data.drop(columns=['index'])
dtf_data.values


# In[73]:


print(dtf_data.isna().sum())


# In[ ]:




