
# coding: utf-8

# In[1]:


import requests
import json
import pymongo
from bs4 import BeautifulSoup
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# In[ ]:


## mysql


# In[2]:


mysql_client = create_engine('mysql://root:1234@52.78.15.216/terraform?charset=utf8')
mysql_client


# In[3]:


#model
base = declarative_base()

class NaverKeywords(base):
    __tablename__ = 'naver'
    id = Column(Integer, primary_key=True)
    rank = Column(Integer, nullable=False)
    keyword = Column(String(50), nullable=False)
    rdate = Column(TIMESTAMP, nullable=False)
    
    def __init__(self, rank, keyword):
        self.rank = rank
        self.keyword = keyword
    def __repr__(self):
        return '<NaverKeyword {}, {}>'.format(self.rank, self.keyword)
    


# In[4]:


#crawling code
def crawling():
    response = requests.get('https://naver.com')
    dom = BeautifulSoup(response.content, 'html.parser')
    keywords = dom.select('.ah_roll_area > .ah_l > .ah_item')
    datas = []
    for keyword in keywords:
        rank = keyword.select_one('.ah_r').text
        keyword = keyword.select_one('.ah_k').text
        datas.append((rank,keyword))
    return datas


# In[5]:


#test code
# datas = crawling()
# datas[-2:]


# In[6]:


#save mysql
def save_mysql(datas, mysql_client = mysql_client):
    keywords = [NaverKeywords(rank, keyword) for rank, keyword in datas]
    #make_session
    maker = sessionmaker(bind=mysql_client)
    session = maker()
    #save datas
    session.add_all(keywords)
    session.commit()
    session.close()


# In[7]:


#create table
base.metadata.create_all(mysql_client)
datas = crawling()
save_mysql(datas, mysql_client)


# In[ ]:


#mongo db


# In[8]:


mongo_client = pymongo.MongoClient('mongodb://52.78.15.216:27017')
mongo_client


# In[9]:


def save_mongo(datas, mongo_client=mongo_client):
    querys = [{'rank':rank, 'keyword':keyword} for rank, keyword in datas]
    mongo_client.terraform.naver.insert(querys)


# In[10]:


#test code
datas = crawling()
save_mongo(datas, mongo_client)


# In[ ]:


###send_slack


# In[11]:


def send_slack(msg, channel='#python', username='provision_bot'):
    WEBHOOK_URL = 'https://hooks.slack.com/services/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'
    payload = {
    'channel': channel,
    'username': username,
    'icon_emoji': ':troll:',
    'text': msg
}
    response = requests.post(
    WEBHOOK_URL,
    data = json.dumps(payload)
)
    return response


# In[12]:


def run():
    base.metadata.create_all(mysql_client)
    datas = crawling()
    save_mysql(datas)
    save_mongo(datas)
    send_slack('naver crawling has been done.Esther')
    


# In[13]:


run()

