import requests
from lxml import etree
import re
from lxml import etree
import codecs
# import sys
# import io
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030')
from multiprocessing.dummy import Pool
import requests
from bs4 import BeautifulSoup
import time
import random
from fake_useragent import UserAgent
def get_html(url):
    try:
        ua = UserAgent()
        #proxies={'https':'222.52.142.246:8080'}
        headers={'User-Agent':ua.random}
        r=requests.get(url,headers=headers,timeout=3)
        time.sleep(random.randint(1,3))
        r.encoding='gbk'
        r.raise_for_status()
        return r.text
    except Exception as e:
        time.sleep(120)
        return ''
def write_to_csv(data,filename):
    if data is None:
        print('数据为空')
        return None
  #  filename = "c:/seven/yiyaolinks.csv"
    with open(filename, "ab+") as fp:
        fp.write(codecs.BOM_UTF8)  # ，第一次打开，这为了防止在Windows下打开CSV文件出现乱码
    with open(filename,"a+",encoding='utf-8', errors='ignore', newline="") as f:
        writer = csv.writer(f)
        writer.writerow(data)
import pandas as pd
data=pd.read_csv('c:/seven/yiyaolinks3.csv',names=['药品种类','名称','价格','评论','详情链接'],encoding='utf-8')
data['名称']=data['名称'].apply(lambda x:x.strip())
cate=data['药品种类']
name=data['名称']
price=data['价格']
comments=data['评论']
url=data['详情链接']
data_list=[(a,b,c,d,e)for a,b,c,d,e in zip(cate,name,price,comments,url)]
import csv
# def cl_data(data):
#     data['info']=data['info'].str.replace("'", '"')
#     data['intro']=data['intro'].str.replace("'", '"').replace("\u3000","")
#     data['intro']=data['intro'].str.replace("\u3000","")
#     data['info']=data['info'].apply(cl_info)
#     data['intro']=data['intro'].apply(cl_intro)
#     data['price']=data['price'].fillna(value=0.0)
#     data['price']=data['price'].str.replace('\r\r\n','')
#     data['price']=data['price'].apply(lambda x:x[:6] if len(x)>7 else x)
#     data['price']=data['price'].astype(float)
#     return data
def get_information(one):
#     cate=noe[0]
#     name=one[1]
#     price=[2]
#     url=one[3]
    print('正在抓取网页:',one[1][:4],one[4])
    p_info='暂无药品信息'
    p_intro='暂无药品简介'
#     if c[i][0].strip()is not None:
#         p_link='http:'+c[i][0].strip()
#     if c[i][1].strip() is not None:
#         price=c[i][1].strip()+'RMB'
#     if c[i][2].strip()is not None:
#         name=c[i][2].strip()
    r=get_html(one[4])
    soup=BeautifulSoup(r,'lxml')
    try:
        p=[i.get_text() for i in soup.select('.specificationBox td')]
        ti=[i.get_text() for i in soup.select('.specificationBox th')]
        info={}
        for k,v in zip(ti[2:],p[:-1]):
            info[k]=v.strip()
        if len(info)!=0:
            p_info=info
        a=[i.get_text() for i in soup.select('.goods_intro td')]
        b=[i.get_text() for i in soup.select('.goods_intro th')]
        intro={}
        for k,v in zip(b,a):
            intro[k]=v.strip()
        if len(intro)!=0:
            p_intro=intro
    except:
        print('get_none!!!')
    one_info=[one[0],one[1],one[2],one[3],one[4],p_info,p_intro]
    write_to_csv(one_info,"c:/seven/yiyao_information00.csv")
    return None
pool=Pool(4)
on_start=time.clock()
pool.map(get_information,data_list[10000:])
print('running time:',time.clock()-on_start)