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
import json
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
from fake_useragent import UserAgent
def get_html(url):
    try:
        ua = UserAgent(verify_ssl=False)
        #proxies={'https':'222.52.142.246:8080'}
        headers={'User-Agent':ua.random}
        response=requests.get(url,headers=headers,timeout=3)
        time.sleep(random.randint(1,2))
        response.encoding='gbk'
        response.raise_for_status()
        return response.text
    except Exception as e:
        time.sleep(120)
        return ''
def get_category_links(url):
   # url='https://www.111.com.cn/categories/'
    response=get_html(url)
    html = etree.HTML(response)
    link1=html.xpath('//div[@class="mc"]/dl/dd/em/a//@href')
    cate=html.xpath('//div[@class="mc"]/dl/dd/em/a/text()')
    cate_links={}
    for key,v in zip(cate,link1):
        if ('category'not in v)and ('search' not in v):#过滤掉不规范的链接url
            cate_links[key]='https:'+v
    return cate_links
def get_information(one):
    print('正在抓取网页:',one[1][:4],one[4])
    p_info='暂无药品信息'
    p_intro='暂无药品简介'
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
    yaopin_info["category"].append(one[0])
    yaopin_info["name"].append(one[1])
    yaopin_info["price"].append(one[2])
    yaopin_info["comments"].append(one[3])
    yaopin_info["url"].append(one[4])
    yaopin_info["info"].append(p_info)
    yaopin_info["intro"].append(p_intro)
    return None
def get_page_links(cate,url):
    r=get_html(url)
    soup=BeautifulSoup(r,'lxml')
    cate=cate
    coms=[]
    for i in soup.find_all(attrs={'class':'comment'}):
        if i.find('a'):
            com=i.find('a').find('em').string
        else:
            com='0'
        coms.append(com)
    p=[i.get_text().strip() for i in soup.select('.price span')]
    u=["https:"+i.attrs['href'].strip() for i in soup.select('.product_pic ')]
    name=[i.attrs['alt'].strip() for i in soup.select('.product_pic img')]
    datas=[[item[0],item[1],item[2],item[3]]for item in zip(name,p,coms,u)]
    #[get_information([cate]+data)for data in datas]#h获取每一条药品信息
    data_list=[[cate]+data for data in datas]
    pool = Pool(4)
    pool.map(get_information, data_list)
    html = etree.HTML(r)
    try:
        next_page=html.xpath('//div[@class="turnPageBottom"]/a[@class="page_next"]//@href')[0]
       # total=html.xpath('//div[@class="turnPageBottom"]/span[2]/text()')[0]
       # print(total[1:-1])
        next_page='https:'+next_page
        print('下一页')
        return get_page_links(cate,next_page)
    except:
        print('last')
        return None
import json
def cl_info(x):
    try:
        if x!='暂无药品信息'and x!='暂无药品简介':
            d={}
            for k,v in x.items():
                d[k[:-3]]=v
            return d
        else:
            return x
    except:
        return x
def cl_intro(x):
    try:
        if x!='暂无药品信息'and x!='暂无药品简介':
            d={}
            for k,v in x.items():
                d[k[:-1]]=v
            return d
        else:
            return x
    except:
        return x
import json
def dic2key(x,key):
    try:
        d=x.get(key,"null")
   #     print(d)
        return d
    except:
        if x=='暂无药品信息'or x=='暂无药品简介':
    #        print(x)
            return x
        else:
            return "unknown"
def clean_data(data):
    import numpy as np
    data["comments"]=data["comments"].astype(np.int64)
    data['price']=data['price'].apply(lambda x:x[:7].strip() if len(x)>9 else x.strip())
    data["price"]=data["price"].astype(float)
    data["info"]=data['info'].apply(cl_info)
    data["intro"]=data["intro"].apply(cl_intro)
    intro_keys=['商品名称', '品牌', '规格', '重量', '生产厂商', '批准文号', '产品类型', '温馨提示']
    for k in intro_keys:
        data[k]=data["intro"].apply(lambda x:dic2key(x,k))
    info_keys=['通用名称', '汉语拼音', '商品名称', '成份', '性状', '功能主治', '规格', '用法用量', '不良反应', '禁忌', '注意事项', '药物相互作用', '药理毒理', '贮藏', '包装', '有效期', '批准文号', '企业名称', '企业地址']
    for k in info_keys:
        data[k]=data["info"].apply(lambda x:dic2key(x,k))
 #   data.sort_values(by=['category','comments'], ascending=[True,False],inplace=True)
    return data
import pymysql
import pandas as pd
from sqlalchemy import create_engine
def to_mysql(data,tablename):
    engine=create_engine('mysql+pymysql://sn_bigdata:sn_bigdaata@2019@172.60.20.204:4000/snbdx?charset=utf8', encoding='utf-8', echo=True)
    print("mysql connected successful")
    try:
        pd.io.sql.to_sql(data,str(tablename), engine, schema='snbdx', if_exists='replace')
        print("Data saved to mysql successfully")
    except:
        print("Data saved failed")
#主函数
def main():
    """医药网网站"""
   # main_url='https://www.111.com.cn/categories/'
   # category_links=get_category_links(main_url)
    category_links = {'肾病': 'https://www.111.com.cn/categories/965145-j1','气血':'https://www.111.com.cn/categories/953785-j1'}
    print("一共获取%d种药品"%len(category_links))
    for cate, url in category_links.items():
        print('种类：', cate, url)
        get_page_links(cate, url)
    data = pd.DataFrame(yaopin_info)  # 得到原始数据框
    print("正在清洗数据...")
    cln_data=clean_data(data)
    del cln_data["info"]
    del cln_data["intro"]
    print("正在写入mysql...")
    tablename='yiyaowang_info'
    to_mysql(cln_data,tablename)
if __name__ == '__main__':
    yaopin_info = {"category": [], "name": [], "price": [], "comments": [], "url": [], "info": [], "intro": []}
    main()
