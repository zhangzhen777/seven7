# -*- coding: utf-8 -*-
import scrapy
from yiyao.items import Item
import time,random
import re
from bs4 import BeautifulSoup

class YaopinSpider(scrapy.Spider):
    name = 'yaopin'
    allowed_domains = ['www.111.com.cn/categories']
    start_urls = ['http://www.111.com.cn/categories/']
    #start_urls=['https://www.111.com.cn/product/972419.html']

    def parse(self, response):
        link1 = response.xpath('//div[@class="mc"]/dl/dd/em/a//@href')
        cate = response.xpath('//div[@class="mc"]/dl/dd/em/a/text()')
        cate_links = {}
        for key, v in zip(cate, link1):
            if ('category' not in v) and ('search' not in v):
                cate_links[key] = 'https:' + v
        for key in cate_links:
            cate=key
            cate_url = cate_links[key]
            myitem=Item()
            myitem['category']=cate

           # myitem['category_url']=cate_url
            try:
                yield scrapy.Request(url=cate_url, callback=lambda response,category=cate: self.parse_url(response, category))
            except:
                print('为获取网页内容!')

    def parse_url(self, response, category):
       # page_links = response.xpath('//div[@class="itemSearchResultCon"]/a//@href').extract()#商品信息链接列表
        pat='''class="itemSearchResultCon".*?href="(.*?)".*?<p class="price".*?<span>(.*?)</span>.*?<p class="titleBox">.*?</span>(.*?)</a>.*?class="comment comment_right"'''
        p_list=re.compile(pat, re.S).findall(response)#链接，价格,名称,商品信息链接列表
        for i in range(len(p_list)):
            p_link=p_list[0]
            price=''
            name=''
            try:
                if p_list:
                    yield scrapy.Request(url=p_link, callback=lambda response, category=category: self.parse_info(response, category))
            except:
                print('没有获取网页内容!')
        try:
            next_page=response.xpath('//div[@class="turnPageBottom"]/a[@class="page_next"]//@href').extract()[0]#翻页
            if next_page:
                 yield scrapy.Request(url=next_page, callback=self.parse_url)
        except:
            print('最后一页了')

    def parse_info(self,response,category):#获取商品详情信息
        time.sleep(random.randint(0,3))
        soup = BeautifulSoup(response, 'lxml')
        try:
            p_info = [i.get_text() for i in soup.select('.specificationBox td')]
            content = [i.get_text() for i in soup.select('.specificationBox th')]
            info = {}
            for k, v in zip(ti[2:], p_info[:-1]):
                info[k] = v.strip()
            if len(info) != 0:
                p_info = info
            a = [i.get_text() for i in soup.select('.goods_intro td')]
            b = [i.get_text() for i in soup.select('.goods_intro th')]
            intro = {}
            for k, v in zip(b, a):
                intro[k] = v.strip()
            if len(intro) != 0:
                p_intro = intro
        except:
            print('get_none!!!')





