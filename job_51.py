# -*- coding:utf-8 -*-
import re
import requests
from lxml import etree
import random
from datetime import datetime, timedelta
from get_ip import CrawlIp
import csv
import gevent
from gevent import monkey
import logging
import time
logger = logging.getLogger()
monkey.patch_all()


class JobCraw(object):
    def __init__(self, search_name, city, key_word, start_num=1):
        self.url = 'http://search.51job.com/jobsearch/search_result.php'
        # 保存申请头信息
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
            }
        # 城市id
        try:
            self.city_number = {'北京': '010000', '上海': '020000', '深圳': '040000', '广州': '030200',
                                '杭州': '080200', '天津': '050000', '西安': '200200', '重庆': '060000',
                                '成都': '090200'}[city]
        except:
            logger.exception("51job输入城市错误")
            print('51job输入城市错误')
        # params参数设置
        self.params = {'jobarea': self.city_number, 'keyword': search_name, 'curr_page': 1}
        # 获取代理
        self.proxies = []
        # 搜索的工作名称
        self.key_word = key_word
        self.workName = search_name
        self.work_datas = []
        self.second_pages = []
        self.search_name = search_name
        print("51job初始化成功")

    def crawl_firest_page(self):
        '''爬取总页面数据'''
        proxy = random.choice(self.proxies)
        try:
            response = requests.get(self.url, params=self.params, headers=self.headers, proxies=proxy, timeout=15)
            # print(len(response.text))
            response.encoding = 'gb2312'
            html = response.content.decode('gb2312')
            #print(html)
            selector = etree.HTML(html)
            page_text = selector.xpath("//span[@class='td'][1]/text()")[0]
            number = re.search(r'\d+', page_text).group()
            return int(number)
        except:
            logger.error("51job爬取总页面数据错误")
            return 1

    def craw_data(self, page):
        self.params['curr_page'] = page   # 设定爬取页面
        proxy = random.choice(self.proxies)
        try:
            # 获取html
            response = requests.get(url=self.url, headers=self.headers, params=self.params, proxies=proxy, timeout=20)  # proxies=proxy,
            if response.status_code >= 400:   # 如果获取为空后，或者状态码不正确
                # print('第%d页找不到数据' % page)
                self.second_pages.append(page)  # 将未爬取到的页面加入到二次爬取的页面中
                logger.warning("51job找不到数据：%s" % response.url)
                return None
            print('51job正在处理%d页内容...' % page)
            logger.debug('51job正在处理%d页内容...' % page)
            # 处理解码问题
            response.encoding = 'gb2312'
            html = response.content.decode('gb2312')
            print(response.url)
            selector = etree.HTML(html)
            tag_lists = selector.xpath("//*[@id='resultList']/div[@class='el']")
            for tag in tag_lists:
                item = {}   # 保存一条数据
                workType = tag.xpath("./p/span/a/@title")[0]
                if not [word for word in self.key_word if word in workType]:  # 如果一个关键词都没包含，直接过滤
                    continue
                item['workName'] = self.workName   # 工作名称
                item['companyName'] = tag.xpath("./span[1]/a/@title")[0] if tag.xpath("./span[1]/a/@title") else 'Null'
                # 创建时间处理
                createTime = tag.xpath("./span[@class='t5']/text()")[0] if tag.xpath("./span[@class='t5']/text()") else 'Null'
                if createTime == 'Null':
                    item['createTime'] = createTime
                elif '-' in createTime:   # 7-24类型
                    item['createTime'] = '2017-' + createTime
                else:  # 3个月前，统一按一个月前处理
                    item['createTime'] = item['createTime'] = str(datetime.now().date() + timedelta(days=-30))
                # 城市处理
                city = tag.xpath("./span[@class='t3']/text()")[0] if tag.xpath("./span[@class='t3']/text()") else 'Null'
                if len(city.split("-")) > 1:
                    item['city'] = city.split("-")[0]
                    item['district'] = city.split("-")[1]
                else:
                    item['city'] = city.split("-")[0]
                    item['district'] = "Null"
                # 处理薪水
                salary = tag.xpath("./span[@class='t4']/text()")[0] if tag.xpath("./span[@class='t4']/text()") else 'Null'
                if '-' in salary:
                    if '千' in salary:    # 如果是按月来计算
                        min = salary.split('-')[0]   # 选取前面
                        max = salary.split('-')[1].split("千")[0]
                        mean_salary = (float(min) + float(max)) / 2
                    elif '万' in salary:   # 如果按WAN
                        min = salary.split('-')[0]  # 选取前面
                        max = salary.split('-')[1].split("万")[0]
                        mean_salary = (float(min) + float(max)) * 5
                    else:
                        mean_salary = 'Null'
                else:
                    mean_salary = 'Null'
                item['salary'] = mean_salary
                item['webName'] = '51job'
                self.work_datas.append(item)
                print(item)
            time.sleep(random.randrange(3, 5))
            # 下载完一页后，加页码数+1
            # 爬完一次需要等待一定的时间爬取下一页
            # time.sleep(random.randrange(3, 7))
        except Exception as e:
            logger.exception("51job")
            print(e)

    def save_data(self, filename='51job.csv'):
        all_data = self.work_datas
        # 获取字段名
        if all_data:
            sheet = all_data[0].keys()
            data = [content.values() for content in all_data]
            with open(filename, 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(sheet)
                csv_writer.writerows(data)
        else:
            logger.error("51job获取数据出错")
            print("51job获取数据出错")

    def start(self):
        print("51job start函数执行")
        filename = "51job/51job_" + self.search_name + '.csv'
        self.proxies = CrawlIp().run(self.url, save_ip=False)
        if len(self.proxies) == 0:
            print("爬取51job代理IP失败")
            logger.error("爬取51job代理IP失败")
            return
        print(len(self.proxies))
        page_number = self.crawl_firest_page()
        print(page_number)
        gevent.joinall([gevent.spawn(self.craw_data, page) for page in range(1, (page_number+1))])  # (page_number+1)
        left_pages = self.second_pages
        for i in range(2):  # 如果爬取失败的页面，遍历五次
            self.second_pages = []
            gevent.joinall([gevent.spawn(self.craw_data, page) for page in left_pages])
            left_pages = self.second_pages
        self.save_data(filename=filename)

if __name__ == '__main__':
    spider = JobCraw("数据产品经理", "北京", key_word=('数据',), start_num=1)
    spider.start()