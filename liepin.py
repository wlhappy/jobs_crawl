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


class LiepinCraw(object):
    def __init__(self, search_name, city, key_word, end_num=100):
        self.city = {'北京': 'bj', '上海': 'sh', '广州': 'gz', '深圳': 'sz', '杭州': 'hz', '全国': ''}[city]
        self.url = 'https://www.liepin.com/' + self.city + '/zhaopin/'
        # 保存申请头信息
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
            }
        # params参数设置
        self.params = {'key': search_name, 'curPage': 0}
        # 获取代理
        self.proxies = []
        # 搜索的工作名称
        self.key_word = key_word
        self.workName = search_name
        self.end_number = end_num
        self.work_datas = []
        self.second_pages = []
        self.search_name = search_name
        print("猎聘初始化成功")

    def craw_data(self, page):
        self.params['curPage'] = page   # 设定爬取页面
        proxy = random.choice(self.proxies)
        try:
        # 获取html
            response = requests.get(url=self.url, headers=self.headers, params=self.params, proxies=proxy, timeout=20)  # proxies=proxy,
            if response.status_code >= 400:   # 如果获取为空后，或者状态码不正确
                # print('第%d页找不到数据' % page)
                self.second_pages.append(page)  # 将未爬取到的页面加入到二次爬取的页面中
                logger.warning("L猎聘找不到数据：%s" % response.url)
                # self.proxies.remove(proxy)   # 将代理删除
                return
            html = response.text
            print('L猎聘正在处理%d页内容...%s' % (page,response.url))
            logger.debug('L猎聘正在处理%d页内容...' % page)
            selector = etree.HTML(html)
            if selector.xpath("//div[@class='result-none']/p/text()"):  # 如果能够找到这句话，说明没搜到
                print("猎聘爬取到%d，已经找不到数据了" % page )
                return
            tag_lists = selector.xpath("//ul[@class='sojob-list']/li")
            for tag in tag_lists:
                item = {}   # 保存一条数据
                workType = tag.xpath(".//span[@class='job-name']/a/span/text()")[0]
                if not [word for word in self.key_word if word in workType]:  # 如果一个关键词都没包含，直接过滤
                    continue
                item['workName'] = self.workName   # 工作名称
                item['companyName'] = tag.xpath(".//p[@class='company-name']/a/text()")[0].strip() if tag.xpath(".//p[@class='company-name']/a/text()") else 'Null'
                # 发布时间的处理
                createTime = tag.xpath(".//p[@class='time-info clearfix']/time/text()")[0] if tag.xpath(".//p[@class='time-info clearfix']/time/text()") else 'Null'
                if '小时' in createTime:
                    hours = int(createTime.split('小时')[0])
                    item['createTime'] = str((datetime.now() - timedelta(hours=hours)).date())
                elif '昨天' in createTime:
                    item['createTime'] = str(datetime.now().date() + timedelta(days=-1))  # 表示昨天
                elif '前天' in createTime:
                    item['createTime'] = str(datetime.now().date() + timedelta(days=-2))  # 表示昨天
                elif '月' in createTime:
                    item['createTime'] = str(datetime.now().date() + timedelta(days=-30))  # 同一处理为一个月前
                else:
                    item['createTime'] = createTime
                # 地区的处理
                city = tag.xpath(".//span[@class='area']/text()")[0] if tag.xpath(".//span[@class='area']/text()") else 'Null'
                if '-' in city:    # 如果是(北京-海淀区)类型
                    item['city'] = city.split("-")[0]
                    item['district'] = city.split("-")[1]
                elif ',' in city:   # 如果是(北京,上海)类型
                    item['city'] = city.split(",")[0]
                    item['district'] = "Null"
                else:
                    item['city'] = city
                    item['district'] = "Null"
                # 处理薪水
                salary = tag.xpath(".//span[@class='text-warning']/text()")[0] if tag.xpath(".//span[@class='text-warning']/text()") else 'Null'
                if salary != 'Null':
                    if '面' in salary:    # 如果是面议
                        mean_salary = salary
                    else:   # 如果是包含数字
                        min = int(salary.split('万')[0].split('-')[0])
                        max = int(salary.split('万')[0].split('-')[1])
                        mean_salary = (min+max)*5/12
                else:
                    mean_salary = 'Null'
                item['salary'] = mean_salary
                # 工作经验
                work_year = tag.xpath(".//p[@class='condition clearfix']/span/text()")[-1] if (".//p[@class='condition clearfix']/span/text()") else '不限'
                if '年' in work_year:
                    item['workYear'] = work_year.split('工作')[0]
                item['webName'] = '猎聘'
                self.work_datas.append(item)
                print(item)
            time.sleep(random.randrange(3, 5))
        # 下载完一页后，加页码数+1
        # 爬完一次需要等待一定的时间爬取下一页
        # time.sleep(random.randrange(3, 7))
        except Exception as e:
            logger.exception("L猎聘")
            exit()

    def save_data(self, filename='liepin.csv'):
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
            logger.exception("L猎聘")
            print("L猎聘获取数据出错")

    def start(self):
        print("L猎聘start函数执行")
        filename = "liepin/liepin_" + self.search_name + '.csv'
        self.proxies = CrawlIp().run(self.url, save_ip=False)
        if len(self.proxies) == 0:
            print("爬取L猎聘代理IP失败")
            logger.error("爬取L猎聘代理IP失败")
            return
        print(len(self.proxies))
        # self.end_number = 1 设置最多爬取的页面数
        gevent.joinall([gevent.spawn(self.craw_data, page) for page in range(0, self.end_number)])  # (page_number+1)
        left_pages = self.second_pages
        for i in range(2):  # 如果爬取失败的页面，遍历五次
            self.second_pages = []
            gevent.joinall([gevent.spawn(self.craw_data, page) for page in left_pages])
            left_pages = self.second_pages
        self.save_data(filename=filename)


if __name__ == '__main__':
    spider = LiepinCraw("数据产品经理", "北京", key_word=('数据', ), end_num=100)
    spider.start()