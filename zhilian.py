# -*- coding:utf-8 -*-
import re
import requests
from lxml import etree
import random
from datetime import datetime, timedelta
from get_ip import CrawlIp
import csv
import logging
import gevent
import time
from gevent import monkey
logger = logging.getLogger()
monkey.patch_all()


class ZhilianCraw(object):
    def __init__(self, search_name, city, key_word, start_num=1):
        self.url = 'http://sou.zhaopin.com/jobs/searchresult.ashx'
        # 保存申请头信息
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
            }
        # params参数设置
        self.params = {'jl': city, 'kw': search_name, 'p': 1}
        # 获取代理
        self.proxies = []
        # 搜索的工作名称
        self.key_word = key_word
        self.workName = search_name
        self.work_datas = []
        self.second_pages = []
        self.search_name = search_name
        print("Z智联初始化成功")

    def crawl_first_page(self):
        proxy = random.choice(self.proxies)
        try:
            # 获取html
            response = requests.get(url=self.url, headers=self.headers, params=self.params, proxies=proxy, timeout=20)  # proxies=proxy,
            html = response.text
            selector = etree.HTML(html)
            number_count = selector.xpath("/html/body/div[3]/div[3]/div[2]/span[1]/em/text()")[0]
            pages_number = int(number_count)//60 + 1
            print(pages_number)
            return pages_number
        except:
            logger.exception("Z智联——获取页面总数出错")
            print("智联——获取页面总数出错")
            return 1

    def craw_data(self, page):
        self.params['p'] = page   # 设定爬取页面
        proxy = random.choice(self.proxies)
        try:
        # 获取html
            response = requests.get(url=self.url, headers=self.headers, params=self.params, proxies=proxy, timeout=20)  # proxies=proxy,
            if response.status_code >= 400:   # 如果获取为空后，或者状态码不正确
                # print('第%d页找不到数据' % page)
                logger.warning("Z智联找不到数据：%s" % response.url)
                self.second_pages.append(page)  # 将未爬取到的页面加入到二次爬取的页面中
                # self.proxies.remove(proxy)   # 将代理删除
                return
            html = response.text
            print('Z智联正在处理%d页内容...' % page)
            logger.debug('Z智联正在处理%d页内容...' % page)
            selector = etree.HTML(html)
            tag_lists = selector.xpath("//table[@class='newlist']")
            if len(tag_lists) > 1:  # 如果能获取到数据
                tag_lists = tag_lists[1:]
            else:
                logger.warning("Z智联第%d页找不到数据：%s" % response.url)
                print('第%d页找不到数据' % page)
                return
            for tag in tag_lists:
                item = {}   # 保存一条数据
                workType = ''.join(tag.xpath(".//tr[1]/td/div/a//text()"))
                if not [word for word in self.key_word if word in workType]:  # 如果一个关键词都没包含，直接过滤
                    continue
                item['workName'] = self.workName   # 工作名称
                item['companyName'] = tag.xpath(".//tr[1]/td[@class='gsmc']/a/text()")[0] if tag.xpath(".//tr[1]/td[@class='gsmc']/a/text()") else 'Null'
                # 发布时间的处理
                createTime = tag.xpath(".//tr[1]/td[@class='gxsj']//text()")[0] if tag.xpath(".//tr[1]/td[@class='gxsj']//text()") else 'Null'
                if createTime == '刚刚':
                    item['createTime'] = str(datetime.now().date())
                if '时前' in createTime:
                    hours_num = int(createTime.split('小时前')[0])
                    item['createTime'] = str(datetime.now().date() + timedelta(hours=-hours_num))
                elif createTime == '昨天':
                    item['createTime'] = str(datetime.now().date() + timedelta(days=-1))  # 表示昨天
                elif createTime == '前天':
                    item['createTime'] = str(datetime.now().date() + timedelta(days=-2))  # 表示昨天
                elif '天前' in createTime:
                    day_num = int(createTime.split('天')[0])
                    item['createTime'] = str(datetime.now().date() + timedelta(days=-day_num))
                elif '-' in createTime:
                    item['createTime'] = '2017-' + createTime
                else:
                    item['createTime'] = createTime
                    # 地区的处理
                city = tag.xpath(".//tr[1]/td[@class='gzdd']/text()")[0] if tag.xpath(".//tr[1]/td[@class='gzdd']/text()") else 'Null'
                if len(city.split("-")) > 1:
                    item['city'] = city.split("-")[0]
                    item['district'] = city.split("-")[1]
                else:
                    item['city'] = city.split("-")[0]
                    item['district'] = "Null"
                # 处理薪水
                salary = tag.xpath(".//tr[1]/td[@class='zwyx']/text()")[0] if tag.xpath(".//tr[1]/td[@class='zwyx']/text()") else 'Null'
                if salary != 'Null':
                    if '面' in salary:    # 如果是面议
                        mean_salary = salary
                    else:   # 如果是包含数字
                        if len(salary.split('-')) > 1:   # 如果是1-2这种类型
                            min = salary.split('-')[0]  # 选取前面
                            max = salary.split('-')[1]
                            mean_salary = (int(min) + int(max)) / 2000
                        else:    # 如果是1 类型
                            mean_salary = int(salary)
                else:
                    mean_salary = 'Null'
                item['salary'] = mean_salary
                # 工作经验
                work_year = tag.xpath(".//tr[2]//li[@class='newlist_deatil_two']/span[4]/text()")[0] if tag.xpath(".//tr[2]//li[@class='newlist_deatil_two']/span[4]/text()") else 'Null'
                item['workYear'] = work_year.split('：')[1] if '年' in work_year.split('：')[1] else '不限'
                item['webName'] = '智联招聘'
                self.work_datas.append(item)
                print(item)
            time.sleep(random.randrange(3, 5))
        # 下载完一页后，加页码数+1
        # 爬完一次需要等待一定的时间爬取下一页
        # time.sleep(random.randrange(3, 7))
        except Exception as e:
            logger.exception("Z智联")
            print(e)

    def save_data(self, filename='zhilian.csv'):
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
            logger.exception("Z智联")
            print("Z智联获取数据出错")

    def start(self):
        print("Z智联start函数执行")
        filename = "zhilian/zhilian_" + self.search_name + '.csv'
        self.proxies = CrawlIp().run(self.url, save_ip=False)
        if len(self.proxies) == 0:
            print("爬取Z智联代理IP失败")
            logger.error("爬取Z智联代理IP失败")
            return
        print(len(self.proxies))
        page_number = self.crawl_first_page()
        # page_number = 1 设置爬取页面数
        gevent.joinall([gevent.spawn(self.craw_data, page) for page in range(1, (page_number+1))])  # (page_number+1)
        left_pages = self.second_pages
        for i in range(2):  # 如果爬取失败的页面，遍历五次
            self.second_pages = []
            gevent.joinall([gevent.spawn(self.craw_data, page) for page in left_pages])
            left_pages = self.second_pages
        self.save_data(filename=filename)


if __name__ == '__main__':
    spider = ZhilianCraw("数据产品经理", "北京", key_word=('数据',), start_num=1)
    spider.start()