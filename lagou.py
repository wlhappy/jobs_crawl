# -*- coding:utf-8 -*-
import re
import requests
import random
import jsonpath
from get_ip import CrawlIp
import csv
import gevent
from gevent import monkey
import logging
import time
logger = logging.getLogger()
monkey.patch_all()


class LagouCraw(object):
    def __init__(self, search_name, city, start_num=1):
        self.url = 'https://www.lagou.com/jobs/positionAjax.json'
        # 保存申请头信息
        self.headers = {
                        "Accept":"application/json, text/javascript, */*; q=0.01",
                        "Accept-Encoding":"gzip, deflate, br",
                        "Accept-Language":"zh-CN,zh;q=0.8",
                        "Cache-Control":"no-cache",
                        "Connection":"keep-alive",
                        "Content-Length":"43",
                        "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
                        "Cookie":"user_trace_token=20170518102421-f7fb3747d19f447ab9e1af1db403d778;LGUID=20170518102421-1a4dd136-3b71-11e7-a97a-525400f775ce;JSESSIONID=ABAAABAAAIAACBIB3046D14C27FD90FE908D1D4310BA170; _putrc=6F7A5BD30EF8BDD4; login=true; unick=%E7%8E%8B%E7%A3%8A; showExpriedIndex=1; showExpriedCompanyHome=1; showExpriedMyPublish=1; hasDeliver=0; TG-TRACK-CODE=index_search; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1501253975,1501330415,1501392755,1501408610; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1501489428; _gid=GA1.2.1166267813.1501169932; _ga=GA1.2.2063142680.1499252931; LGRID=20170731162348-93c1fde9-75c9-11e7-bde3-5254005c3644; SEARCH_ID=96d039aa65394a7b96717ac3280171b2; index_location_city=%E5%8C%97%E4%BA%AC",
                                    "Host":"www.lagou.com",
                        "Origin":"https://www.lagou.com",
                        "Pragma":"no-cache",
                        "Referer":"https://www.lagou.com/jobs/list_Python%E7%88%AC%E8%99%AB?labelWords=&fromSearch=true&suginput=",
                        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
                        "X-Anit-Forge-Code":"0",
                        "X-Anit-Forge-Token":"None",
                        "X-Requested-With":"XMLHttpRequest"
                        }
        # params参数设置
        self.params = {'city': city, 'needAddtionalResult': 'false'}
        # data参数设置
        self.data = {'first': 'false', 'pn': start_num, 'kd': search_name}
        # 获取代理
        self.proxies = []
        # 搜索的工作名称
        self.workName = search_name
        self.work_datas = []
        self.second_pages = []
        self.search_name = search_name
        print("拉勾初始化成功")

    def craw_data(self, page):
        self.data['pn'] = page   # 设定爬取页面
        proxy = random.choice(self.proxies)
        try:
            # 获取html
            #print('正在获取第%d页数据' % self.data['pn'])
            print(proxy)
            response = requests.post(url=self.url, headers=self.headers, params=self.params, data=self.data, proxies=proxy, timeout=20)  # ,
            datas = jsonpath.jsonpath(response.json(), '$..result')[0]
            print(datas)
            if (not datas) or response.status_code >= 400:   # 如果获取为空后，或者状态码不正确
                # print('第%d页找不到数据' % page)
                self.second_pages.append(page)  # 将未爬取到的页面加入到二次爬取的页面中
                # self.proxies.remove(proxy)   # 将代理删除
                logger.warning("拉勾找不到数据：%s" % response.url)
                return None
            print('拉勾正在处理%d页内容...' % page)
            logger.debug('拉勾正在处理%d页内容...' % page)
            for data in datas:
                item = {}   # 保存一条数据
                item['workName'] = self.workName
                item['companyName'] = data['companyFullName'] if data['companyFullName'] else 'Null'
                createTime = data['createTime'] if data['createTime'] else 'Null'
                item['createTime'] = createTime.split(' ')[0]
                item['workYear'] = data['workYear'] if data['workYear'] else 'Null'
                item['city'] = data['city'] if data['city'] else 'Null'
                # 处理薪水
                salary = data['salary'] if data['salary'] else 'Null'
                salary_scope = re.findall(r'(\d+)', salary)
                mean_salary = (int(salary_scope[0]) + int(salary_scope[1])) / 2 if len(
                    salary_scope) > 1 else int(salary_scope[0])
                item['salary'] = mean_salary
                item['district'] = data['district'] if data['district'] else 'Null'
                item['webName'] = '拉勾网'
                item['job_url'] = 'https://www.lagou.com/jobs/{}.html'.format(data['positionId'])
                self.work_datas.append(item)
                print(item)
            time.sleep(random.randrange(3, 5))
            # 下载完一页后，加页码数+1
            # 爬完一次需要等待一定的时间爬取下一页
            # time.sleep(random.randrange(3, 7))
        except Exception as e:
            logger.exception("拉勾")
            print(e)

    def save_data(self, filename='lagou.csv'):
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
            logger.exception("拉勾获取数据出错")
            print("拉勾获取数据出错")

    def start(self, file_name=None):
        print("拉勾start函数执行")
        filename = "lagou/lagou_" + self.search_name + '.csv'
        self.proxies = CrawlIp().run(self.url, save_ip=False)
        print(len(self.proxies))
        if len(self.proxies) == 0:
            print("爬取拉勾代理IP失败")
            logger.error("爬取拉勾代理IP失败")
            return
        gevent.joinall([gevent.spawn(self.craw_data, page) for page in range(1, 200)])
        left_pages = self.second_pages
        for i in range(3):  # 如果爬取失败的页面，遍历五次
            self.second_pages = []
            gevent.joinall([gevent.spawn(self.craw_data, page) for page in left_pages])
            left_pages = self.second_pages
        self.save_data(filename=filename)

if __name__ == '__main__':
    spider = LagouCraw("数据抓取", "北京", start_num=1)
    spider.start()