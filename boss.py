# -*- coding:utf-8 -*-
import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
from get_ip import CrawlIp
from multiprocessing.dummy import Pool
import random
import time
import logging
logger = logging.getLogger()


class BossCraw(object):
    def __init__(self, search_name, city, key_word):
        # 申请一个session
        self.ses = requests.Session()
        # 保存申请头信息
        self.headers = {
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                        "Accept-Encoding":"gzip, deflate",
                        "Accept-Language": "zh-CN,zh;q=0.8",
                        "Cache-Control":"no-cache",
                        "Connection": "keep-alive",
                        "Cookie":"__c=1501286108; sid=sem_pz_bdpc_index; __g=sem_pz_bdpc_index; lastCity=101010100; __l=l=%2F%3Fsid%3Dsem_pz_bdpc_index&r=http%3A%2F%2Fbzclk.baidu.com%2Fadrc.php%3Ft%3D06KL00c00fDNJKT0q1wu0KZEgsZu1MVu000000aTYH300000jCFs0M.THdBULP1doZA80K85yF9pywd0ZnqmWcsujTLP1Rsnj0YPyPhn6Kd5H9DnjRdfRwAPRmsnHD4PWckwWuKnb7DPWD4wW-KrjD40ADqI1YhUyPGujYzrHm3rHmYnj6dFMKzUvwGujYkP6K-5y9YIZ0lQzqLILT8Xh9GTA-8QhPEUitOTv-b5gP-UNqsX-qBuZKWgv-8uAN30APzm1YkP1bdnf%26tpl%3Dtpl_10085_15673_1%26l%3D1053924352%26attach%3Dlocation%253D%2526linkName%253D%2525E6%2525A0%252587%2525E9%2525A2%252598%2526linkText%253DBoss%2525E7%25259B%2525B4%2525E8%252581%252598%2525EF%2525BC%25259A%2525E6%25258D%2525A2%2525E5%2525B7%2525A5%2525E4%2525BD%25259C%2525E5%2525B0%2525B1%2525E6%252598%2525AF%2525E6%25258D%2525A2Boss%2526xp%253Did(%252522m28cbf248%252522)%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FH2%25255B1%25255D%25252FA%25255B1%25255D%2526linkType%253D%2526checksum%253D91%26ie%3DUTF-8%26f%3D8%26tn%3Dbaidu%26wd%3DBoss%25E7%259B%25B4%25E8%2581%2598%26oq%3DBoss%25E7%259B%25B4%25E8%2581%2598%26rqlang%3Dcn&g=%2F%3Fsid%3Dsem_pz_bdpc_index; __a=83597956.1499645802.1501221500.1501286108.169.4.11.11; Hm_lvt_194df3105ad7148dcf2b98a91b5e727a=1501222866,1501222877,1501286108,1501327911; Hm_lpvt_194df3105ad7148dcf2b98a91b5e727a=1501328040",
                        "Host": "www.zhipin.com",
                        "Pragma":"no-cache",
                        "Upgrade-Insecure-Requests": "1",
                        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
                        }
        self.ses.headers.update(self.headers)
        try:
            self.city_number = {'北京':101010100, '上海':'101020100', '深圳':'101280600', '广州': '101280100',
                                '杭州': '101210100', '天津': '101030100', '西安': '101110100', '重庆': '101040100',
                                '成都': '101270100'}[city]
        except:
            print('输入城市错误')
            logger.exception("BOSS直聘")
        #配置url
        self.url = 'http://www.zhipin.com/c{0}/h_{0}/'.format(self.city_number)
        # 使用代理
        # self.proxy = proxy
        # params参数设置
        self.params = {'query': search_name}
        self.workName = search_name
        self.key_word = key_word   # 需要输入检测的关键字参数，因为BOSS直聘会出现非搜索类的公众
        self.work_datas = []
        self.second_page = []  # 用于二次遍历的page
        self.search_name = search_name
        print("BOSS初始化成功")

    def craw_data(self, page):
        self.params['page'] = page
        try:
            # print('代理ip:%s' % proxy)
            response = self.ses.get(self.url, params=self.params, headers=self.headers, timeout=15)  #
            html = response.text
            soup = BeautifulSoup(html, 'lxml')
            datas = soup.select('#main > div.job-box > div.job-list > ul > li')
            if not datas or response.status_code >= 400:
                self.second_page.append(page)
                logger.warning("BOSS直聘找不到数据：%s" % response.url)
                return None
            print('BOSS正在处理%d页内容...' % page)
            logger.debug('BOSS正在处理%d页内容...' % page)
            for data in datas:
                item = {}  # 用来保存每一个公司的信息
                job_url = "http://www.zhipin.com/" + data.select_one('a').get('href')
                #print(job_url)
                workType = data.select_one('.info-primary h3').get_text()
                if not [word for word in self.key_word if word in workType]:  # 如果一个关键词都没包含，直接过滤
                    continue
                item['workName'] = self.workName   # 工作名称
                # 获取平均薪水
                salary = data.select_one('.info-primary h3 span').get_text()
                salary_scope = re.findall(r'(\d+)', salary)
                mean_salary = (int(salary_scope[0]) + int(salary_scope[1])) / 2 if len(
                    salary_scope) > 1 else int(salary_scope[0])
                item['salary'] = mean_salary
                # 获取公司名称
                item['companyName'] = data.select_one('.company-text h3').get_text()
                # 获取发布时间
                if data.select_one('.job-time span'):
                    create_time = data.select_one('.job-time span').get_text()
                    create_time = create_time.replace('发布于', '')
                    if create_time == '昨天':
                        create_time = str(datetime.now().date() + timedelta(days=-1))  # 表示昨天
                    elif '月' in create_time:
                        create_time = create_time.replace('月', '-')
                        create_time = create_time.replace('日', '')
                        create_time = '2017-' + create_time
                    elif ':' in create_time:
                        create_time = str(datetime.now().date())
                    item['createTime'] = create_time
                else:
                    item['createTime'] = "Null"
                # 获取地区位置和工作经验
                if data.select_one('.info-primary p'):
                    content = data.select_one('.info-primary p').get_text()
                    item['city'] = content[:2]
                    item['workYear'] = content[2:-2]
                else:
                    item['city'] = "Null"
                    item['workYear'] = "Null"

                item['webName'] = 'BOSS直聘'
                print(item)
                self.work_datas.append(item)
                item['job_url'] = job_url
            time.sleep(random.randrange(3, 5))
        except Exception as e:
            logger.exception("BOSS直聘")
            print(e)

    def save_data(self, filename='Boss.csv'):
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
            print("BOSS直聘获取数据出错")
            logger.warning("BOSS直聘获取数据出错！！！" )

    def start(self):
        filename = "boss/BOSS_" + self.search_name + '.csv'
        # 如果需要用到代理ip还需要构建一个header
        # self.proxies = CrawlIp().run(self.url, save_ip=False)
        for page in range(1, 31):
            self.craw_data(page)
        left_pages = self.second_page
        # 没有爬到数据的网页重复爬取
        for i in range(2):
            self.second_page = []
            for page in left_pages:
                self.craw_data(page)
            left_pages = self.second_page
        self.save_data(filename=filename)

if __name__ == '__main__':
    spider = BossCraw("数据产品经理", "北京", key_word=('数据',))
    spider.start()