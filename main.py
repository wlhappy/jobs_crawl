# -*- coding:utf-8 -*-
import job_51
import lagou
import boss
import liepin
import zhilian
import gevent
from gevent import monkey
import logging
import logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger()
monkey.patch_all()


def main():
    # bs = boss.BossCraw()
    # 初始化各个对象
    bs = boss.BossCraw("android", "北京", key_word=('Android', '安卓', 'android'))
    lg = lagou.LagouCraw("android", "北京", start_num=1)
    j5 = job_51.JobCraw("android", "北京", start_num=1, key_word=('android', '安卓', 'Android'))
    lp = liepin.LiepinCraw("android", "北京", key_word=('android', '安卓', 'Android'), end_num=100)
    zl = zhilian.ZhilianCraw("android", "北京", start_num=1, key_word=('android', '安卓', 'Android'))
    # 创建进程池
    gevent.joinall([gevent.spawn(bs.start),
                    gevent.spawn(lg.start),
                    gevent.spawn(j5.start),
                    gevent.spawn(lp.start),
                    gevent.spawn(zl.start)])
    # po = Pool(2)
    # po.apply_async(bs.start)
    # po.apply_async(lg.start)
    # po.apply_async(j5.start)
    # po.apply_async(lp.start)
    # po.apply_async(zl.start)
    # 等待进程池结束
    print("---start----")
    print("---end-----")
if __name__ == '__main__':
    main()
