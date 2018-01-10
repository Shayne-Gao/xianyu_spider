#!/usr/bin/env python
# encoding: utf-8
# 访问 http://tool.lu/pyc/ 查看更多信息
import json
import urlparse
import HTMLParser
import ConfigParser
import os
import urllib
import time
import MySQLdb
import sys
##获取每个爬虫关键字每日统计的数量
class LifeDB:
    db = None
    cursor = None
    dictCursor = None
    REC_TYPE_COST = 0
    REC_TYPE_INCOME = 1
    REC_VALID_TRUE = 1
    REC_VALID_FALSE = 0
    
    def __init__(self):
        self.db = MySQLdb.connect('localhost', 'root', 'IWLX8IS12Rl', 'life', charset = 'utf8')
        self.cursor = self.db.cursor()
        self.dictCursor = self.db.cursor(MySQLdb.cursors.DictCursor)
    
    def queryBySql(self, sql,cursorType='default'):
        try:
            if cursorType =='dict':
                thisCursor = self.dictCursor
            else:
                thisCursor = self.cursor
            thisCursor.execute(sql)
            self.db.commit()
            result = thisCursor.fetchall()
            return result
        except Exception as e:
            print 'MYSQL ERROR:', str(e)
            print sql
            self.db.rollback()

    def getSMZDMstat(self,limitdays):
        sql = """SELECT matched_rule,DATE_FORMAT(create_time,'%%Y-%%m-%%d') AS days,COUNT(*) COUNT  
FROM smzdm_record 
where  DATE_SUB(CURDATE(), INTERVAL %s DAY) <= date(create_time)  
GROUP BY matched_rule,days 
order by matched_rule,create_time"""%(str(limitdays))
#        print sql
        res = self.queryBySql(sql,'dict');
        return res

    def getXIANYUstat(self,limitdays):
        sql = """SELECT keywords,DATE_FORMAT(ctime,'%%Y-%%m-%%d') AS days,COUNT(*) COUNT  
FROM xianyu_record 
where  DATE_SUB(CURDATE(), INTERVAL %s DAY) <= date(ctime)  
GROUP BY keywords,days 
order by keywords,ctime"""%(str(limitdays))
#        print sql
        res = self.queryBySql(sql,'dict');
        return res


    def outputStat(self,days):
        print '-------SMZDM---------'
        smzdmRes = self.getSMZDMstat(days)
        for r in smzdmRes:
            print "%s\t%10s\t%s"%(r['days'],r['matched_rule'][0:5],r['COUNT'])
   
        print '-------XIANYU---------'
        xyRes = self.getXIANYUstat(days)
        for r in xyRes:
            print "%s\t%10s\t%s"%(r['days'],r['keywords'][0:5],r['COUNT'])

    def getItemPriceList(self,name,limitdays=7):
        sql = "SELECT title,price,link,DATE_FORMAT(ctime,'%%Y-%%m-%%d') AS days,ctime,link FROM xianyu_record WHERE keywords LIKE '%%%s%%' AND DATE_SUB(CURDATE(), INTERVAL %s DAY) <= date(ctime)   ORDER BY ctime"%(name,limitdays)
        res = self.queryBySql(sql,'dict');
        return res

    def outputPrice(self,name,days):
        res = self.getItemPriceList(name,days)
        print "--------明细结果-------"
        for r in res:
            print "%s\t%10s\t%s\t%s"%(r['ctime'],r['price'],r['title'],r['link'])
        #获得每日统计信息
        stat={}
        allMax = 0
        allMin = 99999
        allCount = 0
        allSum = 0
        allPrices = []
        for r in res:
            #calc all stat
            allMin = r['price'] if r['price']<allMin else allMin
            allMax = r['price'] if r['price']>allMax else allMax
            allCount += 1
            allSum += r['price']
            allPrices.append(r['price'])
            #calc day stat
            if r['days'] not in stat:
                stat[r['days']] = {}
                stat[r['days']]['max'] = 0
                stat[r['days']]['min'] = 9999
                stat[r['days']]['prices']=[]
            if r['price'] > stat[r['days']]['max']:
                stat[r['days']]['max']  = r['price']
                stat[r['days']]['max_title'] = r['title']
            if r['price'] < stat[r['days']]['min'] :
                stat[r['days']]['min'] = r['price']
                stat[r['days']]['min_title'] = r['title']
            stat[r['days']]['sum'] = stat[r['days']].get('sum',0)+ r['price']
            stat[r['days']]['count'] =  stat[r['days']].get('count',0) + 1
            stat[r['days']]['prices'].append(r['price'])
        stat['Summary']={}
        stat['Summary']['min']=allMin
        stat['Summary']['max']=allMax
        stat['Summary']['count']=allCount
        stat['Summary']['sum']=allSum
        stat['Summary']['prices'] = allPrices
        
        #计算统计信息
        print "--------Stat----------"
        print "%12s\t%s\t%7s\t%s\t%s\t%s"%('日期','记录数','均价','最低价','最高价','众数')
        stats = stat.items()
        stats.sort()
        for day,v in stats:
            
            avg = v['sum'] / v['count']
            mode = self.getMode(v['prices'])
            print "%12s\t%s\t%7.2f\t%s\t%s\t%s"%(day,v['count'],avg,v['min'],v['max'],mode)

    # 众数  
    def getMode(self,arr):  
        mode = [];  
        arr_appear = dict((a, arr.count(a)) for a in arr);  # 统计各个元素出现的次数  
        if max(arr_appear.values()) == 1:  # 如果最大的出现为1  
            return;  # 则没有众数  
        else:  
            for k, v in arr_appear.items():  # 否则，出现次数最大的数字，就是众数  
                if v == max(arr_appear.values()):  
                    mode.append(k);  
        return mode;  
    def execute(self,argv):
        if  len(argv)<3:
            print "usage: [-s limit_days] or [-p itemname limitdays]"
            print "-s : stat for crawl items.  limit_days: 0 for today"
            print "-p : price for item."
            exit()
        if argv[1] == "-s":
            self.outputStat(argv[2])
        elif argv[1] == "-p":
            if len(argv)==3:
                days=3
            else:
                days=argv[3]
            self.outputPrice(argv[2],days)

#main 支持多种查询 stat查询统计数量 price查询物品价格
LifeDB().execute(sys.argv)
