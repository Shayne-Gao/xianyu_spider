#!/usr/bin/python
# coding:utf-8 
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import scrapy
import HTMLParser
import cgi
import hashlib
import time
import pymysql
from urllib import quote
import requests
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
import smtplib
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class XianyuSpider(scrapy.Spider):
    #指定爬虫的名称，用于执行
    name = "xianyu"

    database_ip_and_port = 'localhost'
    database_name = 'life'
    database_username = 'root'
    database_password = 'IWLX8IS12Rl'
    sock = 'SCU15524T3b74cdf9889582dd05f0f6311c30daa95a02d41d5e091'
    keys_file_path = './../../keys'

    def read_local_file_keys(self):
        file = open(self.keys_file_path)
        finalRes = []
        for line in file:
            if line.startswith("#"):
                 continue
            res = {}
            lineList = line.lower().replace("\n",'').replace("\r",'').split('|')
            if len(lineList) <4:
                continue

            keys = lineList[0].split(',')
          
            res['keywords'] = keys
            res['filter'] = {}
            res['filter']['price_min'] = lineList[1]
            res['filter']['price_max'] = lineList[2]
            res['filter']['ignore'] = lineList[3].split(',')
            res['follower']=lineList[4] if len(lineList) >=5 else None
            finalRes.append(res);
        return finalRes;

    #入口函数
    def start_requests(self):
        #指定待抓取的连接池，可以从调度器中获取
        items = self.read_local_file_keys()
        for item in items:
                for key in item['keywords']:
                    #拼接url
                    htmlKeyWord = quote(key.encode('gb2312'))
                    url = 'https://s.2.taobao.com/list/list.htm?q='+ str(htmlKeyWord)+'&search_type=item&app=shopsearch&start='+item['filter']['price_min']+'&end='+item['filter']['price_max']
                    #拼接meta filter
                    #执行抓取，指定url和回调函数parse
                    yield scrapy.Request(url=url, callback=self.parse, meta=item)
    #抓取结果处理
    def parse(self, response):
        print response
        items = response.xpath(("//*[@id='J_ItemListsContainer']/div/div/div[2]/div[1]/a/img/@title")).extract()
        items = response.xpath("//*[@id='J_ItemListsContainer']/div")
        startTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        for quote in items:
            result = {}
            result['keywords'] = ",".join(response.meta['keywords'])
            result['title'] = quote.xpath("div/div[2]/div[1]/a/img/@title").extract_first()
            if  isinstance(result['title'],basestring) and  len(result['title']) > 0:
                    result['title'] =result['title'].replace('</font>','').replace('<font color=red>','')
            else:
                continue
            result['seller'] =  quote.xpath("div/div[1]/div[2]/a/text()").extract_first()
            result['price'] = quote.xpath("div/div[2]/div[2]/div[1]/span/em/text()").extract_first()
            result['desc'] = quote.xpath("div/div[2]/div[3]/text()").extract_first()
            result['img_src'] = quote.xpath("div/div[2]/div[1]/a/img/@data-ks-lazyload-custom").extract_first()
            result['link'] = quote.xpath("div/div[2]/div[1]/a/@href").extract_first()
            result['address'] =  quote.xpath("div/div[2]/div[2]/div[2]/text()").extract_first()

            #根据link获取h5的闲鱼地址
            itemId = result['link'] .replace('//2.taobao.com/item.htm?id=','')
            result['itemid'] = itemId
            result['h5_link'] = 'https://h5.m.taobao.com/2shou/newDetail.html?id='+itemId
            isHited = False
            isExisted = True
            if self.filter(result, response.meta['filter']):
                isHited = True
                #查询是否存在
                if not self.is_data_existed(result):
                    isExisted = False
        #            self.insert_data(result)
                    self.push_wechat(result)
                    #如果有follower的邮件地址就发邮件通知
                    if response.meta['follower'] is not None:
                        self.send_mail(result,response.meta['follower'])
        #            print json.dumps(result,ensure_ascii=False,encoding="gb2312");
                    yield result
            if isHited:
                hitedStr ='\033[1;31mHit!\033[0m'
            else:
                hitedStr = 'Miss'
            print "%s|%6s|%s|Exist:%5s|%8s|%s|%s|%s"%(startTime,result['keywords'],hitedStr,isExisted,result['price'],result['title'],result['address'],result['desc'])


    #业务过滤器，执行如价格过滤等条件。 如果返回true说明数据有效，否则无效
    def filter(self,result,filter):
        #过滤价格
        minP = float(filter['price_min'])
        maxP = float(filter['price_max'])
        result['price'] = float(result['price'])
        if result['price'] < minP or result['price'] > maxP:
            return False
        #过滤title
        for ignore in filter['ignore']:
            if ignore  in result['title']:
                return False
        #过滤内容
        return True

    def md5(self,result):
#    tempResult = sorted(result.items(), key=lambda result: result[0])
        tempResult = result['title']+str(result['price'])+result['link']+result['seller']
        tempResult = tempResult.replace(' ','');
        strRes = str(tempResult)
        m = hashlib.md5()
        m.update(strRes.encode(encoding='utf-8'))
#    print tempResult,m.hexdigest()
        return m.hexdigest()

    def push_wechat(self,data):
        h5Link = "https://g.alicdn.com/idleFish-F2e/app-basic/item.html?itemid=%s&ut_sk=1.VysuoiF98NADAC0Hjidchivd_21407387_1514271709212.Weixin.detail.563120851452.3402573859"%data['itemid']
        infoStr = '''# %s \n\r
[H5链接](%s) \n\r 
%s \n\r 
[跳转到咸鱼打开](%s) \n\r
%s 
Keywords:%s 
\n\r ![logo](%s)'''%(data['desc'],data['h5_link'],data['link'],h5Link,data['address'],data['keywords'],data['img_src'])
        url = 'https://sc.ftqq.com/%s.send' % self.sock
        payload = {'text' : '%s' % (str(data['price'])+"元I"+data['title']),'desp':infoStr}
        requests.post(url,data=payload,verify=False)

    def send_mail(self,data,follower):
        smtp_server = 'smtp.163.com'
        username = 'molv_reporter@163.com'
        password = ''
        to_addr = follower
        #文件正文
        h5Link = "https://g.alicdn.com/idleFish-F2e/app-basic/item.html?itemid=%s&ut_sk=1.VysuoiF98NADAC0Hjidchivd_21407387_1514271709212.Weixin.detail.563120851452.3402573859"%data['itemid']
        dataStr  = ''' <br><h1>%s</h1> 
        <br><h3>%s</h3> 
        <br><h4> ￥ %s  出售于%s </h4>
        <br> 点击图片跳转闲鱼打开 <br>
        <a href="%s"><img src="%s"></a>
        '''%(data['title'],data['desc'],data['price'],data['address'],h5Link,data['img_src'])
        msg = MIMEText(dataStr, 'html', 'utf-8')
        msg['From']=formataddr(['XY reporter',username])
        msg['To']=formataddr(["Dear",to_addr])
        msg['Subject'] = Header(str(data['price'])+" | "+data['title'], 'utf-8').encode()
        server = smtplib.SMTP_SSL(smtp_server, 465)
        server.login(username, password)
        server.sendmail(username, [to_addr], msg.as_string())
        server.quit()


    def is_data_existed(self,result):
        md5result = self.md5(result);
        db = pymysql.connect(self.database_ip_and_port, self.database_username, self.database_password, self.database_name)
        cursor = db.cursor()
        sql = "SELECT * FROM xianyu_record where md5 = '%s'" % md5result

        try:
            cursor.execute(sql)
            #print(cursor.rowcount)
            if cursor.rowcount > 0 :
                return True
            else:
                return False
        except:
            db.rollback()
        db.close()

    def insert_data(self,result):
        db = pymysql.connect(self.database_ip_and_port, self.database_username, self.database_password, self.database_name)
        db.set_charset('utf8')
        cursor = db.cursor()
        result['title'] = result['title'].replace("'","\'")
        sql = "INSERT INTO xianyu_record(title,price,seller,address,keywords,content,link,img_src,`md5`) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
          (result['title'],result['price'],result['seller'],result['address'],result['keywords'],result['desc'],result['link'],result['img_src'],self.md5(result))

#        print sql
        try:
            cursor.execute(sql)
            db.commit()
            if cursor.rowcount > 0:
                return True
            else:
                return False
        except Exception as e:
            print(e)
            db.rollback()
        db.close()
