#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re
import sys, os
import datetime
import smtplib
from email.MIMEText import MIMEText
from email.Header import Header

def get_content(collect_date):
    fold = os.getcwd()

    style = ''
    for line in open(fold+'/css/table_style.css'):
        style = style + line.strip()

    # 邮件正文内容
    hdate = u" 表1.日期: ".encode('gbk')
    headline = u" 婚庆漏斗的show指标统计如下".encode('gbk')
    headline = "%s %s %s"%(hdate, str(collect_date), headline)
    #headline = str(u" 日期: " + collect_date + u" 婚庆漏斗的show指标统计如下").decode('utf-8').encode('gbk')
    str1 = u"流量漏斗".encode('gbk')
    str2 = u"指标".encode('gbk')
    str3 = u"过滤比例".encode('gbk')
    head0 = '<tr><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th></tr>'%(str1, str2, str3)
    content = '<html><th> %s </th></br>'%(headline)+style+'\n<body><table class="gridtable">\n'+head0
    pre_show = -1
    for line in open(fold + '/result/' + "/mail.dat"):
        line = line.decode('utf-8').encode('gbk')
        data = line.split('\t')
        line = '<tr><td>' + line.replace('\t', '</td><td>') + '</td><td>'
        show = float(data[1])
        ratio = round(100*(1 - show/pre_show), 2)
        ratio = str(ratio) + '%'
        if pre_show == -1:
            pre_show = show
            ratio = '-'
        pre_show = show
        line = line + str(ratio) + '</td></tr>'
        content = content + line
    content = content + '\n'

    hdate = u" 表2.日期: ".encode('gbk')
    headline = u" 婚庆大卡各指标统计如下".encode('gbk')
    headline = "%s %s %s"%(hdate, str(collect_date), headline)
    str1 = u"cmatch".encode('gbk')
    str2 = u"sum_show".encode('gbk')
    str3 = u"sum_clk".encode('gbk')
    str4 = u"sum_bid".encode('gbk')
    str5 = u"sum_charge".encode('gbk')
    str6 = u"wasq".encode('gbk')
    str7 = u"ubmq".encode('gbk')
    str8 = u"ctr2".encode('gbk')
    str9 = u"asb".encode('gbk')
    str10 = u"acb".encode('gbk')
    #str11 = u"asob".encode('gbk')
    str12 = u"acp".encode('gbk')
    str13 = u"copc".encode('gbk')
    head1='<tr><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th><th colspan="1"> %s </th></tr>' % (str1,str2,str3,str4,str5,str6,str7,str8,str9,str10,str12,str13)
    content = content + '</table></br></br></br>' + headline + '<table class="gridtable">'+head1
    for line in open(fold + '/data.1'):
        #line = [str(round(100*float(i), 2)) + '%' for i in line.split('\x01')] 
        line = line.split('\x01')
        line[5] = str(round(100*float(line[5]), 2)) + '%'
        line[6] = str(round(100*float(line[6]), 2)) + '%'
        line[7] = str(round(100*float(line[7]), 2)) + '%'
        line[3] = str(int(float(line[3])))
        line[8] = str(round(float(line[8]), 2))
        line[9] = str(round(float(line[9]), 2))
        #line[10] = str(round(float(line[10]), 2))
        line[11] = str(round(float(line[11]), 2))
        line[-1] = str(round(float(line[-1]), 2))
        #line = line.replace('\x01','</td><td>')
        del line[10]
        line = '</td><td>'.join(line)
        line = '<tr><td>' + line.replace('\t', '</td><td>') + '</td></tr>\n'
        content = content + line
    content = content + '\n'

    content = content +'</table></br></body></html>'

    return content

if __name__ == "__main__":
    collect_date = sys.argv[1]

    content=get_content(collect_date)

    if content=="":
        print("Mission Failed")
    else:
        msg = MIMEText(content,'html','gbk')
        msg['Subject'] ="结婚垂类指标监控_"+collect_date
        msg['From'] = 'rongyu01@baidu.com'
        #msg['To'] = 'rongyu01@baidu.com'
        msg['To'] = 'liusheng02@baidu.com,ztc-stra@baidu.com,gaoyucai@baidu.com,shenlin04@baidu.com,renfeng01@baidu.com,rongyu01@baidu.com'
        #msg['Cc'] = 'ztc-auction@baidu.com,ztc-model@baidu.com'
        sender="rongyu01@baidu.com"
        receiver=["liusheng02@baidu.com","ztc-stra@baidu.com","gaoyucai@baidu.com","shenlin04@baidu.com","renfeng01@baidu.com","rongyu01@baidu.com"]
        #receiver=["rongyu01@baidu.com"]
        try:
            svr = smtplib.SMTP('hotswap-in.baidu.com')
            svr.sendmail(sender,receiver, msg.as_string())
            print "email send ok"
        except smtplib.SMTPException:
            print "email send fail!"
