import openpyxl
import requests
from bs4 import BeautifulSoup as BS
import os 
import pandas as pd
from progressbar import ProgressBar as Bar

import json
import time
import random
import re

proxy = {
    "http": "http://127.0.0.1:12639",
    "https": "https://127.0.0.1:12639",
}

headers={
    "Host": "www.chinanpo.gov.cn",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "Origin": "http://www.chinanpo.gov.cn",
    "Content-Type": "application/x-www-form-urlencoded"
}

def sleep(s=1,e=2):
    s = random.randint(s,e)
    print("sleep for %d seconds" % s)
    time.sleep(s)

def shanghaiFunds():
    # http://www.chinanpo.gov.cn/search/poporg.html?i=1143515&u=53310000MJ495410XG
    headers={
        "Host": "www.chinanpo.gov.cn",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
        "Origin": "http://www.chinanpo.gov.cn",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    proxy = {
        "http": "127.0.0.1:12639",
        "https": "127.0.0.1:12639",
    }
    url = "http://www.chinanpo.gov.cn/search/orgcx.html"

    import re
    # funds = set()
    # for page in range(1,344):
    #     params = dict(tabIndex=2,t=3,
    #         orgName="%E4%B8%8A%E6%B5%B7",unifiedCode='',regName='',legalName='',
    #         supOrgName='',corporateType='',status=2,orgAddNo=31,regNum=-2,regDate='',regDateEnd='',
    #         page_flag="true", pagesize_key="usciList",
    #         goto_page="first" if page==1 else "next",current_page=page,total_count=17109,page_size=50
    #     )
    #     r = requests.post(url, params, proxies=proxy, headers= headers)
    #     if r.status_code != 200:
    #         continue
    #     # sleep()
    #     records = list(re.findall('popOrgWin\(\"(\d+)\",\"(\S+)\"\)',r.text))
    #     print("Parsing page {}, {} records found.".format(page, len(records)))
    #     for p in records:
    #         funds.add("+".join(p))
        
    # with open("data/shanghai.txt","w",encoding="utf8") as writer:
    #     writer.writelines("\n".join([" ".join(p.split("+")) for p in funds]))
    
    url = "http://www.chinanpo.gov.cn/search/poporg.html"
    with open("data/shanghai.txt","r",encoding="utf8") as reader:
        records = [tuple(l.strip().split(" ")) for l in reader]

        lines = []
        for idx,(i,u) in enumerate(records):
            if (idx+1) % 500 == 0:
                with open("data/shanghai.csv","a",encoding="utf8") as writer:
                    writer.writelines("\n".join(lines))
                lines = []

            print("Getting data from {}/{}".format(idx+1, len(records)))
            r = requests.get(url, dict(i=i,u=u), proxies=proxy)
            if r.status_code != 200:
                continue
            soup = BS(r.text, "html.parser")
            
            title = re.search("\S+",soup.find("div",{"class": "title_bg"}).text).group()
            title = re.sub("\r|\n|,"," ", title)
            td = re.sub("\r|\n|,",' ',soup.findAll("td")[-1].text.strip())
            # print(title)
            # print(td)
            lines.append("{},{}".format(title,td))
            # break
            # sleep()

        with open("data/shanghai.csv","a",encoding="utf8") as writer:
            writer.writelines("\n".join(lines))

def shenzhenReportLinks():
    ofile = "declare_ids.csv"

    # 42页到940页
    url = "http://218.17.83.146:9008/SOCSP_PS_SP/api/info/njnb"
    dfs = []
    for page in (1,2):
        params = dict(pageNum=page, pageSize=10000, size=10000)
        r = requests.post(url, params, proxies=proxy, headers= headers)

        data = json.loads(r.text)
        if data["msg"] != "成功":
            raise ValueError(data["msg"])

        dfs.append(pd.DataFrame(data["data"]["list"]))

    df = pd.concat(dfs)
    df.to_csv(ofile,index=False)

    # try:
    #     bar = Bar(940-42+1).start()
    #     for page in range(42, 940 + 1):
    #         bar.update(bar.currval+1)
    #         if page in pages:
    #             continue
            
    #         #"pageNum":42,"pageSize":10,"size":10,"startRow":411,"endRow":420,"total":28843,"pages":2885
            
    #         params = dict(pageNum=4, pageSize=100, size=100)
    #         r = requests.post(url, params, proxies=proxy, headers= headers)
    #         if r.status_code != 200:
    #             continue

    #         data = json.loads(r.text)
    #         if data["msg"] != "成功":
    #             raise ValueError(data["msg"])

    #         print(data)
    #         print("pageNum=<{}> pageSize=<{}> size=<{}> startRow=<{}> pages=<{}>".format(data["pageNum"],data["pageSize"],data["size"],data["startRow"],data["pages"]))
    #         records = data["data"]["list"]

    #         # SOCIETY_NAME: 深圳市审计学会
    #         # CREDIT_CODE: 5144030050267905XD
    #         # REPORTYEAR: 2019
    #         # ID: 7157b521759d40818f474f3933786af1
    #         # DECLAREID: f930004b253c47ab86c97d0074ebc303
    #         break
    #     bar.finish()
    # except Exception as ex:
    #     raise ex
    # finally:
    #     pass

    #     params = dict(tabIndex=2,t=3,
    #         orgName="%E4%B8%8A%E6%B5%B7",unifiedCode='',regName='',legalName='',
    #         supOrgName='',corporateType='',status=2,orgAddNo=31,regNum=-2,regDate='',regDateEnd='',
    #         page_flag="true", pagesize_key="usciList",
    #         goto_page="first" if page==1 else "next",current_page=page,total_count=17109,page_size=50
    #     )
    #     r = requests.post(url, params, proxies=proxy, headers= headers)
    #     if r.status_code != 200:
    #         continue

def shenzhenReports():
    # ofile = "declare_ids_2018.csv"
    # agents = pd.read_csv(ofile)

    # <iframe  name="首页" src="/SOCSP_O/njnbMf/toIndex?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=483cb448150749c4876f79a471960aee&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="基本信息" src="/SOCSP_O/njnbMf/toJbxx?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=e75f654cbe124a7992e55add4066ae35&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="监事会信息" src="/SOCSP_O/njnbMf/toZzjsJs?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=29ab3ebe15734ae1a2f198a3f33ceda6&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="组织建设一" src="/SOCSP_O/njnbMf/toZzjs?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=d7c9509e77d1451db95751fb6a8b1f18&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="理事会信息" src="/SOCSP_O/njnbMf/toLShxx?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=d46af5dd7b9c479faadf0ca8d8025569&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="组织建设二" src="/SOCSP_O/njnbMf/toZzjs2?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=aaa5fb5e9652457cb7c24ee2ab17180f&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="内部制度建设" src="/SOCSP_O/njnbMf/toNbzdjs?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=20239a9b95844778a74ec40b939c55d7&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="党建情况" src="/SOCSP_O/publicnjnb/toDjqk?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=ca7a850398ce4abca104a86a419e75aa&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="工作采集表" src="/SOCSP_O/njnbMf/toGzcjb?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=dfd720d52d3c469d9b30c888c536f71c&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="监督管理" src="/SOCSP_O/njnbMf/toJdgl?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=1d439d63e33e45908438999ecfef5b1e&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="资产负债表" src="/SOCSP_O/publicnjnb/topublicZcfz?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=31968c50628b456a9869de3a84f4d05d&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="业务活动表" src="/SOCSP_O/publicnjnb/topublicYwhd?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=7972c934d8f34a0cafb60ed4f231d907&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="现金流量表" src="/SOCSP_O/publicnjnb/topublicXjll?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=c17f07c63da0422686b5a5c9583f6cce&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="附注" src="/SOCSP_O/njnbMf/toFz?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=2f36a1f054214ac085078576b15f4aa5&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="净资产" src="/SOCSP_O/njnbMf/toJzc?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=0bdaae1500534a55b760f6b0c85c364c&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="政府补助收入" src="/SOCSP_O/njnbMf/toZfbzsr?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=fb516bd0bbbb4db39955ee9893e641c5&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="年度公益活动" src="/SOCSP_O/njnbMf/toNdgyhd?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=0fef381cd7104541898e10e2494dd17f&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="年度工作总结" src="/SOCSP_O/njnbMf/toNdgzzj?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=83202c84780c4b98962927bb6a05bcf8&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="公益支出比例" src="/SOCSP_O/njnbShtt/toWelfareForm?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=72086076a7b94cc3be72f88c4ef59b84&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="业务开展情况" src="/SOCSP_O/njnbJjh/toYwkzqk?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=089d349df95645d783779ab079face11&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="公益项目收支" src="/SOCSP_O/njnbMf/toGyxmsz?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=da10eca2d46e4ef6a708d5430a09094c&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="公益活动支出" src="/SOCSP_O/publicnjnb/toGyhdzc?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=2206dc51892b455d87fde7dbffd68154&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
#     from selenium import webdriver#导入库
    
#     chrome_options = webdriver.ChromeOptions()
#     chrome_options.add_argument('--proxy-server=127.0.0.1:12639')
# #
#     browser = webdriver.Chrome("E:/chromedriver_win32/chromedriver.exe",options=chrome_options)

    url = "http://218.17.83.146:9009/SOCSP_O/publicnjnb/printAll?declareId="
    settings = [
        ("资产负债表","/SOCSP_O/publicnjnb/topublicZcfz?formId=31968c50628b456a9869de3a84f4d05d&print=1&dy=1&declareId=")
    ]
    name,did = ('深圳市宝安区新安街道幸福海岸幼儿园','04fde8135bd04309a6b5c864fd5e1dc9')
    #for name,did in ():

    #print(url + settings[0][1] + did)
    # url_ = url + settings[0][1] + did
    # print(url_)
    # browser.get(url_)#打开浏览器预设网址
    # print(browser.page_source)#打印网页源代码
    
    # break 

    url_ = url +  did
    print(url_)
    r = requests.get(url_, proxies = proxy, headers = headers, cookies={"JSESSIONID": "5F96AFA08B4D31EBE31969A8D6AF2C69"})
    if r.status_code != 200:
        #continue
        pass
    print(r.text)
    # browser.close()#关闭浏览器

if __name__ == '__main__':
    # shenzhenReportLinks()
    shenzhenReports()
