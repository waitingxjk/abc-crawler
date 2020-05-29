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
    # "Host": "www.chinanpo.gov.cn",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "Origin": "http://www.chinanpo.gov.cn",
    "Content-Type": "application/x-www-form-urlencoded"
}

coockies = {
    "ASP.NET_SessionId": "vj4jkfxl1vefcsc5dufvyz2x",
    "Hm_lpvt_297a9be5a55d666e645ce55f89be171a": "1590650126",
    "Hm_lvt_297a9be5a55d666e645ce55f89be171a": "1590648558,1590648559,1590648759,1590648759"
}

def sleep(s=1,e=2):
    s = random.randint(s,e)
    print("sleep for %d seconds" % s)
    time.sleep(s)

def guangzhouReportLinks():
    url = "https://www.gdnpo.gov.cn/home/index/indexywnjdtnew?MainMc=%E5%9F%BA%E9%87%91%E4%BC%9A&MainCreID=&NJND=2018&cp={}"
    
    total = 25
    for i in range(1, 25):
        print("Loading page {}/{}".format(i, total))

        r = requests.get(url.format(i), proxies = proxy, headers = headers, cookies=coockies)
        if r.status_code != 200:
            continue
        content = r.text

        with open("guangdong/page-{}.html".format(i),"w",encoding="utf8") as writer:
            writer.write(content)

def load_html(path,name,tname):
    file = "{}/{}-{}.html".format(path,name,tname)
    print("Parsing <{}>".format(file))

    with open(file,"r",encoding="utf8") as reader:
        data = reader.read()
    
    soup = BS(data, "html.parser")
    return soup

def guangzhouReports():
    reports = []

    total = 25
    for i in range(1, 25):
        soup = load_html("guangdong/pages","page",i)
        table = soup.find("table",{"class": "admintable"})

        for row in table.tbody.findAll("tr"):
            fields = [v.text.strip() if i<6 else v.find('a').get('href') for i, v in enumerate(row.findAll("td"))]
            if fields[5] != '已报送并公开':
                continue
            reports.append(fields)
    
    import pandas as pd
    data = pd.DataFrame(reports,columns=["序号","名称","代码","法人","年份","年报","链接"])
    data.to_csv("guangdong/reports/links.csv",index=False)

headers={
    "Host": "www.gdnpo.gov.cn",
    # "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    # "Content-Type": "application/x-www-form-urlencoded",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "max-age=0"
}

def parse_frames():
    import pandas as pd 
    data = pd.read_csv("guangdong/reports/links.csv")
    data.columns = ["序号","名称","代码","法人","年份","年报","链接"]

    url = "https://www.gdnpo.gov.cn/pub/xxgsShzzPageYwDetail"

    records = []
    for i,(name,link) in enumerate(zip(data.名称,data.链接)):
        link = url + link[link.rfind("/"):]
        print("<{}/{}> downloading frame information for <{}>: <{}>".format(i+1,data.shape[0],name, link))

        r = requests.get(link, proxies = proxy, headers = headers, cookies=coockies)
        if r.status_code != 200:
            continue
        soup = BS(r.text, "html.parser")
        menu = soup.find("p",{"id": "ifmenus"})

        m = {"名称": name, "链接": link}
        m.update({a.text.strip(): a.get("href") for a in menu.findAll("a")})

        records.append(m)

    data = pd.DataFrame(records)

    data.to_csv("guangdong/frames/links.csv",index=False)
        

def downloadHTML(link):
    print("Downloading from <{}>".format(link))
    r = requests.get(link, proxies = proxy, headers = headers, cookies=coockies)
    if r.status_code != 200:
        return None
    return BS(r.text, "html.parser")

def parse_reports():
    import pandas as pd 
    data = pd.read_csv("guangdong/frames/links.csv")

    url = "https://www.gdnpo.gov.cn"

    def parse_ywhdb(r):
        link = r["业务活动表"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}

        rows = soup.findAll("table")[0].tbody.findAll("tr")

        # for row in rows:
        #     print("=" * 80)
        #     print(row)

        fields = [tuple(td for td in r.findAll("td")) for r in rows]
        fields = [v for v in fields if len(v)==8]
        maps = {
            "1": "捐赠收入",
            "2": "会费收入",
            "3": "提供服务收入",
            "4": "商品销售收入",
            "5": "政府补助收入",
            "6": "投资收益",
            "9": "其他收入",
            "11": "收入合计",
            "12": "业务活动成本",
            "21": "管理费用",
            "24": "筹资费用",
            "28": "其他费用",
            "35": "费用合计",
            "45": "净资产变动额",
        }
        values = {"机构": r["名称"]}
        for row in fields:
            rid = row[1].text.strip()
            if rid in maps:
                for i,it in [(2,"上年末期"),(5,"本年累计")]:
                    for j,jt in [(0,"非限定"),(1,"限定"),(2,"合计")]:
                        #print("rid=<{}> row[i+j]=<{}>".format(rid, row[i+j]))
                        values["{}-{}-{}".format(maps[rid],it,jt)] = row[i+j].find("font",{"name": "showArea"}).text.strip()
        return values

    from collections import defaultdict

    tabs = defaultdict(list)
    for i,r in enumerate(data.to_dict("records")):
        print("="*80)
        print("<{}/{}> Parsing for <{}>".format(i+1,data.shape[0],r["名称"]))
        values = {}
        values.update(parse_ywhdb(r))
        tabs["业务活动"].append(values)

    writer = pd.ExcelWriter('results/广东基金会.xlsx')

    for tab, records in tabs.items():
        df = pd.DataFrame(records) 
        df.to_excel(writer,tab,index=False)
    writer.save()

if __name__ == '__main__':
    # guangzhouReportLinks()
    # guangzhouReports()
    # parse_frames()
    parse_reports()
